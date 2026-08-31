package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func configAsMap(t *testing.T, config Config) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func testMonitor(t *testing.T) *Monitor {
	t.Helper()
	dir := t.TempDir()
	monitor := newMonitor()
	monitor.dataDir = dir
	monitor.configPath = filepath.Join(dir, "config.json")
	monitor.dbPath = filepath.Join(dir, "history.db")
	monitor.metricPath = filepath.Join(dir, "metric_state.json")
	monitor.config = normalizeConfig(configAsMap(t, defaultConfig()))
	if err := monitor.initDB(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = monitor.db.Close() })
	return monitor
}

func TestNormalizeConfigMigratesEndpointTimeout(t *testing.T) {
	raw := configAsMap(t, defaultConfig())
	groups := raw["groups"].([]any)
	delete(groups[0].(map[string]any), "timeout")
	endpoints := raw["endpoints"].([]any)
	endpoints[0].(map[string]any)["timeout"] = 77

	normalized := normalizeConfig(raw)
	if normalized.Groups[0].Timeout != 77 {
		t.Fatalf("timeout = %d, want 77", normalized.Groups[0].Timeout)
	}
}

func TestNormalizeConfigGroupIcon(t *testing.T) {
	raw := configAsMap(t, defaultConfig())
	groups := raw["groups"].([]any)
	delete(groups[0].(map[string]any), "icon")
	if icon := normalizeConfig(raw).Groups[0].Icon; icon != "openai" {
		t.Fatalf("legacy group icon = %q, want openai", icon)
	}
	groups[0].(map[string]any)["icon"] = "gemini"
	if icon := normalizeConfig(raw).Groups[0].Icon; icon != "gemini" {
		t.Fatalf("configured group icon = %q, want gemini", icon)
	}
	groups[0].(map[string]any)["icon"] = "unknown"
	if icon := normalizeConfig(raw).Groups[0].Icon; icon != "openai" {
		t.Fatalf("invalid group icon = %q, want openai", icon)
	}
}

func TestInitDBMigratesRateMultiplierColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "history.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE checks (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		endpoint_id TEXT NOT NULL,
		endpoint_name TEXT NOT NULL,
		group_id TEXT NOT NULL,
		group_name TEXT NOT NULL,
		model_id TEXT NOT NULL,
		status TEXT NOT NULL,
		ttft_ms REAL,
		error TEXT,
		checked_at REAL NOT NULL
	)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	monitor := newMonitor()
	monitor.dbPath = path
	if err := monitor.initDB(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = monitor.db.Close() })
	if _, err := monitor.db.Exec("SELECT rate_multiplier FROM checks LIMIT 0"); err != nil {
		t.Fatalf("rate_multiplier column was not migrated: %v", err)
	}
}

func TestDashboardHidesAdminShortcut(t *testing.T) {
	if strings.Contains(dashboardPage, `href="/admin"`) {
		t.Fatal("public dashboard still links to the admin page")
	}
}

func TestStatusFromErrorAcceptsPunctuation(t *testing.T) {
	if status := statusFromError("HTTP 404: Responses: not found"); status != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", status, http.StatusNotFound)
	}
}

func TestAdminViewRedactsQQSecrets(t *testing.T) {
	config := defaultConfig()
	config.QQPush.AppSecret = "secret"
	config.QQPush.GroupOpenID = "group"
	view := newMonitor().adminConfigView(config)
	qq := view["qq_push"].(map[string]any)
	if qq["app_secret"] != "" || qq["group_openid"] != "" {
		t.Fatalf("sensitive QQ fields were not redacted: %#v", qq)
	}
	if qq["app_secret_set"] != true || qq["group_bound"] != true {
		t.Fatalf("QQ presence flags missing: %#v", qq)
	}
}

func TestChatProbeUsesFreshRequestAndNoTokenLimit(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("Connection"); got != "close" {
			t.Errorf("Connection = %q, want close", got)
		}
		if got := r.Header.Get("X-Request-ID"); got == "" {
			t.Error("X-Request-ID was not set")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		if _, exists := body["max_output_tokens"]; exists {
			t.Error("request contains unsupported max_output_tokens")
		}
		if _, exists := body["max_tokens"]; exists {
			t.Error("request contains provider-specific max_tokens")
		}
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer server.Close()

	monitor := newMonitor()
	endpoint := Endpoint{ID: "api", Name: "test", BaseURL: server.URL, TestPrompt: "Hi", Enabled: true}
	body := map[string]any{"model": "demo", "messages": []any{map[string]any{"role": "user", "content": "Hi"}}, "stream": true}
	started := time.Now()
	result := monitor.chatAttempt(nilContext(), endpoint, body, started)
	if !result.Success {
		t.Fatalf("probe failed: %#v", result)
	}
	if calls.Load() != 1 {
		t.Fatalf("probe calls = %d, want 1", calls.Load())
	}
	if result.Elapsed < 0 {
		t.Fatalf("elapsed = %v", result.Elapsed)
	}
}

func TestCheckModelRetriesHTTPFailureThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/responses" {
			http.NotFound(w, r)
			return
		}
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte(`{"error":{"message":"temporary"}}`))
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"))
	}))
	defer server.Close()

	monitor := newMonitor()
	endpoint := Endpoint{ID: "api", Name: "test", BaseURL: server.URL, TestPrompt: "Hi", Enabled: true}
	group := Group{ID: "group", Name: "test", Enabled: true, Timeout: 5}
	record := monitor.checkModel(endpoint, group, "demo")
	if record.Status != "ok" {
		t.Fatalf("status = %q, error = %q", record.Status, record.Error)
	}
	if calls.Load() != 2 {
		t.Fatalf("chat calls = %d, want 2", calls.Load())
	}
}

func TestCheckModelPrefersResponsesAndCapturesRateMultiplier(t *testing.T) {
	var responseCalls, chatCalls, billingCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			if got := r.Header.Get("X-Request-ID"); got == "" {
				t.Error("Responses X-Request-ID was not set")
			}
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Errorf("decode Responses body: %v", err)
			}
			if _, exists := body["max_output_tokens"]; exists {
				t.Error("Responses request contains unsupported max_output_tokens")
			}
			responseCalls.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\",\"effective_rate_multiplier\":1.75}\n\n"))
		case "/v1/chat/completions":
			chatCalls.Add(1)
			http.Error(w, "chat should not be called", http.StatusInternalServerError)
		case "/v1/sub2api/billing":
			billingCalls.Add(1)
			http.Error(w, "billing should not be called", http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	monitor := newMonitor()
	endpoint := Endpoint{ID: "api", Name: "test", BaseURL: server.URL, TestPrompt: "Hi", Enabled: true}
	group := Group{ID: "group", Name: "test", Enabled: true, Timeout: 5}
	record := monitor.checkModel(endpoint, group, "demo")
	if record.Status != "ok" || record.ProbeProtocol != "responses" {
		t.Fatalf("record = %#v, want successful responses probe", record)
	}
	if record.RateMultiplier == nil || *record.RateMultiplier != 1.75 {
		t.Fatalf("rate multiplier = %v, want 1.75", pointerValue(record.RateMultiplier))
	}
	if responseCalls.Load() != 1 || chatCalls.Load() != 0 || billingCalls.Load() != 0 {
		t.Fatalf("calls: responses=%d chat=%d billing=%d", responseCalls.Load(), chatCalls.Load(), billingCalls.Load())
	}
}

func TestCheckModelFallsBackToChatAndBillingRate(t *testing.T) {
	var responseCalls, chatCalls, billingCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			responseCalls.Add(1)
			http.NotFound(w, r)
		case "/v1/chat/completions":
			chatCalls.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n"))
		case "/v1/sub2api/billing":
			billingCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"effective_rate_multiplier":"2.5x"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	monitor := newMonitor()
	endpoint := Endpoint{ID: "api", Name: "test", BaseURL: server.URL, TestPrompt: "Hi", Enabled: true}
	group := Group{ID: "group", Name: "test", Enabled: true, Timeout: 5}
	record := monitor.checkModel(endpoint, group, "demo")
	if record.Status != "ok" || record.ProbeProtocol != "chat" {
		t.Fatalf("record = %#v, want successful chat fallback", record)
	}
	if record.RateMultiplier == nil || *record.RateMultiplier != 2.5 {
		t.Fatalf("rate multiplier = %v, want 2.5", pointerValue(record.RateMultiplier))
	}
	if responseCalls.Load() != 1 || chatCalls.Load() != 1 || billingCalls.Load() != 1 {
		t.Fatalf("calls: responses=%d chat=%d billing=%d", responseCalls.Load(), chatCalls.Load(), billingCalls.Load())
	}
}

func TestResponsesStreamOutputDetection(t *testing.T) {
	if !responsesStreamOutput("response.output_item.added", map[string]any{"type": "response.output_item.added"}) {
		t.Error("output item event was not detected")
	}
	if responsesStreamOutput("response.created", map[string]any{"type": "response.created"}) {
		t.Error("response.created should not prove output")
	}
}

func TestDashboardUsesSQLiteHistory(t *testing.T) {
	monitor := testMonitor(t)
	config := monitor.snapshotConfig()
	endpoint, group := config.Endpoints[0], config.Groups[0]
	model := "demo"
	checkedAt := nowUnix()
	ttft := 125.0
	rate := 1.5
	record := Record{
		EndpointID: endpoint.ID, EndpointName: endpoint.Name, GroupID: group.ID, GroupName: group.Name,
		Model: model, Status: "ok", TTFTMs: &ttft, RateMultiplier: &rate, CheckedAt: formatTimeString(checkedAt), CheckedAtTS: checkedAt,
	}
	monitor.stateMu.Lock()
	monitor.latestResults[modelKey(endpoint.ID, model)] = record
	monitor.knownModels[endpoint.ID] = []string{model}
	monitor.endpointPingMs[endpoint.ID] = 4.2
	monitor.historyValidAfter = checkedAt - 1
	monitor.stateMu.Unlock()
	if err := monitor.insertHistory([]Record{record}); err != nil {
		t.Fatal(err)
	}
	payload, err := monitor.buildDashboardPayload()
	if err != nil {
		t.Fatal(err)
	}
	summary := payload["summary"].(map[string]any)
	current := summary["current"].(currentCount)
	if current.OK != 1 || current.Total != 1 {
		t.Fatalf("current count = %#v", current)
	}
	models := payload["models"].([]map[string]any)
	if len(models) != 1 || models[0]["model"] != model {
		t.Fatalf("models = %#v", models)
	}
	if models[0]["rate_multiplier"] != rate {
		t.Fatalf("dashboard rate multiplier = %#v, want %v", models[0]["rate_multiplier"], rate)
	}
	groups := payload["groups"].([]map[string]any)
	if groups[0]["icon"] != "openai" {
		t.Fatalf("dashboard group icon = %#v, want openai", groups[0]["icon"])
	}
	recent := models[0]["recent_results"].([]any)
	if len(recent) != 1 || recent[0].(recentResult).RateMultiplier == nil || *recent[0].(recentResult).RateMultiplier != rate {
		t.Fatalf("recent rate multiplier = %#v, want %v", recent, rate)
	}
}

func TestDashboardDisplayModelControlsHistory(t *testing.T) {
	monitor := testMonitor(t)
	config := monitor.snapshotConfig()
	endpoint, group := config.Endpoints[0], config.Groups[0]
	config.Groups[0].DefaultModel = &ModelRef{EndpointID: endpoint.ID, ModelID: "selected"}
	monitor.configMu.Lock()
	monitor.config = config
	monitor.configMu.Unlock()

	checkedAt := nowUnix()
	selectedTTFT, otherTTFT := 125.0, 12_500.0
	selected := Record{
		EndpointID: endpoint.ID, EndpointName: endpoint.Name, GroupID: group.ID, GroupName: group.Name,
		Model: "selected", Status: "ok", TTFTMs: &selectedTTFT, CheckedAt: formatTimeString(checkedAt), CheckedAtTS: checkedAt,
	}
	other := Record{
		EndpointID: endpoint.ID, EndpointName: endpoint.Name, GroupID: group.ID, GroupName: group.Name,
		Model: "other", Status: "fluctuation", TTFTMs: &otherTTFT, CheckedAt: formatTimeString(checkedAt - 0.1), CheckedAtTS: checkedAt - 0.1,
	}
	monitor.stateMu.Lock()
	monitor.latestResults[modelKey(endpoint.ID, selected.Model)] = selected
	monitor.latestResults[modelKey(endpoint.ID, other.Model)] = other
	monitor.knownModels[endpoint.ID] = []string{other.Model, selected.Model}
	monitor.historyValidAfter = checkedAt - 1
	monitor.stateMu.Unlock()
	if err := monitor.insertHistory([]Record{other, selected}); err != nil {
		t.Fatal(err)
	}

	payload, err := monitor.buildDashboardPayload()
	if err != nil {
		t.Fatal(err)
	}
	groups := payload["groups"].([]map[string]any)
	display := groups[0]["display_model"].(map[string]any)
	if display["model"] != selected.Model {
		t.Fatalf("display model = %q, want %q", display["model"], selected.Model)
	}
	recent := display["recent_results"].([]any)
	if len(recent) != 1 || recent[0].(recentResult).Status != selected.Status {
		t.Fatalf("display history = %#v, want only selected model history", recent)
	}
}

func TestEmptyDatabaseDashboardIsAvailable(t *testing.T) {
	monitor := testMonitor(t)
	monitor.stateMu.Lock()
	monitor.historyValidAfter = 0
	monitor.stateMu.Unlock()
	payload, err := monitor.buildDashboardPayload()
	if err != nil {
		t.Fatal(err)
	}
	if payload["models"] == nil || payload["summary"] == nil {
		t.Fatalf("empty dashboard payload is incomplete: %#v", payload)
	}
}

func TestQQMentionTargetOnlyAcceptsBoundGroup(t *testing.T) {
	settings := QQPushSettings{MentionEnabled: true, GroupOpenID: "bound"}
	if _, _, ok := (&Monitor{}).qqMentionTarget(settings, "GROUP_AT_MESSAGE_CREATE", map[string]any{"id": "m1", "group_openid": "other"}); ok {
		t.Error("mention from another group was accepted")
	}
	group, message, ok := (&Monitor{}).qqMentionTarget(settings, "GROUP_AT_MESSAGE_CREATE", map[string]any{"id": "m1", "group_openid": "bound"})
	if !ok || group != "bound" || message != "m1" {
		t.Fatalf("target = %q, %q, %v", group, message, ok)
	}
}

func TestBasicAdminAuth(t *testing.T) {
	monitor := newMonitor()
	monitor.adminPassword = "password"
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/admin/config", nil)
	monitor.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status without auth = %d", recorder.Code)
	}
	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/api/admin/config", nil)
	request.SetBasicAuth("admin", "password")
	monitor.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusInternalServerError && recorder.Code != http.StatusOK {
		t.Fatalf("status with auth = %d, body=%s", recorder.Code, recorder.Body.String())
	}
}

func nilContext() context.Context { return context.Background() }
