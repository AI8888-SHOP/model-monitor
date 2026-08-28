package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
	record := Record{
		EndpointID: endpoint.ID, EndpointName: endpoint.Name, GroupID: group.ID, GroupName: group.Name,
		Model: model, Status: "ok", TTFTMs: &ttft, CheckedAt: formatTimeString(checkedAt), CheckedAtTS: checkedAt,
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
