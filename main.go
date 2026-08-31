package main

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	_ "modernc.org/sqlite"
)

// The existing UI is kept as a static template while the executable backend
// moves to Go. This keeps the v2 dashboard and admin contract unchanged.
//
//go:embed web/dashboard.html
var dashboardPage string

//go:embed web/admin.html
var adminPage string

const (
	defaultListenHost            = "0.0.0.0"
	defaultListenPort            = 8020
	defaultGroupIcon             = "openai"
	defaultCheckInterval         = 60
	defaultTimeout               = 180
	defaultMaxWorkers            = 16
	defaultRetentionHours        = 72
	defaultQQPushIntervalMinutes = 5
	fluctuationThreshold         = 10 * time.Second
	modelRetryDelay              = 500 * time.Millisecond
	maxHTTPRetryAttempts         = 3
	maxHTTPRetryDelay            = 5 * time.Second
	rateCacheTTL                 = 30 * time.Second
	responsesFallbackTimeout     = 10 * time.Second
	qqMentionDedupWindow         = 10 * time.Minute
	maxRequestBody               = 2 * 1024 * 1024
	maxQQMessageLength           = 4000
	ttftMetricVersion            = "first-output-serial-v1"
)

var (
	timezoneCN = time.FixedZone("Asia/Shanghai", 8*60*60)
	idPattern  = regexp.MustCompile(`[^A-Za-z0-9_-]`)
	groupIcons = map[string]bool{"openai": true, "grok": true, "gemini": true, "claude": true}
)

type ModelRef struct {
	EndpointID string `json:"endpoint_id"`
	ModelID    string `json:"model_id"`
}

type Group struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Description   string    `json:"description"`
	Icon          string    `json:"icon"`
	Enabled       bool      `json:"enabled"`
	CheckInterval int       `json:"check_interval"`
	Timeout       int       `json:"timeout"`
	DefaultModel  *ModelRef `json:"default_model"`
}

type Endpoint struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	BaseURL    string `json:"base_url"`
	APIKey     string `json:"api_key"`
	GroupID    string `json:"group_id"`
	Enabled    bool   `json:"enabled"`
	TestPrompt string `json:"test_prompt"`
	MaxTokens  int    `json:"max_tokens"`
}

type QQPushSettings struct {
	Enabled         bool       `json:"enabled"`
	MentionEnabled  bool       `json:"mention_enabled"`
	AppID           string     `json:"app_id"`
	AppSecret       string     `json:"app_secret"`
	GroupOpenID     string     `json:"group_openid"`
	IntervalMinutes int        `json:"interval_minutes"`
	SelectedModels  []ModelRef `json:"selected_models"`
}

type Config struct {
	Version               int            `json:"version"`
	CheckInterval         int            `json:"check_interval"`
	MaxWorkers            int            `json:"max_workers"`
	HistoryRetentionHours int            `json:"history_retention_hours"`
	Groups                []Group        `json:"groups"`
	Endpoints             []Endpoint     `json:"endpoints"`
	IgnoredModels         []ModelRef     `json:"ignored_models"`
	QQPush                QQPushSettings `json:"qq_push"`
}

type Record struct {
	EndpointID      string   `json:"endpoint_id"`
	EndpointName    string   `json:"endpoint_name"`
	EndpointBaseURL string   `json:"-"`
	GroupID         string   `json:"group_id"`
	GroupName       string   `json:"group_name"`
	Model           string   `json:"model"`
	Status          string   `json:"status"`
	TTFTMs          *float64 `json:"ttft_ms"`
	Error           string   `json:"error,omitempty"`
	CheckedAt       string   `json:"checked_at"`
	CheckedAtTS     float64  `json:"checked_at_ts"`
	ProbeProtocol   string   `json:"probe_protocol,omitempty"`
	RateMultiplier  *float64 `json:"rate_multiplier,omitempty"`
	EndpointPingMs  *float64 `json:"endpoint_ping_ms,omitempty"`
}

type windowStat struct {
	OK           int      `json:"ok"`
	Total        int      `json:"total"`
	Availability *float64 `json:"availability"`
	AvgTTFTMs    *float64 `json:"avg_ttft_ms"`
}

type windowDefinition struct {
	Key     string `json:"key"`
	Label   string `json:"label"`
	Seconds int    `json:"seconds"`
}

var windows = []windowDefinition{
	{Key: "1h", Label: "近1小时", Seconds: 3600},
	{Key: "3h", Label: "近3小时", Seconds: 10800},
	{Key: "24h", Label: "近1天", Seconds: 86400},
}

type qqRuntime struct {
	CaptureStatus    string
	CaptureMessage   string
	CaptureCode      string
	CaptureStartedAt float64
	LastPushAt       float64
	LastPushOK       *bool
	LastPushError    string
	NextPushAt       float64
	MentionStatus    string
	MentionMessage   string
	LastMentionAt    float64
	LastMentionOK    *bool
	LastMentionError string
}

type Monitor struct {
	configMu sync.RWMutex
	config   Config

	stateMu             sync.RWMutex
	latestResults       map[string]Record
	knownModels         map[string][]string
	endpointErrors      map[string]string
	endpointPingMs      map[string]float64
	groupResetAfter     map[string]float64
	lastCheckStartedAt  float64
	lastCheckFinishedAt float64
	checkRunning        bool
	historyValidAfter   float64
	forceCheck          bool
	refreshRequested    bool

	db            *sql.DB
	dataDir       string
	configPath    string
	dbPath        string
	metricPath    string
	listenHost    string
	listenPort    int
	adminUser     string
	adminPassword string

	wake          chan struct{}
	qqPushWake    chan struct{}
	qqMentionWake chan struct{}
	qqRuntime     qqRuntime
	qqTokenMu     sync.Mutex
	qqToken       string
	qqTokenExp    time.Time
	qqTokenID     string
	rateMu        sync.Mutex
	rateCache     map[string]rateCacheEntry

	qqCaptureMu     sync.Mutex
	qqCaptureCancel context.CancelFunc
	qqMentionSeenMu sync.Mutex
	qqMentionSeen   map[string]time.Time
}

type rateCacheEntry struct {
	value     *float64
	expiresAt time.Time
}

type checkTask struct {
	endpoint Endpoint
	group    Group
	modelID  string
}

type checkResult struct {
	key    string
	task   checkTask
	record Record
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envSet(name string, fallback []string) map[string]bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		result := make(map[string]bool, len(fallback))
		for _, item := range fallback {
			result[item] = true
		}
		return result
	}
	result := map[string]bool{}
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			result[item] = true
		}
	}
	return result
}

func clamp(value, fallback, minimum, maximum int) int {
	if value == 0 && fallback != 0 && minimum <= 0 {
		value = fallback
	}
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

func stringValue(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return ""
	}
}

func boolValue(value any, fallback bool) bool {
	if value == nil {
		return fallback
	}
	if parsed, ok := value.(bool); ok {
		return parsed
	}
	return fallback
}

func intValue(value any, fallback int) int {
	if value == nil {
		return fallback
	}
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	case json.Number:
		parsed, err := typed.Int64()
		if err == nil {
			return int(parsed)
		}
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(typed))
		if err == nil {
			return parsed
		}
	}
	return fallback
}

func mapValue(value any) map[string]any {
	parsed, _ := value.(map[string]any)
	return parsed
}

func sliceValue(value any) []any {
	parsed, _ := value.([]any)
	return parsed
}

func normalizeID(value any, prefix string) string {
	cleaned := idPattern.ReplaceAllString(stringValue(value), "_")
	if cleaned == "" {
		return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	}
	if len(cleaned) > 64 {
		cleaned = cleaned[:64]
	}
	return cleaned
}

func normalizeBaseURL(value any) string {
	raw := stringValue(value)
	if raw == "" {
		return ""
	}
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "http://" + raw
	}
	return strings.TrimRight(raw, "/")
}

func normalizeGroupIcon(value any) string {
	icon := strings.ToLower(stringValue(value))
	if groupIcons[icon] {
		return icon
	}
	return defaultGroupIcon
}

func formatTime(timestamp float64) any {
	if timestamp <= 0 {
		return nil
	}
	return time.Unix(int64(timestamp), int64((timestamp-math.Floor(timestamp))*1e9)).In(timezoneCN).Format("2006-01-02 15:04:05")
}

func formatTimeString(timestamp float64) string {
	formatted := formatTime(timestamp)
	if formatted == nil {
		return ""
	}
	return formatted.(string)
}

func nowUnix() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

func modelKey(endpointID, modelID string) string {
	return endpointID + "|" + modelID
}

func availability(ok, total int) *float64 {
	if total == 0 {
		return nil
	}
	value := math.Round(float64(ok)/float64(total)*10000) / 100
	return &value
}

func floatPointer(value float64) *float64 {
	return &value
}

func stringPointer(value string) *string {
	return &value
}

func defaultConfig() Config {
	host := os.Getenv("SUB2API_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := envInt("SUB2API_PORT", 8080)
	baseURL := normalizeBaseURL(fmt.Sprintf("%s:%d", host, port))
	groupID := "grp_default"
	endpointID := "api_default"
	excluded := envSet("EXCLUDED_MODELS", []string{"mimo-v2.5-tts", "qwen3.5", "minimax-m2.5"})
	ignored := make([]ModelRef, 0, len(excluded))
	for modelID := range excluded {
		ignored = append(ignored, ModelRef{EndpointID: endpointID, ModelID: modelID})
	}
	sort.Slice(ignored, func(i, j int) bool { return ignored[i].ModelID < ignored[j].ModelID })
	return Config{
		Version:               3,
		CheckInterval:         envInt("CHECK_INTERVAL", defaultCheckInterval),
		MaxWorkers:            envInt("MAX_WORKERS", defaultMaxWorkers),
		HistoryRetentionHours: envInt("HISTORY_RETENTION_HOURS", defaultRetentionHours),
		Groups: []Group{{
			ID:            groupID,
			Name:          firstNonEmpty(os.Getenv("DEFAULT_GROUP_NAME"), "默认分组"),
			Icon:          defaultGroupIcon,
			Enabled:       true,
			CheckInterval: envInt("CHECK_INTERVAL", defaultCheckInterval),
			Timeout:       envInt("TIMEOUT", defaultTimeout),
		}},
		Endpoints: []Endpoint{{
			ID:         endpointID,
			Name:       firstNonEmpty(os.Getenv("DEFAULT_API_NAME"), "sub2api"),
			BaseURL:    baseURL,
			APIKey:     strings.TrimSpace(os.Getenv("API_KEY")),
			GroupID:    groupID,
			Enabled:    true,
			TestPrompt: firstNonEmpty(os.Getenv("TEST_PROMPT"), "Hi"),
			MaxTokens:  16,
		}},
		IgnoredModels: ignored,
		QQPush: QQPushSettings{
			AppID:           strings.TrimSpace(os.Getenv("QQ_BOT_APP_ID")),
			AppSecret:       strings.TrimSpace(os.Getenv("QQ_BOT_APP_SECRET")),
			GroupOpenID:     strings.TrimSpace(os.Getenv("QQ_GROUP_OPENID")),
			IntervalMinutes: envInt("QQ_PUSH_INTERVAL_MINUTES", defaultQQPushIntervalMinutes),
			SelectedModels:  []ModelRef{},
		},
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func normalizeModelRef(value any) *ModelRef {
	var endpointID, modelID string
	switch typed := value.(type) {
	case map[string]any:
		endpointID = stringValue(typed["endpoint_id"])
		modelID = stringValue(typed["model_id"])
	case string:
		parts := strings.SplitN(typed, "|", 2)
		if len(parts) == 2 {
			endpointID, modelID = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		}
	case ModelRef:
		endpointID, modelID = typed.EndpointID, typed.ModelID
	case *ModelRef:
		if typed != nil {
			endpointID, modelID = typed.EndpointID, typed.ModelID
		}
	}
	if endpointID == "" || modelID == "" {
		return nil
	}
	modelID = truncateRunes(modelID, 200)
	return &ModelRef{EndpointID: normalizeID(endpointID, "api"), ModelID: modelID}
}

func normalizeConfig(raw map[string]any) Config {
	fallback := defaultConfig()
	globalInterval := clamp(intValue(raw["check_interval"], fallback.CheckInterval), defaultCheckInterval, 10, 86400)
	legacyTimeouts := map[string]int{}
	for _, item := range sliceValue(raw["endpoints"]) {
		endpoint := mapValue(item)
		if endpoint == nil || endpoint["timeout"] == nil {
			continue
		}
		groupID := normalizeID(endpoint["group_id"], "grp")
		if _, exists := legacyTimeouts[groupID]; !exists {
			legacyTimeouts[groupID] = clamp(intValue(endpoint["timeout"], defaultTimeout), defaultTimeout, 5, 600)
		}
	}

	groups := []Group{}
	seenGroups := map[string]bool{}
	for _, item := range sliceValue(raw["groups"]) {
		group := mapValue(item)
		if group == nil {
			continue
		}
		id := normalizeID(group["id"], "grp")
		if seenGroups[id] {
			id = normalizeID(nil, "grp")
		}
		seenGroups[id] = true
		name := firstNonEmpty(stringValue(group["name"]), "未命名分组")
		name = truncateRunes(name, 80)
		description := stringValue(group["description"])
		description = truncateRunes(description, 200)
		timeoutFallback := defaultTimeout
		if legacy, exists := legacyTimeouts[id]; exists {
			timeoutFallback = legacy
		}
		groups = append(groups, Group{
			ID:            id,
			Name:          name,
			Description:   description,
			Icon:          normalizeGroupIcon(group["icon"]),
			Enabled:       boolValue(group["enabled"], true),
			CheckInterval: clamp(intValue(group["check_interval"], globalInterval), globalInterval, 10, 86400),
			Timeout:       clamp(intValue(group["timeout"], timeoutFallback), defaultTimeout, 5, 600),
			DefaultModel:  normalizeModelRef(group["default_model"]),
		})
	}
	if len(groups) == 0 {
		groups = fallback.Groups
	}
	groupIDs := map[string]bool{}
	for _, group := range groups {
		groupIDs[group.ID] = true
	}

	endpoints := []Endpoint{}
	seenEndpoints := map[string]bool{}
	for _, item := range sliceValue(raw["endpoints"]) {
		endpoint := mapValue(item)
		if endpoint == nil {
			continue
		}
		id := normalizeID(endpoint["id"], "api")
		if seenEndpoints[id] {
			id = normalizeID(nil, "api")
		}
		seenEndpoints[id] = true
		baseURL := normalizeBaseURL(endpoint["base_url"])
		if baseURL == "" {
			continue
		}
		groupID := normalizeID(endpoint["group_id"], "grp")
		if !groupIDs[groupID] {
			groupID = groups[0].ID
		}
		name := firstNonEmpty(stringValue(endpoint["name"]), "API")
		name = truncateRunes(name, 80)
		prompt := firstNonEmpty(stringValue(endpoint["test_prompt"]), firstNonEmpty(os.Getenv("TEST_PROMPT"), "Hi"))
		prompt = truncateRunes(prompt, 500)
		endpoints = append(endpoints, Endpoint{
			ID:         id,
			Name:       name,
			BaseURL:    truncateRunes(baseURL, 300),
			APIKey:     truncateRunes(stringValue(endpoint["api_key"]), 500),
			GroupID:    groupID,
			Enabled:    boolValue(endpoint["enabled"], true),
			TestPrompt: prompt,
			MaxTokens:  maxInt(16, clamp(intValue(endpoint["max_tokens"], 16), 16, 1, 256)),
		})
	}
	if len(endpoints) == 0 {
		endpoints = fallback.Endpoints
	}
	endpointIDs := map[string]bool{}
	for _, endpoint := range endpoints {
		endpointIDs[endpoint.ID] = true
	}

	ignored := normalizeRefList(raw["ignored_models"], endpointIDs)
	ignoredKeys := map[string]bool{}
	for _, item := range ignored {
		ignoredKeys[modelKey(item.EndpointID, item.ModelID)] = true
	}
	for index := range groups {
		ref := groups[index].DefaultModel
		if ref == nil || !endpointIDs[ref.EndpointID] || ignoredKeys[modelKey(ref.EndpointID, ref.ModelID)] {
			groups[index].DefaultModel = nil
			continue
		}
		for _, endpoint := range endpoints {
			if endpoint.ID == ref.EndpointID && endpoint.GroupID != groups[index].ID {
				groups[index].DefaultModel = nil
				break
			}
		}
	}

	qqRaw := mapValue(raw["qq_push"])
	selected := []ModelRef{}
	if qqRaw != nil {
		selected = normalizeRefList(qqRaw["selected_models"], endpointIDs)
	}
	qq := QQPushSettings{}
	if qqRaw != nil {
		qq = QQPushSettings{
			Enabled:         boolValue(qqRaw["enabled"], false),
			MentionEnabled:  boolValue(qqRaw["mention_enabled"], false),
			AppID:           truncateRunes(stringValue(qqRaw["app_id"]), 80),
			AppSecret:       truncateRunes(stringValue(qqRaw["app_secret"]), 500),
			GroupOpenID:     truncateRunes(stringValue(qqRaw["group_openid"]), 300),
			IntervalMinutes: clamp(intValue(qqRaw["interval_minutes"], defaultQQPushIntervalMinutes), defaultQQPushIntervalMinutes, 1, 1440),
			SelectedModels:  selected,
		}
	}
	if qq.AppID == "" {
		qq.AppID = fallback.QQPush.AppID
	}
	if qq.AppSecret == "" {
		qq.AppSecret = fallback.QQPush.AppSecret
	}
	if qq.GroupOpenID == "" {
		qq.GroupOpenID = fallback.QQPush.GroupOpenID
	}
	if qq.IntervalMinutes == 0 {
		qq.IntervalMinutes = defaultQQPushIntervalMinutes
	}
	return Config{
		Version:               3,
		CheckInterval:         globalInterval,
		MaxWorkers:            clamp(intValue(raw["max_workers"], defaultMaxWorkers), defaultMaxWorkers, 1, 128),
		HistoryRetentionHours: clamp(intValue(raw["history_retention_hours"], defaultRetentionHours), defaultRetentionHours, 24, 2160),
		Groups:                groups,
		Endpoints:             endpoints,
		IgnoredModels:         ignored,
		QQPush:                qq,
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func normalizeRefList(value any, endpointIDs map[string]bool) []ModelRef {
	result := []ModelRef{}
	seen := map[string]bool{}
	for _, item := range sliceValue(value) {
		ref := normalizeModelRef(item)
		if ref == nil || !endpointIDs[ref.EndpointID] {
			continue
		}
		key := modelKey(ref.EndpointID, ref.ModelID)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, *ref)
	}
	return result
}

func (m *Monitor) snapshotConfig() Config {
	m.configMu.RLock()
	defer m.configMu.RUnlock()
	encoded, _ := json.Marshal(m.config)
	var copyConfig Config
	_ = json.Unmarshal(encoded, &copyConfig)
	return copyConfig
}

func newMonitor() *Monitor {
	dataDir := os.Getenv("DATA_DIR")
	if strings.TrimSpace(dataDir) == "" {
		dataDir = filepath.Join(".", "data")
	}
	return &Monitor{
		dataDir:         dataDir,
		configPath:      filepath.Join(dataDir, "config.json"),
		dbPath:          filepath.Join(dataDir, "history.db"),
		metricPath:      filepath.Join(dataDir, "metric_state.json"),
		listenHost:      firstNonEmpty(os.Getenv("LISTEN_HOST"), defaultListenHost),
		listenPort:      envInt("LISTEN_PORT", defaultListenPort),
		adminUser:       firstNonEmpty(os.Getenv("ADMIN_USER"), "admin"),
		adminPassword:   os.Getenv("ADMIN_PASSWORD"),
		wake:            make(chan struct{}, 1),
		qqPushWake:      make(chan struct{}, 1),
		qqMentionWake:   make(chan struct{}, 1),
		latestResults:   map[string]Record{},
		knownModels:     map[string][]string{},
		endpointErrors:  map[string]string{},
		endpointPingMs:  map[string]float64{},
		groupResetAfter: map[string]float64{},
		qqMentionSeen:   map[string]time.Time{},
		rateCache:       map[string]rateCacheEntry{},
		qqRuntime: qqRuntime{
			CaptureStatus:  "idle",
			CaptureMessage: "尚未开始绑定",
			MentionStatus:  "disabled",
			MentionMessage: "未启用 @ 查询",
		},
	}
}

func (m *Monitor) loadConfig() error {
	if err := os.MkdirAll(m.dataDir, 0o700); err != nil {
		return err
	}
	var raw map[string]any
	if content, err := os.ReadFile(m.configPath); err == nil {
		decoder := json.NewDecoder(bytes.NewReader(content))
		decoder.UseNumber()
		if err := decoder.Decode(&raw); err != nil {
			return fmt.Errorf("配置 JSON 格式错误: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if raw == nil {
		raw = map[string]any{}
		encoded, _ := json.Marshal(defaultConfig())
		_ = json.Unmarshal(encoded, &raw)
	}
	normalized := normalizeConfig(raw)
	if err := m.writeConfig(normalized); err != nil {
		return err
	}
	m.configMu.Lock()
	m.config = normalized
	m.configMu.Unlock()
	return nil
}

func (m *Monitor) writeConfig(config Config) error {
	encoded, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	tmpPath := m.configPath + ".tmp"
	if err := os.WriteFile(tmpPath, encoded, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, m.configPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	_ = os.Chmod(m.configPath, 0o600)
	return nil
}

func (m *Monitor) saveConfig(next Config) (Config, error) {
	encoded, err := json.Marshal(next)
	if err != nil {
		return Config{}, err
	}
	var raw map[string]any
	if err := json.Unmarshal(encoded, &raw); err != nil {
		return Config{}, err
	}
	normalized := normalizeConfig(raw)
	if err := m.writeConfig(normalized); err != nil {
		return Config{}, err
	}
	m.configMu.Lock()
	m.config = normalized
	m.configMu.Unlock()
	m.wakeScheduler(true, false)
	m.signalQQ()
	return normalized, nil
}

func (m *Monitor) adminConfigView(config Config) map[string]any {
	encoded, _ := json.Marshal(config)
	var public map[string]any
	_ = json.Unmarshal(encoded, &public)
	qq, _ := public["qq_push"].(map[string]any)
	if qq == nil {
		qq = map[string]any{}
		public["qq_push"] = qq
	}
	secretSet := config.QQPush.AppSecret != ""
	groupBound := config.QQPush.GroupOpenID != ""
	qq["app_secret"] = ""
	qq["group_openid"] = ""
	qq["app_secret_set"] = secretSet
	qq["group_bound"] = groupBound
	return public
}

func (m *Monitor) saveAdminConfig(next Config) (Config, error) {
	current := m.snapshotConfig()
	if next.QQPush.AppSecret == "" {
		next.QQPush.AppSecret = current.QQPush.AppSecret
	}
	if next.QQPush.GroupOpenID == "" {
		next.QQPush.GroupOpenID = current.QQPush.GroupOpenID
	}
	return m.saveConfig(next)
}

func (m *Monitor) initMetricState() error {
	state := map[string]any{}
	if content, err := os.ReadFile(m.metricPath); err == nil {
		_ = json.Unmarshal(content, &state)
	}
	if state["ttft_metric_version"] != ttftMetricVersion {
		state = map[string]any{"ttft_metric_version": ttftMetricVersion, "valid_after": nowUnix()}
		encoded, _ := json.MarshalIndent(state, "", "  ")
		if err := os.WriteFile(m.metricPath, append(encoded, '\n'), 0o600); err != nil {
			return err
		}
	}
	m.stateMu.Lock()
	m.historyValidAfter = floatValue(state["valid_after"])
	m.stateMu.Unlock()
	return nil
}

func floatValue(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case json.Number:
		parsed, _ := typed.Float64()
		return parsed
	case int64:
		return float64(typed)
	case int:
		return float64(typed)
	}
	return 0
}

func (m *Monitor) initDB() error {
	db, err := sql.Open("sqlite", m.dbPath)
	if err != nil {
		return err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	statements := []string{
		`CREATE TABLE IF NOT EXISTS checks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			endpoint_id TEXT NOT NULL,
			endpoint_name TEXT NOT NULL,
			group_id TEXT NOT NULL,
			group_name TEXT NOT NULL,
			model_id TEXT NOT NULL,
			status TEXT NOT NULL,
			ttft_ms REAL,
			rate_multiplier REAL,
			error TEXT,
			checked_at REAL NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_checks_time ON checks(checked_at)`,
		`CREATE INDEX IF NOT EXISTS idx_checks_model_time ON checks(endpoint_id, model_id, checked_at)`,
		`CREATE INDEX IF NOT EXISTS idx_checks_group_time ON checks(group_id, checked_at)`,
		`CREATE INDEX IF NOT EXISTS idx_checks_endpoint_time ON checks(endpoint_id, checked_at)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			_ = db.Close()
			return err
		}
	}
	if err := ensureSQLiteColumn(db, "checks", "rate_multiplier", "REAL"); err != nil {
		_ = db.Close()
		return err
	}
	m.db = db
	return nil
}

func ensureSQLiteColumn(db *sql.DB, table, column, definition string) error {
	rows, err := db.Query("PRAGMA table_info(" + table + ")")
	if err != nil {
		return err
	}
	for rows.Next() {
		var cid int
		var name, columnType string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey); err != nil {
			return err
		}
		if name == column {
			rows.Close()
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	_, err = db.Exec("ALTER TABLE " + table + " ADD COLUMN " + column + " " + definition)
	return err
}

func (m *Monitor) wakeScheduler(refresh, force bool) {
	m.stateMu.Lock()
	if refresh {
		m.refreshRequested = true
	}
	if force {
		m.forceCheck = true
	}
	m.stateMu.Unlock()
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *Monitor) signalQQ() {
	select {
	case m.qqPushWake <- struct{}{}:
	default:
	}
	select {
	case m.qqMentionWake <- struct{}{}:
	default:
	}
}

func (m *Monitor) takeSchedulerFlags() (bool, bool) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	refresh, force := m.refreshRequested, m.forceCheck
	m.refreshRequested, m.forceCheck = false, false
	return refresh, force
}

func (m *Monitor) insertHistory(records []Record) error {
	if len(records) == 0 {
		return nil
	}
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO checks (
		endpoint_id, endpoint_name, group_id, group_name, model_id,
		status, ttft_ms, rate_multiplier, error, checked_at
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, record := range records {
		var ttft any
		if record.TTFTMs != nil {
			ttft = *record.TTFTMs
		}
		var rateMultiplier any
		if record.RateMultiplier != nil {
			rateMultiplier = *record.RateMultiplier
		}
		var probeError any
		if record.Error != "" {
			probeError = record.Error
		}
		if _, err := stmt.Exec(
			record.EndpointID, record.EndpointName, record.GroupID, record.GroupName,
			record.Model, record.Status, ttft, rateMultiplier, probeError, record.CheckedAtTS,
		); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (m *Monitor) pruneHistory(retentionHours int) error {
	cutoff := nowUnix() - float64(retentionHours)*3600
	_, err := m.db.Exec("DELETE FROM checks WHERE checked_at < ?", cutoff)
	return err
}

func (m *Monitor) clearGroupHistory(groupID string) error {
	snapshot := m.snapshotConfig()
	valid := false
	for _, group := range snapshot.Groups {
		if group.ID == groupID {
			valid = true
			break
		}
	}
	if !valid {
		return errors.New("分组不存在")
	}
	if _, err := m.db.Exec("DELETE FROM checks WHERE group_id = ?", groupID); err != nil {
		return err
	}
	resetAt := nowUnix()
	m.stateMu.Lock()
	m.groupResetAfter[groupID] = resetAt
	for key, record := range m.latestResults {
		if record.GroupID == groupID {
			delete(m.latestResults, key)
		}
	}
	m.stateMu.Unlock()
	return nil
}

func (m *Monitor) historyFilter(endpointIDs map[string]bool, ignored map[string]bool, validAfter float64) (string, []any) {
	parts := []string{}
	params := []any{}
	if validAfter > 0 {
		parts = append(parts, "checked_at >= ?")
		params = append(params, validAfter)
	}
	if endpointIDs != nil {
		ids := sortedKeys(endpointIDs)
		if len(ids) == 0 {
			return " AND 1 = 0", nil
		}
		parts = append(parts, "endpoint_id IN ("+placeholders(len(ids))+")")
		for _, id := range ids {
			params = append(params, id)
		}
	}
	if len(ignored) > 0 {
		keys := sortedKeys(ignored)
		parts = append(parts, "(endpoint_id || '|' || model_id) NOT IN ("+placeholders(len(keys))+")")
		for _, key := range keys {
			params = append(params, key)
		}
	}
	if len(parts) == 0 {
		return "", params
	}
	return " AND " + strings.Join(parts, " AND "), params
}

func sortedKeys(values map[string]bool) []string {
	keys := make([]string, 0, len(values))
	for key, enabled := range values {
		if enabled {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	return keys
}

func placeholders(count int) string {
	items := make([]string, count)
	for index := range items {
		items[index] = "?"
	}
	return strings.Join(items, ",")
}

func scanNullableFloat(value any) *float64 {
	switch typed := value.(type) {
	case float64:
		return floatPointer(typed)
	case float32:
		return floatPointer(float64(typed))
	case int64:
		return floatPointer(float64(typed))
	case []byte:
		parsed, err := strconv.ParseFloat(string(typed), 64)
		if err == nil {
			return floatPointer(parsed)
		}
	case string:
		parsed, err := strconv.ParseFloat(typed, 64)
		if err == nil {
			return floatPointer(parsed)
		}
	}
	return nil
}

func emptyWindowStat() windowStat {
	return windowStat{OK: 0, Total: 0, Availability: nil, AvgTTFTMs: nil}
}

func (m *Monitor) queryGlobalWindows(endpointIDs map[string]bool, ignored map[string]bool, validAfter float64) (map[string]windowStat, error) {
	output := map[string]windowStat{}
	filter, params := m.historyFilter(endpointIDs, ignored, validAfter)
	for _, window := range windows {
		row := m.db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) AS total_count,
			COALESCE(SUM(CASE WHEN status IN ('ok', 'fluctuation') THEN 1 ELSE 0 END), 0) AS ok_count,
			AVG(CASE WHEN status IN ('ok', 'fluctuation') THEN ttft_ms ELSE NULL END)
			FROM checks WHERE checked_at >= ?%s`, filter), append([]any{nowUnix() - float64(window.Seconds)}, params...)...)
		var total, ok int
		var avg any
		if err := row.Scan(&total, &ok, &avg); err != nil {
			return nil, err
		}
		output[window.Key] = windowStat{OK: ok, Total: total, Availability: availability(ok, total), AvgTTFTMs: roundPointer(scanNullableFloat(avg), 1)}
	}
	return output, nil
}

func (m *Monitor) queryGroupedWindows(fields []string, endpointIDs map[string]bool, ignored map[string]bool, validAfter float64) (map[string]map[string]windowStat, error) {
	output := map[string]map[string]windowStat{}
	for _, window := range windows {
		output[window.Key] = map[string]windowStat{}
	}
	filter, params := m.historyFilter(endpointIDs, ignored, validAfter)
	selectFields := strings.Join(fields, ", ")
	groupBy := strings.Join(fields, ", ")
	for _, window := range windows {
		args := append([]any{nowUnix() - float64(window.Seconds)}, params...)
		rows, err := m.db.Query(fmt.Sprintf(`SELECT %s,
			COUNT(*) AS total_count,
			COALESCE(SUM(CASE WHEN status IN ('ok', 'fluctuation') THEN 1 ELSE 0 END), 0) AS ok_count,
			AVG(CASE WHEN status IN ('ok', 'fluctuation') THEN ttft_ms ELSE NULL END)
			FROM checks WHERE checked_at >= ?%s GROUP BY %s`, selectFields, filter, groupBy), args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			values := make([]any, len(fields))
			dest := make([]any, len(fields))
			for index := range values {
				dest[index] = &values[index]
			}
			var total, ok int
			var avg any
			dest = append(dest, &total, &ok, &avg)
			if err := rows.Scan(dest...); err != nil {
				rows.Close()
				return nil, err
			}
			keyParts := make([]string, len(values))
			for index, value := range values {
				keyParts[index] = fmt.Sprint(value)
			}
			output[window.Key][strings.Join(keyParts, "|")] = windowStat{
				OK: ok, Total: total, Availability: availability(ok, total), AvgTTFTMs: roundPointer(scanNullableFloat(avg), 1),
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return nil, err
		}
		rows.Close()
	}
	return output, nil
}

type recentResult struct {
	Status         string   `json:"status"`
	TTFTMs         *float64 `json:"ttft_ms"`
	RateMultiplier *float64 `json:"rate_multiplier,omitempty"`
	Error          string   `json:"error,omitempty"`
	CheckedAt      any      `json:"checked_at"`
}

func (m *Monitor) queryRecentModelResults(endpointIDs map[string]bool, ignored map[string]bool, limit int, validAfter float64) (map[string][]recentResult, error) {
	output := map[string][]recentResult{}
	filter, params := m.historyFilter(endpointIDs, ignored, validAfter)
	rows, err := m.db.Query("SELECT endpoint_id, model_id, status, ttft_ms, rate_multiplier, error, checked_at FROM checks WHERE 1 = 1"+filter+" ORDER BY checked_at DESC", params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var endpointID, modelID, status string
		var ttft, rateMultiplier, probeError, checkedAt any
		if err := rows.Scan(&endpointID, &modelID, &status, &ttft, &rateMultiplier, &probeError, &checkedAt); err != nil {
			return nil, err
		}
		key := modelKey(endpointID, modelID)
		if len(output[key]) >= limit {
			continue
		}
		var errorText string
		if probeError != nil {
			errorText = fmt.Sprint(probeError)
		}
		output[key] = append(output[key], recentResult{
			Status: status, TTFTMs: roundPointer(scanNullableFloat(ttft), 1),
			RateMultiplier: roundPointer(scanNullableFloat(rateMultiplier), 4), Error: errorText,
			CheckedAt: formatTime(floatValue(checkedAt)),
		})
	}
	return output, rows.Err()
}

func roundPointer(value *float64, places int) *float64 {
	if value == nil {
		return nil
	}
	factor := math.Pow10(places)
	rounded := math.Round(*value*factor) / factor
	return &rounded
}

var freshHTTPTransport = &http.Transport{
	Proxy:                 http.ProxyFromEnvironment,
	DisableKeepAlives:     true,
	MaxIdleConns:          0,
	MaxIdleConnsPerHost:   0,
	IdleConnTimeout:       1 * time.Millisecond,
	TLSHandshakeTimeout:   15 * time.Second,
	ResponseHeaderTimeout: 0,
	ExpectContinueTimeout: 1 * time.Second,
}

var probeHTTPClient = &http.Client{Transport: freshHTTPTransport}

func openAIURL(baseURL, suffix string) (string, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return "", err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return "", errors.New("仅支持 http/https API 地址")
	}
	if parsed.Hostname() == "" {
		return "", errors.New("API 地址缺少主机名")
	}
	prefix := strings.TrimRight(parsed.Path, "/")
	if !strings.HasSuffix(prefix, "/v1") {
		prefix += "/v1"
	}
	parsed.Path = prefix + suffix
	parsed.RawPath = ""
	return parsed.String(), nil
}

func requestID() string {
	return fmt.Sprintf("model-monitor-%d", time.Now().UnixNano())
}

func endpointHeaders(endpoint Endpoint, accept string) http.Header {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	headers.Set("Accept", accept)
	headers.Set("X-Request-ID", requestID())
	headers.Set("Connection", "close")
	if endpoint.APIKey != "" {
		headers.Set("Authorization", "Bearer "+endpoint.APIKey)
	}
	return headers
}

func readBodyLimit(reader io.Reader, limit int64) ([]byte, error) {
	return io.ReadAll(io.LimitReader(reader, limit+1))
}

func detailFromBody(body []byte) string {
	detail := strings.TrimSpace(string(body))
	if len(detail) > 240 {
		detail = detail[:240]
	}
	return detail
}

func (m *Monitor) fetchModels(endpoint Endpoint) ([]string, string) {
	started := time.Now()
	defer func() {
		m.stateMu.Lock()
		m.endpointPingMs[endpoint.ID] = roundFloat(float64(time.Since(started).Microseconds())/1000, 1)
		m.stateMu.Unlock()
	}()
	path, err := openAIURL(endpoint.BaseURL, "/models")
	if err != nil {
		return nil, err.Error()
	}
	timeout := time.Duration(clamp(envInt("TIMEOUT", defaultTimeout), defaultTimeout, 5, 600)) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err.Error()
	}
	for key, values := range endpointHeaders(endpoint, "application/json") {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	req.Close = true
	resp, err := probeHTTPClient.Do(req)
	if err != nil {
		return nil, truncateError(err.Error())
	}
	defer resp.Body.Close()
	body, readErr := readBodyLimit(resp.Body, 512*1024)
	if readErr != nil {
		return nil, truncateError(readErr.Error())
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Sprintf("HTTP %d: %s", resp.StatusCode, detailFromBody(body))
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, truncateError(err.Error())
	}
	items, _ := payload["data"].([]any)
	seen := map[string]bool{}
	models := []string{}
	for _, item := range items {
		entry := mapValue(item)
		modelID := stringValue(entry["id"])
		if modelID != "" && !seen[modelID] {
			seen[modelID] = true
			models = append(models, modelID)
		}
	}
	sort.Strings(models)
	return models, ""
}

func truncateError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 240 {
		return value[:240]
	}
	return value
}

func roundFloat(value float64, places int) float64 {
	factor := math.Pow10(places)
	return math.Round(value*factor) / factor
}

func isRetryableStatus(status int) bool {
	return status == 408 || status == 425 || status == 429 || status >= 500
}

func statusFromError(message string) int {
	message = strings.TrimSpace(message)
	index := strings.Index(message, "HTTP ")
	if index < 0 {
		return 0
	}
	parts := strings.Fields(message[index:])
	if len(parts) < 2 {
		return 0
	}
	status, _ := strconv.Atoi(parts[1])
	return status
}

func extractErrorMessage(payload map[string]any) string {
	if payload == nil {
		return ""
	}
	if errorData, ok := payload["error"].(map[string]any); ok {
		message := stringValue(errorData["message"])
		errType := stringValue(errorData["type"])
		if message != "" && errType != "" {
			return errType + ": " + message
		}
		if message != "" {
			return message
		}
		if errType != "" {
			return errType
		}
	}
	if message, ok := payload["error"].(string); ok && strings.TrimSpace(message) != "" {
		return strings.TrimSpace(message)
	}
	return stringValue(payload["message"])
}

func numberValue(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	case string:
		raw := strings.TrimSpace(typed)
		if strings.HasSuffix(strings.ToLower(raw), "x") {
			raw = strings.TrimSpace(raw[:len(raw)-1])
		}
		parsed, err := strconv.ParseFloat(raw, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func rateMultiplierValue(value any) *float64 {
	parsed, ok := numberValue(value)
	if !ok || math.IsNaN(parsed) || math.IsInf(parsed, 0) || parsed < 0 {
		return nil
	}
	parsed = roundFloat(parsed, 4)
	return &parsed
}

func rateMultiplierFromPayload(payload map[string]any) *float64 {
	if payload == nil {
		return nil
	}
	keys := []string{
		"effective_rate_multiplier",
		"resolved_rate_multiplier",
		"rate_multiplier",
		"effectiveRateMultiplier",
		"resolvedRateMultiplier",
		"rateMultiplier",
	}
	for _, key := range keys {
		if value := rateMultiplierValue(payload[key]); value != nil {
			return value
		}
	}
	for _, value := range payload {
		if nested := mapValue(value); nested != nil {
			if multiplier := rateMultiplierFromPayload(nested); multiplier != nil {
				return multiplier
			}
		}
		for _, item := range sliceValue(value) {
			if nested := mapValue(item); nested != nil {
				if multiplier := rateMultiplierFromPayload(nested); multiplier != nil {
					return multiplier
				}
			}
		}
	}
	return nil
}

func rateMultiplierFromHeaders(headers http.Header) *float64 {
	if headers == nil {
		return nil
	}
	known := []string{
		"Rate-Multiplier",
		"Effective-Rate-Multiplier",
		"Resolved-Rate-Multiplier",
		"X-Rate-Multiplier",
		"X-Effective-Rate-Multiplier",
		"X-Resolved-Rate-Multiplier",
		"X-Sub2API-Rate-Multiplier",
	}
	for _, key := range known {
		if value := rateMultiplierValue(headers.Get(key)); value != nil {
			return value
		}
	}
	for key, values := range headers {
		lower := strings.ToLower(key)
		if (!strings.Contains(lower, "rate") && !strings.Contains(lower, "billing")) || !strings.Contains(lower, "multiplier") {
			continue
		}
		for _, raw := range values {
			if value := rateMultiplierValue(raw); value != nil {
				return value
			}
		}
	}
	return nil
}

func firstNonNilRate(values ...*float64) *float64 {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func cloneFloatPointer(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func rateCacheKey(endpoint Endpoint) string {
	return endpoint.ID + "|" + endpoint.BaseURL
}

func (m *Monitor) cachedRateMultiplier(endpoint Endpoint) (*float64, bool) {
	now := time.Now()
	key := rateCacheKey(endpoint)
	m.rateMu.Lock()
	defer m.rateMu.Unlock()
	entry, ok := m.rateCache[key]
	if !ok {
		return nil, false
	}
	if !entry.expiresAt.After(now) {
		delete(m.rateCache, key)
		return nil, false
	}
	return cloneFloatPointer(entry.value), true
}

func (m *Monitor) cacheRateMultiplier(endpoint Endpoint, value *float64) {
	m.rateMu.Lock()
	if m.rateCache == nil {
		m.rateCache = map[string]rateCacheEntry{}
	}
	m.rateCache[rateCacheKey(endpoint)] = rateCacheEntry{
		value:     cloneFloatPointer(value),
		expiresAt: time.Now().Add(rateCacheTTL),
	}
	m.rateMu.Unlock()
}

func (m *Monitor) fetchBillingRate(ctx context.Context, endpoint Endpoint) *float64 {
	if ctx == nil {
		ctx = context.Background()
	}
	if value, ok := m.cachedRateMultiplier(endpoint); ok {
		return value
	}
	target, err := openAIURL(endpoint.BaseURL, "/sub2api/billing")
	if err != nil {
		return nil
	}
	billingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(billingCtx, http.MethodGet, target, nil)
	if err != nil {
		return nil
	}
	for key, values := range endpointHeaders(endpoint, "application/json") {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	req.Close = true
	resp, err := probeHTTPClient.Do(req)
	if err != nil {
		m.cacheRateMultiplier(endpoint, nil)
		return nil
	}
	defer resp.Body.Close()
	body, err := readBodyLimit(resp.Body, 256*1024)
	if err != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		m.cacheRateMultiplier(endpoint, nil)
		return nil
	}
	var payload map[string]any
	if json.Unmarshal(body, &payload) != nil {
		m.cacheRateMultiplier(endpoint, nil)
		return nil
	}
	value := rateMultiplierFromPayload(payload)
	m.cacheRateMultiplier(endpoint, value)
	return cloneFloatPointer(value)
}

func (m *Monitor) resolveRateMultiplier(ctx context.Context, endpoint Endpoint, direct *float64) *float64 {
	if direct != nil {
		m.cacheRateMultiplier(endpoint, direct)
		return cloneFloatPointer(direct)
	}
	return m.fetchBillingRate(ctx, endpoint)
}

func firstOutputInChoice(choice map[string]any) bool {
	if choice == nil {
		return false
	}
	if value := choice["text"]; value != nil && stringValue(value) != "" {
		return true
	}
	delta := mapValue(choice["delta"])
	for _, field := range []string{"reasoning_content", "reasoning", "thinking", "thoughts", "content", "output_text", "refusal"} {
		value := delta[field]
		if value == nil {
			continue
		}
		if stringValue(value) != "" || len(sliceValue(value)) > 0 || len(mapValue(value)) > 0 {
			return true
		}
	}
	return false
}

func assistantMessagePresent(choice map[string]any) bool {
	if choice == nil {
		return false
	}
	message := mapValue(choice["message"])
	if message == nil {
		return false
	}
	if stringValue(message["role"]) == "assistant" {
		return true
	}
	for _, field := range []string{"content", "output_text", "refusal", "tool_calls"} {
		value := message[field]
		if stringValue(value) != "" || len(sliceValue(value)) > 0 || len(mapValue(value)) > 0 {
			return true
		}
	}
	return false
}

func chatChoicesContainOutput(payload map[string]any, stream bool) bool {
	choices, _ := payload["choices"].([]any)
	for _, item := range choices {
		choice := mapValue(item)
		if stream && firstOutputInChoice(choice) {
			return true
		}
		if !stream && assistantMessagePresent(choice) {
			return true
		}
	}
	return false
}

func responsesInput(requestBody map[string]any) string {
	messages, ok := requestBody["messages"].([]any)
	if !ok {
		return firstNonEmpty(stringValue(requestBody["input"]), "Hi")
	}
	parts := []string{}
	for _, item := range messages {
		message := mapValue(item)
		if message == nil {
			continue
		}
		if content := stringValue(message["content"]); content != "" {
			parts = append(parts, content)
			continue
		}
		for _, contentItem := range sliceValue(message["content"]) {
			contentMap := mapValue(contentItem)
			content := firstNonEmpty(stringValue(contentMap["text"]), stringValue(contentMap["content"]))
			if content != "" {
				parts = append(parts, content)
			}
		}
	}
	return firstNonEmpty(strings.Join(parts, "\n"), "Hi")
}

func responsesOutputPresent(payload map[string]any) bool {
	if payload == nil || extractErrorMessage(payload) != "" {
		return false
	}
	if strings.TrimSpace(stringValue(payload["output_text"])) != "" {
		return true
	}
	output := sliceValue(payload["output"])
	if len(output) > 0 {
		if stringValue(payload["status"]) == "completed" {
			return true
		}
		for _, item := range output {
			entry := mapValue(item)
			if entry == nil {
				continue
			}
			typeName := stringValue(entry["type"])
			if typeName == "message" || typeName == "output_text" || typeName == "reasoning" || typeName == "tool_call" {
				return true
			}
			for _, part := range sliceValue(entry["content"]) {
				content := mapValue(part)
				if content != nil && (stringValue(content["text"]) != "" || stringValue(content["type"]) == "output_text" || stringValue(content["type"]) == "refusal") {
					return true
				}
			}
		}
	}
	return chatChoicesContainOutput(payload, false)
}

func responsesStreamOutput(eventName string, payload map[string]any) bool {
	eventName = strings.ToLower(eventName)
	switch eventName {
	case "response.output_item.added", "response.output_text.delta", "response.output_text.done", "response.reasoning_summary_text.delta", "response.refusal.delta":
		return true
	case "response.completed":
		response := mapValue(payload["response"])
		return stringValue(response["status"]) == "completed"
	}
	if extractErrorMessage(payload) != "" {
		return false
	}
	delta := payload["delta"]
	return stringValue(delta) != "" || len(sliceValue(delta)) > 0 || len(mapValue(delta)) > 0
}

type probeAttempt struct {
	Success        bool
	Retryable      bool
	Elapsed        float64
	RateMultiplier *float64
	Error          string
}

func elapsedMs(started time.Time) float64 {
	return roundFloat(float64(time.Since(started).Microseconds())/1000, 1)
}

func newJSONRequest(ctx context.Context, method, target string, body map[string]any, endpoint Endpoint, accept string) (*http.Request, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	for key, values := range endpointHeaders(endpoint, accept) {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	req.Close = true
	return req, nil
}

func (m *Monitor) chatNonStream(ctx context.Context, endpoint Endpoint, requestBody map[string]any, started time.Time) probeAttempt {
	target, err := openAIURL(endpoint.BaseURL, "/chat/completions")
	if err != nil {
		return probeAttempt{Retryable: false, Elapsed: elapsedMs(started), Error: err.Error()}
	}
	body := cloneMap(requestBody)
	body["stream"] = false
	req, err := newJSONRequest(ctx, http.MethodPost, target, body, endpoint, "application/json")
	if err != nil {
		return probeAttempt{Retryable: true, Elapsed: elapsedMs(started), Error: err.Error()}
	}
	resp, err := probeHTTPClient.Do(req)
	if err != nil {
		return probeAttempt{Retryable: true, Elapsed: elapsedMs(started), Error: truncateError(err.Error())}
	}
	defer resp.Body.Close()
	raw, readErr := readBodyLimit(resp.Body, 512*1024)
	if readErr != nil {
		return probeAttempt{Retryable: true, Elapsed: elapsedMs(started), Error: truncateError(readErr.Error())}
	}
	if resp.StatusCode != http.StatusOK {
		return probeAttempt{Retryable: isRetryableStatus(resp.StatusCode), Elapsed: elapsedMs(started), Error: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, detailFromBody(raw))}
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return probeAttempt{Retryable: true, Elapsed: elapsedMs(started), Error: "响应 JSON 无效：" + truncateError(err.Error())}
	}
	if chatChoicesContainOutput(payload, false) {
		return probeAttempt{Success: true, Elapsed: elapsedMs(started), RateMultiplier: firstNonNilRate(rateMultiplierFromHeaders(resp.Header), rateMultiplierFromPayload(payload))}
	}
	errorText := firstNonEmpty(extractErrorMessage(payload), "non-stream response missing assistant message")
	return probeAttempt{Retryable: true, Elapsed: elapsedMs(started), Error: truncateError(errorText)}
}

func (m *Monitor) chatAttempt(ctx context.Context, endpoint Endpoint, requestBody map[string]any, started time.Time) probeAttempt {
	target, err := openAIURL(endpoint.BaseURL, "/chat/completions")
	if err != nil {
		return probeAttempt{Retryable: false, Elapsed: elapsedMs(started), Error: err.Error()}
	}
	req, err := newJSONRequest(ctx, http.MethodPost, target, requestBody, endpoint, "text/event-stream, application/json")
	if err != nil {
		return probeAttempt{Retryable: true, Elapsed: elapsedMs(started), Error: err.Error()}
	}
	resp, err := probeHTTPClient.Do(req)
	if err != nil {
		return probeAttempt{Retryable: true, Elapsed: elapsedMs(started), Error: truncateError(err.Error())}
	}
	if resp.StatusCode != http.StatusOK {
		raw, _ := readBodyLimit(resp.Body, 240)
		resp.Body.Close()
		return probeAttempt{Retryable: isRetryableStatus(resp.StatusCode), Elapsed: elapsedMs(started), Error: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, detailFromBody(raw))}
	}
	contentType := strings.ToLower(resp.Header.Get("Content-Type"))
	streamError := ""
	found := false
	var responseRate *float64
	if strings.Contains(contentType, "text/event-stream") || contentType == "" {
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 4096), 512*1024)
		eventName := ""
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if strings.HasPrefix(line, "event:") {
				eventName = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
				continue
			}
			if !strings.HasPrefix(line, "data:") {
				continue
			}
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data == "[DONE]" {
				break
			}
			var payload map[string]any
			if json.Unmarshal([]byte(data), &payload) != nil {
				eventName = ""
				continue
			}
			responseRate = firstNonNilRate(responseRate, rateMultiplierFromPayload(payload))
			if errorText := extractErrorMessage(payload); errorText != "" || strings.EqualFold(eventName, "error") {
				streamError = firstNonEmpty(errorText, "SSE event: "+eventName)
				break
			}
			if chatChoicesContainOutput(payload, true) {
				found = true
				break
			}
			eventName = ""
		}
		if scanErr := scanner.Err(); scanErr != nil {
			streamError = firstNonEmpty(streamError, truncateError(scanErr.Error()))
		}
		resp.Body.Close()
	} else {
		raw, readErr := readBodyLimit(resp.Body, 512*1024)
		resp.Body.Close()
		if readErr == nil {
			var payload map[string]any
			if json.Unmarshal(raw, &payload) == nil {
				responseRate = firstNonNilRate(responseRate, rateMultiplierFromPayload(payload))
				if chatChoicesContainOutput(payload, false) {
					found = true
				}
			}
		}
	}
	if found {
		return probeAttempt{Success: true, Elapsed: elapsedMs(started), RateMultiplier: firstNonNilRate(rateMultiplierFromHeaders(resp.Header), responseRate)}
	}
	nonStream := m.chatNonStream(ctx, endpoint, requestBody, started)
	if nonStream.Success {
		nonStream.RateMultiplier = firstNonNilRate(responseRate, nonStream.RateMultiplier)
		return nonStream
	}
	errorText := firstNonEmpty(nonStream.Error, streamError, "stream ended before first output")
	return probeAttempt{Retryable: nonStream.Retryable || streamError != "", Elapsed: nonStream.Elapsed, Error: truncateError(errorText)}
}

func (m *Monitor) responsesAttempt(ctx context.Context, endpoint Endpoint, requestBody map[string]any, started time.Time) probeAttempt {
	target, err := openAIURL(endpoint.BaseURL, "/responses")
	if err != nil {
		return probeAttempt{Retryable: false, Elapsed: elapsedMs(started), Error: err.Error()}
	}
	body := map[string]any{"model": requestBody["model"], "input": responsesInput(requestBody), "stream": true}
	req, err := newJSONRequest(ctx, http.MethodPost, target, body, endpoint, "text/event-stream, application/json")
	if err != nil {
		return probeAttempt{Retryable: true, Elapsed: elapsedMs(started), Error: err.Error()}
	}
	resp, err := probeHTTPClient.Do(req)
	if err != nil {
		return probeAttempt{Retryable: true, Elapsed: elapsedMs(started), Error: truncateError(err.Error())}
	}
	defer resp.Body.Close()
	responseRate := rateMultiplierFromHeaders(resp.Header)
	if resp.StatusCode != http.StatusOK {
		raw, _ := readBodyLimit(resp.Body, 240)
		return probeAttempt{Retryable: isRetryableStatus(resp.StatusCode), Elapsed: elapsedMs(started), Error: fmt.Sprintf("HTTP %d: Responses: %s", resp.StatusCode, detailFromBody(raw))}
	}
	contentType := strings.ToLower(resp.Header.Get("Content-Type"))
	if !strings.Contains(contentType, "text/event-stream") {
		raw, readErr := readBodyLimit(resp.Body, 512*1024)
		if readErr != nil {
			return probeAttempt{Retryable: true, Elapsed: elapsedMs(started), Error: truncateError(readErr.Error())}
		}
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			return probeAttempt{Retryable: false, Elapsed: elapsedMs(started), Error: "Responses 响应 JSON 无效：" + truncateError(err.Error())}
		}
		if responsesOutputPresent(payload) {
			return probeAttempt{Success: true, Elapsed: elapsedMs(started), RateMultiplier: firstNonNilRate(responseRate, rateMultiplierFromPayload(payload))}
		}
		return probeAttempt{Retryable: false, Elapsed: elapsedMs(started), Error: firstNonEmpty(extractErrorMessage(payload), "Responses API 响应缺少模型输出")}
	}
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), 512*1024)
	eventName := ""
	var lastPayload map[string]any
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "event:") {
			eventName = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			break
		}
		var payload map[string]any
		if json.Unmarshal([]byte(data), &payload) != nil {
			eventName = ""
			continue
		}
		lastPayload = payload
		responseRate = firstNonNilRate(responseRate, rateMultiplierFromPayload(payload))
		if errorText := extractErrorMessage(payload); errorText != "" {
			return probeAttempt{Retryable: true, Elapsed: elapsedMs(started), Error: truncateError(errorText)}
		}
		payloadType := stringValue(payload["type"])
		if responsesStreamOutput(firstNonEmpty(eventName, payloadType), payload) {
			return probeAttempt{Success: true, Elapsed: elapsedMs(started), RateMultiplier: responseRate}
		}
		eventName = ""
	}
	if err := scanner.Err(); err != nil {
		return probeAttempt{Retryable: true, Elapsed: elapsedMs(started), Error: truncateError(err.Error())}
	}
	if responsesOutputPresent(lastPayload) {
		return probeAttempt{Success: true, Elapsed: elapsedMs(started), RateMultiplier: responseRate}
	}
	return probeAttempt{Retryable: false, Elapsed: elapsedMs(started), Error: "Responses 流在首个输出前结束"}
}

func cloneMap(value map[string]any) map[string]any {
	result := make(map[string]any, len(value)+1)
	for key, item := range value {
		result[key] = item
	}
	return result
}

func shouldFallbackToChat(attempt probeAttempt) bool {
	status := statusFromError(attempt.Error)
	if status == 400 || status == 404 || status == 405 || status == 422 || status >= 500 {
		return true
	}
	lower := strings.ToLower(strings.TrimSpace(attempt.Error))
	return strings.HasPrefix(lower, "responses ") || strings.Contains(lower, "context deadline exceeded")
}

func (m *Monitor) checkModel(endpoint Endpoint, group Group, modelID string) Record {
	checkedAt := nowUnix()
	record := Record{
		EndpointID:      endpoint.ID,
		EndpointName:    endpoint.Name,
		EndpointBaseURL: endpoint.BaseURL,
		GroupID:         group.ID,
		GroupName:       group.Name,
		Model:           modelID,
		Status:          "unknown",
		CheckedAt:       formatTimeString(checkedAt),
		CheckedAtTS:     checkedAt,
	}
	prompt := firstNonEmpty(endpoint.TestPrompt, firstNonEmpty(os.Getenv("TEST_PROMPT"), "Hi"))
	requestBody := map[string]any{
		"model":    modelID,
		"messages": []any{map[string]any{"role": "user", "content": prompt}},
		"stream":   true,
	}
	started := time.Now()
	timeoutSeconds := clamp(group.Timeout, defaultTimeout, 5, 600)
	deadline := started.Add(time.Duration(timeoutSeconds) * time.Second)
	attempts := 0
	lastError := ""
	consecutiveHTTPFailures := 0
	useResponses := true
	protocol := "responses"
	if cached, ok := m.cachedRateMultiplier(endpoint); ok {
		record.RateMultiplier = cached
	}
	resolveRate := func(direct *float64) *float64 {
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()
		return m.resolveRateMultiplier(ctx, endpoint, direct)
	}

	for {
		if !time.Now().Before(deadline) {
			if statusFromError(lastError) != 0 {
				return finishCheckError(record, attempts, lastError, elapsedMs(started))
			}
			return finishCheckTimeout(record, timeoutSeconds, attempts, lastError, elapsedMs(started))
		}
		attempts++
		attempt := probeAttempt{}
		if useResponses {
			responsesDeadline := deadline
			if fallbackDeadline := time.Now().Add(responsesFallbackTimeout); fallbackDeadline.Before(responsesDeadline) {
				responsesDeadline = fallbackDeadline
			}
			ctx, cancel := context.WithDeadline(context.Background(), responsesDeadline)
			attempt = m.responsesAttempt(ctx, endpoint, requestBody, started)
			cancel()
			if !attempt.Success && shouldFallbackToChat(attempt) {
				useResponses = false
				protocol = "chat"
				ctx, cancel = context.WithDeadline(context.Background(), deadline)
				chatAttempt := m.chatAttempt(ctx, endpoint, requestBody, started)
				cancel()
				if chatAttempt.Success || chatAttempt.Error != "" {
					attempt = chatAttempt
				}
			}
		} else {
			protocol = "chat"
			ctx, cancel := context.WithDeadline(context.Background(), deadline)
			attempt = m.chatAttempt(ctx, endpoint, requestBody, started)
			cancel()
		}
		if attempt.Success {
			record.TTFTMs = floatPointer(attempt.Elapsed)
			record.RateMultiplier = resolveRate(attempt.RateMultiplier)
			record.ProbeProtocol = protocol
			if attempt.Elapsed > float64(fluctuationThreshold/time.Millisecond) {
				record.Status = "fluctuation"
				record.Error = fmt.Sprintf("检测波动：%.1f秒后恢复，共尝试 %d 次", attempt.Elapsed/1000, attempts)
			} else {
				record.Status = "ok"
			}
			return record
		}
		lastError = firstNonEmpty(attempt.Error, "检测失败")
		status := statusFromError(lastError)
		if !attempt.Retryable {
			return finishCheckError(record, attempts, lastError, attempt.Elapsed)
		}
		if status != 0 {
			consecutiveHTTPFailures++
			if consecutiveHTTPFailures >= maxHTTPRetryAttempts {
				return finishCheckError(record, attempts, lastError, attempt.Elapsed)
			}
		} else {
			consecutiveHTTPFailures = 0
		}
		wait := modelRetryDelay
		if status != 0 {
			exponent := consecutiveHTTPFailures - 1
			if exponent < 0 {
				exponent = 0
			}
			wait = time.Duration(float64(modelRetryDelay) * math.Pow(2, float64(minInt(exponent, 3))))
			if wait > maxHTTPRetryDelay {
				wait = maxHTTPRetryDelay
			}
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			continue
		}
		if wait > remaining {
			wait = remaining
		}
		time.Sleep(wait)
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func finishCheckTimeout(record Record, timeoutSeconds, attempts int, lastError string, elapsed float64) Record {
	retries := maxInt(0, attempts-1)
	record.Status = "timeout"
	record.TTFTMs = floatPointer(elapsed)
	record.Error = fmt.Sprintf("检测超时：%d秒内重试 %d 次仍不可用", timeoutSeconds, retries)
	if lastError != "" {
		record.Error += "；最后错误：" + lastError
	}
	record.Error = truncateError(record.Error)
	return record
}

func finishCheckError(record Record, attempts int, lastError string, elapsed float64) Record {
	record.Status = "error"
	record.TTFTMs = floatPointer(elapsed)
	message := firstNonEmpty(lastError, "检测失败")
	if attempts > 1 {
		message = fmt.Sprintf("检测失败：连续尝试 %d 次仍不可用；最后错误：%s", attempts, message)
	}
	record.Error = truncateError(message)
	return record
}

func (m *Monitor) discoverCheckTasks(snapshot Config) (map[string]checkTask, error) {
	groups := map[string]Group{}
	for _, group := range snapshot.Groups {
		if group.Enabled {
			groups[group.ID] = group
		}
	}
	ignored := map[string]bool{}
	for _, item := range snapshot.IgnoredModels {
		ignored[modelKey(item.EndpointID, item.ModelID)] = true
	}
	tasks := map[string]checkTask{}
	errorsByEndpoint := map[string]string{}
	fetchedModels := map[string][]string{}
	for _, endpoint := range snapshot.Endpoints {
		if !endpoint.Enabled {
			continue
		}
		group, exists := groups[endpoint.GroupID]
		if !exists {
			continue
		}
		models, errorText := m.fetchModels(endpoint)
		if errorText != "" {
			errorsByEndpoint[endpoint.ID] = errorText
			m.stateMu.RLock()
			models = append([]string(nil), m.knownModels[endpoint.ID]...)
			m.stateMu.RUnlock()
		} else {
			fetchedModels[endpoint.ID] = append([]string(nil), models...)
		}
		for _, modelID := range models {
			if ignored[modelKey(endpoint.ID, modelID)] {
				continue
			}
			key := modelKey(endpoint.ID, modelID)
			tasks[key] = checkTask{endpoint: endpoint, group: group, modelID: modelID}
		}
	}
	m.stateMu.Lock()
	for endpointID, models := range fetchedModels {
		m.knownModels[endpointID] = models
	}
	m.endpointErrors = errorsByEndpoint
	m.stateMu.Unlock()
	return tasks, nil
}

func groupInterval(snapshot Config, groupID string) int {
	for _, group := range snapshot.Groups {
		if group.ID == groupID {
			return clamp(group.CheckInterval, snapshot.CheckInterval, 10, 86400)
		}
	}
	return clamp(snapshot.CheckInterval, defaultCheckInterval, 10, 86400)
}

func activeIntervals(snapshot Config) []int {
	intervals := []int{snapshot.CheckInterval}
	for _, group := range snapshot.Groups {
		if group.Enabled {
			intervals = append(intervals, group.CheckInterval)
		}
	}
	for index, value := range intervals {
		intervals[index] = clamp(value, defaultCheckInterval, 10, 86400)
	}
	return intervals
}

func (m *Monitor) modelIsMonitorable(snapshot Config, endpointID, groupID, modelID string) bool {
	groupOK := false
	for _, group := range snapshot.Groups {
		if group.ID == groupID {
			groupOK = group.Enabled
			break
		}
	}
	if !groupOK {
		return false
	}
	for _, endpoint := range snapshot.Endpoints {
		if endpoint.ID == endpointID {
			if !endpoint.Enabled || endpoint.GroupID != groupID {
				return false
			}
			for _, ignored := range snapshot.IgnoredModels {
				if ignored.EndpointID == endpointID && ignored.ModelID == modelID {
					return false
				}
			}
			return true
		}
	}
	return false
}

func fallbackRecord(task checkTask, err error) Record {
	checkedAt := nowUnix()
	return Record{
		EndpointID: task.endpoint.ID, EndpointName: task.endpoint.Name, EndpointBaseURL: task.endpoint.BaseURL,
		GroupID: task.group.ID, GroupName: task.group.Name, Model: task.modelID, Status: "error",
		Error: truncateError(err.Error()), CheckedAt: formatTimeString(checkedAt), CheckedAtTS: checkedAt,
	}
}

func (m *Monitor) storeCheckRecord(record Record) {
	snapshot := m.snapshotConfig()
	if !m.modelIsMonitorable(snapshot, record.EndpointID, record.GroupID, record.Model) {
		m.stateMu.Lock()
		delete(m.latestResults, modelKey(record.EndpointID, record.Model))
		m.stateMu.Unlock()
		return
	}
	m.stateMu.RLock()
	resetAt := m.groupResetAfter[record.GroupID]
	m.stateMu.RUnlock()
	if record.CheckedAtTS < resetAt {
		return
	}
	if err := m.insertHistory([]Record{record}); err != nil {
		log.Printf("[ERROR] insert history: %v", err)
		return
	}
	m.stateMu.Lock()
	m.latestResults[modelKey(record.EndpointID, record.Model)] = record
	m.lastCheckFinishedAt = nowUnix()
	m.stateMu.Unlock()
	log.Printf("[%s] %s / %s TTFT=%vms err=%s", record.Status, record.EndpointName, record.Model, pointerValue(record.TTFTMs), record.Error)
}

func pointerValue(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func (m *Monitor) triggerCheck() bool {
	m.wakeScheduler(true, true)
	return true
}

func (m *Monitor) setCheckRunning(value bool) {
	m.stateMu.Lock()
	m.checkRunning = value
	m.stateMu.Unlock()
}

func (m *Monitor) checkerLoop() {
	tasks := map[string]checkTask{}
	nextDue := map[string]time.Time{}
	running := map[string]checkTask{}
	runningEndpoints := map[string]bool{}
	results := make(chan checkResult, 128)
	lastDiscovery := time.Time{}
	lastPrune := time.Time{}
	for {
		now := time.Now()
		snapshot := m.snapshotConfig()
		refreshRequested, forceCheck := m.takeSchedulerFlags()
		intervals := activeIntervals(snapshot)
		discoveryInterval := 60 * time.Second
		if len(intervals) > 0 {
			minimum := intervals[0]
			for _, interval := range intervals[1:] {
				if interval < minimum {
					minimum = interval
				}
			}
			if time.Duration(minimum)*time.Second > discoveryInterval {
				discoveryInterval = time.Duration(minimum) * time.Second
			}
		}
		if refreshRequested || forceCheck || len(tasks) == 0 || lastDiscovery.IsZero() || now.Sub(lastDiscovery) >= discoveryInterval {
			discovered, err := m.discoverCheckTasks(snapshot)
			if err != nil {
				log.Printf("[ERROR] discover check tasks: %v", err)
			} else {
				active := map[string]bool{}
				for key := range running {
					active[key] = true
				}
				for key := range discovered {
					active[key] = true
				}
				for key := range nextDue {
					if !active[key] {
						delete(nextDue, key)
					}
				}
				for key, task := range discovered {
					if _, exists := nextDue[key]; !exists && !hasRunning(running, key) {
						nextDue[key] = now
					} else if refreshRequested && !hasRunning(running, key) {
						candidate := now.Add(time.Duration(groupInterval(snapshot, task.group.ID)) * time.Second)
						if due, exists := nextDue[key]; !exists || candidate.Before(due) {
							nextDue[key] = candidate
						}
					}
				}
				tasks = discovered
				lastDiscovery = now
			}
		}
		if forceCheck {
			for key := range tasks {
				if !hasRunning(running, key) {
					nextDue[key] = now
				}
			}
		}
		maxWorkers := clamp(snapshot.MaxWorkers, defaultMaxWorkers, 1, 128)
		for len(running) < maxWorkers {
			key, due, ok := earliestDue(tasks, nextDue, running, runningEndpoints, now)
			if !ok || due.After(now) {
				break
			}
			task := tasks[key]
			delete(nextDue, key)
			if !m.modelIsMonitorable(m.snapshotConfig(), task.endpoint.ID, task.group.ID, task.modelID) {
				delete(tasks, key)
				continue
			}
			running[key] = task
			runningEndpoints[task.endpoint.ID] = true
			m.stateMu.Lock()
			m.checkRunning = true
			m.lastCheckStartedAt = nowUnix()
			m.stateMu.Unlock()
			go func(key string, task checkTask) {
				var record Record
				func() {
					defer func() {
						if recovered := recover(); recovered != nil {
							record = fallbackRecord(task, fmt.Errorf("%v", recovered))
						}
					}()
					record = m.checkModel(task.endpoint, task.group, task.modelID)
				}()
				results <- checkResult{key: key, task: task, record: record}
			}(key, task)
		}
		m.setCheckRunning(len(running) > 0)

		waitDuration := 5 * time.Second
		if due, ok := earliestNextDue(nextDue); ok {
			if until := time.Until(due); until < waitDuration {
				waitDuration = maxDuration(200*time.Millisecond, until)
			}
		}
		if len(running) > 0 {
			waitDuration = minDuration(waitDuration, time.Second)
		}
		select {
		case result := <-results:
			delete(running, result.key)
			delete(runningEndpoints, result.task.endpoint.ID)
			m.storeCheckRecord(result.record)
			current := m.snapshotConfig()
			if _, exists := tasks[result.key]; exists && m.modelIsMonitorable(current, result.task.endpoint.ID, result.task.group.ID, result.task.modelID) {
				nextDue[result.key] = time.Now().Add(time.Duration(groupInterval(current, result.task.group.ID)) * time.Second)
			} else {
				delete(tasks, result.key)
				delete(nextDue, result.key)
			}
			m.setCheckRunning(len(running) > 0)
			if lastPrune.IsZero() || time.Since(lastPrune) >= 5*time.Minute {
				if err := m.pruneHistory(clamp(current.HistoryRetentionHours, defaultRetentionHours, 24, 2160)); err != nil {
					log.Printf("[ERROR] prune history: %v", err)
				}
				lastPrune = time.Now()
			}
		case <-m.wake:
		case <-time.After(waitDuration):
		}
	}
}

func hasRunning(running map[string]checkTask, key string) bool {
	_, ok := running[key]
	return ok
}

func earliestDue(tasks map[string]checkTask, due map[string]time.Time, running map[string]checkTask, runningEndpoints map[string]bool, now time.Time) (string, time.Time, bool) {
	key := ""
	var selected time.Time
	for candidate, when := range due {
		if _, exists := tasks[candidate]; !exists || hasRunning(running, candidate) {
			continue
		}
		task := tasks[candidate]
		if runningEndpoints[task.endpoint.ID] {
			continue
		}
		if key == "" || when.Before(selected) {
			key, selected = candidate, when
		}
	}
	return key, selected, key != "" && !selected.After(now)
}

func earliestNextDue(due map[string]time.Time) (time.Time, bool) {
	var selected time.Time
	for _, when := range due {
		if selected.IsZero() || when.Before(selected) {
			selected = when
		}
	}
	return selected, !selected.IsZero()
}

func maxDuration(a, b time.Duration) time.Duration {
	if a > b {
		return a
	}
	return b
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

type currentCount struct {
	Total         int      `json:"total"`
	OK            int      `json:"ok"`
	Fluctuation   int      `json:"fluctuation"`
	Timeout       int      `json:"timeout"`
	Error         int      `json:"error"`
	AvgTTFTMs     *float64 `json:"avg_ttft_ms"`
	FastestTTFTMs *float64 `json:"fastest_ttft_ms"`
}

func countRecords(records []Record) currentCount {
	result := currentCount{Total: len(records)}
	ttfts := []float64{}
	for _, record := range records {
		if record.Status == "ok" || record.Status == "fluctuation" {
			result.OK++
			if record.TTFTMs != nil {
				ttfts = append(ttfts, *record.TTFTMs)
			}
		}
		if record.Status == "fluctuation" {
			result.Fluctuation++
		}
		if record.Status == "timeout" {
			result.Timeout++
		}
	}
	result.Error = result.Total - result.OK
	if len(ttfts) > 0 {
		total := 0.0
		fastest := ttfts[0]
		for _, value := range ttfts {
			total += value
			if value < fastest {
				fastest = value
			}
		}
		result.AvgTTFTMs = floatPointer(roundFloat(total/float64(len(ttfts)), 1))
		result.FastestTTFTMs = floatPointer(roundFloat(fastest, 1))
	}
	return result
}

func windowStatFor(values map[string]windowStat, key string) windowStat {
	if value, ok := values[key]; ok {
		return value
	}
	return emptyWindowStat()
}

func recentToAny(values []recentResult) []any {
	result := make([]any, len(values))
	for index, value := range values {
		result[index] = value
	}
	return result
}

func (m *Monitor) stateSnapshot() (map[string]Record, map[string][]string, map[string]string, map[string]float64, float64, float64, bool, float64) {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	latest := map[string]Record{}
	for key, record := range m.latestResults {
		latest[key] = record
	}
	known := map[string][]string{}
	for endpointID, models := range m.knownModels {
		known[endpointID] = append([]string(nil), models...)
	}
	errorsByEndpoint := map[string]string{}
	for endpointID, errorText := range m.endpointErrors {
		errorsByEndpoint[endpointID] = errorText
	}
	pings := map[string]float64{}
	for endpointID, ping := range m.endpointPingMs {
		pings[endpointID] = ping
	}
	return latest, known, errorsByEndpoint, pings, m.lastCheckStartedAt, m.lastCheckFinishedAt, m.checkRunning, m.historyValidAfter
}

func publicEndpoint(endpoint Endpoint) map[string]any {
	return map[string]any{
		"id": endpoint.ID, "name": endpoint.Name, "group_id": endpoint.GroupID,
		"enabled": endpoint.Enabled, "test_prompt": endpoint.TestPrompt, "max_tokens": endpoint.MaxTokens,
	}
}

func dashboardDisplayModel(group Group, rows []map[string]any) map[string]any {
	if group.DefaultModel != nil {
		for _, row := range rows {
			if stringValue(row["endpoint_id"]) == group.DefaultModel.EndpointID && stringValue(row["model"]) == group.DefaultModel.ModelID {
				return row
			}
		}
		return nil
	}
	for _, row := range rows {
		if stringValue(row["group_id"]) == group.ID {
			return row
		}
	}
	return nil
}

func (m *Monitor) buildDashboardPayload() (map[string]any, error) {
	snapshot := m.snapshotConfig()
	latest, known, endpointErrors, pings, started, finished, running, validAfter := m.stateSnapshot()
	groupsByID := map[string]Group{}
	groupOrder := map[string]int{}
	for index, group := range snapshot.Groups {
		groupsByID[group.ID] = group
		groupOrder[group.ID] = index
	}
	endpointsByID := map[string]Endpoint{}
	activeGroupIDs := map[string]bool{}
	for _, group := range snapshot.Groups {
		if group.Enabled {
			activeGroupIDs[group.ID] = true
		}
	}
	activeEndpointIDs := map[string]bool{}
	for _, endpoint := range snapshot.Endpoints {
		endpointsByID[endpoint.ID] = endpoint
		if endpoint.Enabled && activeGroupIDs[endpoint.GroupID] {
			activeEndpointIDs[endpoint.ID] = true
		}
	}
	ignored := map[string]bool{}
	for _, item := range snapshot.IgnoredModels {
		ignored[modelKey(item.EndpointID, item.ModelID)] = true
	}
	records := []Record{}
	for _, record := range latest {
		if activeEndpointIDs[record.EndpointID] && !ignored[modelKey(record.EndpointID, record.Model)] && record.CheckedAtTS >= validAfter {
			records = append(records, record)
		}
	}
	globalWindows, err := m.queryGlobalWindows(activeEndpointIDs, ignored, validAfter)
	if err != nil {
		return nil, err
	}
	groupWindows, err := m.queryGroupedWindows([]string{"group_id"}, activeEndpointIDs, ignored, validAfter)
	if err != nil {
		return nil, err
	}
	endpointWindows, err := m.queryGroupedWindows([]string{"endpoint_id"}, activeEndpointIDs, ignored, validAfter)
	if err != nil {
		return nil, err
	}
	modelWindows, err := m.queryGroupedWindows([]string{"endpoint_id", "model_id"}, activeEndpointIDs, ignored, validAfter)
	if err != nil {
		return nil, err
	}
	recent, err := m.queryRecentModelResults(activeEndpointIDs, ignored, 60, validAfter)
	if err != nil {
		return nil, err
	}

	rows := []map[string]any{}
	for _, record := range records {
		endpoint := endpointsByID[record.EndpointID]
		group := groupsByID[record.GroupID]
		publicRecord := map[string]any{
			"endpoint_id": record.EndpointID, "endpoint_name": record.EndpointName,
			"group_id": record.GroupID, "group_name": record.GroupName, "model": record.Model,
			"status": record.Status, "ttft_ms": pointerValue(record.TTFTMs), "checked_at": record.CheckedAt,
			"checked_at_ts": record.CheckedAtTS, "endpoint_enabled": endpoint.Enabled, "group_enabled": group.Enabled,
			"rate_multiplier": pointerValue(record.RateMultiplier), "endpoint_ping_ms": valueOrNil(pings[record.EndpointID]),
			"windows": map[string]windowStat{}, "recent_results": recentToAny(recent[modelKey(record.EndpointID, record.Model)]),
		}
		if record.Error != "" {
			publicRecord["error"] = record.Error
		} else {
			publicRecord["error"] = nil
		}
		if record.ProbeProtocol != "" {
			publicRecord["probe_protocol"] = record.ProbeProtocol
		}
		rowWindows := map[string]windowStat{}
		for _, window := range windows {
			rowWindows[window.Key] = windowStatFor(modelWindows[window.Key], modelKey(record.EndpointID, record.Model))
		}
		publicRecord["windows"] = rowWindows
		rows = append(rows, publicRecord)
	}
	sort.Slice(rows, func(i, j int) bool {
		left, right := rows[i], rows[j]
		li, ri := groupOrder[stringValue(left["group_id"])], groupOrder[stringValue(right["group_id"])]
		if li != ri {
			return li < ri
		}
		if stringValue(left["endpoint_name"]) != stringValue(right["endpoint_name"]) {
			return stringValue(left["endpoint_name"]) < stringValue(right["endpoint_name"])
		}
		return stringValue(left["model"]) < stringValue(right["model"])
	})

	groupPayload := []map[string]any{}
	for _, group := range snapshot.Groups {
		groupRecords := []Record{}
		for _, record := range records {
			if record.GroupID == group.ID {
				groupRecords = append(groupRecords, record)
			}
		}
		groupPayload = append(groupPayload, map[string]any{
			"id": group.ID, "name": group.Name, "description": group.Description, "enabled": group.Enabled,
			"icon": group.Icon, "check_interval": group.CheckInterval, "timeout": group.Timeout, "default_model": group.DefaultModel,
			"display_model": dashboardDisplayModel(group, rows),
			"current":       countRecords(groupRecords), "windows": groupedWindowPayload(groupWindows, group.ID),
		})
	}
	endpointPayload := []map[string]any{}
	for _, endpoint := range snapshot.Endpoints {
		endpointRecords := []Record{}
		for _, record := range records {
			if record.EndpointID == endpoint.ID {
				endpointRecords = append(endpointRecords, record)
			}
		}
		endpointData := publicEndpoint(endpoint)
		endpointData["group_name"] = groupsByID[endpoint.GroupID].Name
		endpointData["known_model_count"] = len(known[endpoint.ID])
		endpointData["ping_ms"] = valueOrNil(pings[endpoint.ID])
		if endpointErrors[endpoint.ID] != "" {
			endpointData["fetch_error"] = endpointErrors[endpoint.ID]
		} else {
			endpointData["fetch_error"] = nil
		}
		endpointData["current"] = countRecords(endpointRecords)
		endpointData["windows"] = groupedWindowPayload(endpointWindows, endpoint.ID)
		endpointPayload = append(endpointPayload, endpointData)
	}
	sort.Slice(endpointPayload, func(i, j int) bool {
		li, ri := groupOrder[stringValue(endpointPayload[i]["group_id"])], groupOrder[stringValue(endpointPayload[j]["group_id"])]
		if li != ri {
			return li < ri
		}
		return stringValue(endpointPayload[i]["name"]) < stringValue(endpointPayload[j]["name"])
	})
	return map[string]any{
		"service": map[string]any{
			"listen_port": m.listenPort, "check_interval": snapshot.CheckInterval,
			"last_check_started_at": formatTime(started), "last_check_finished_at": formatTime(finished),
			"last_check_finished_ts": valueOrNil(finished), "check_running": running,
		},
		"windows": windows,
		"summary": map[string]any{
			"current": countRecords(records), "windows": globalWindows,
			"ignored_count": len(snapshot.IgnoredModels), "api_count": len(snapshot.Endpoints), "group_count": len(snapshot.Groups),
		},
		"groups": groupPayload, "endpoints": endpointPayload, "models": rows,
	}, nil
}

func valueOrNil(value float64) any {
	if value == 0 {
		return nil
	}
	return value
}

func groupedWindowPayload(values map[string]map[string]windowStat, key string) map[string]windowStat {
	result := map[string]windowStat{}
	for _, window := range windows {
		result[window.Key] = windowStatFor(values[window.Key], key)
	}
	return result
}

func (m *Monitor) buildAdminPayload() (map[string]any, error) {
	snapshot := m.snapshotConfig()
	_, known, endpointErrors, _, _, finished, running, _ := m.stateSnapshot()
	ignored := map[string]bool{}
	for _, item := range snapshot.IgnoredModels {
		ignored[modelKey(item.EndpointID, item.ModelID)] = true
	}
	selected := map[string]bool{}
	for _, item := range snapshot.QQPush.SelectedModels {
		selected[modelKey(item.EndpointID, item.ModelID)] = true
	}
	modelPayload := []map[string]any{}
	for _, endpoint := range snapshot.Endpoints {
		models := []map[string]any{}
		for _, modelID := range known[endpoint.ID] {
			models = append(models, map[string]any{
				"id": modelID, "ignored": ignored[modelKey(endpoint.ID, modelID)],
				"qq_selected": selected[modelKey(endpoint.ID, modelID)],
			})
		}
		modelPayload = append(modelPayload, map[string]any{
			"endpoint_id": endpoint.ID, "endpoint_name": endpoint.Name, "models": models,
			"fetch_error": endpointErrors[endpoint.ID],
		})
	}
	return map[string]any{
		"config": m.adminConfigView(snapshot),
		"models": modelPayload,
		"runtime": map[string]any{
			"check_running": running, "last_check_finished_at": formatTime(finished), "qq_push": m.qqRuntimePayload(),
		},
	}, nil
}

func (m *Monitor) legacyResultsPayload() map[string]any {
	m.stateMu.RLock()
	defer m.stateMu.RUnlock()
	models := make([]any, 0, len(m.latestResults))
	for _, record := range m.latestResults {
		models = append(models, record)
	}
	return map[string]any{"last_check": formatTime(m.lastCheckFinishedAt), "models": models}
}

func (m *Monitor) updateQQRuntime(values map[string]any) {
	m.stateMu.Lock()
	defer m.stateMu.Unlock()
	for key, value := range values {
		switch key {
		case "capture_status":
			m.qqRuntime.CaptureStatus = stringValue(value)
		case "capture_message":
			m.qqRuntime.CaptureMessage = stringValue(value)
		case "capture_code":
			m.qqRuntime.CaptureCode = stringValue(value)
		case "capture_started_at":
			m.qqRuntime.CaptureStartedAt = floatValue(value)
		case "last_push_at":
			m.qqRuntime.LastPushAt = floatValue(value)
		case "last_push_ok":
			if value == nil {
				m.qqRuntime.LastPushOK = nil
			} else {
				parsed := boolValue(value, false)
				m.qqRuntime.LastPushOK = &parsed
			}
		case "last_push_error":
			m.qqRuntime.LastPushError = stringValue(value)
		case "next_push_at":
			m.qqRuntime.NextPushAt = floatValue(value)
		case "mention_status":
			m.qqRuntime.MentionStatus = stringValue(value)
		case "mention_message":
			m.qqRuntime.MentionMessage = stringValue(value)
		case "last_mention_at":
			m.qqRuntime.LastMentionAt = floatValue(value)
		case "last_mention_ok":
			if value == nil {
				m.qqRuntime.LastMentionOK = nil
			} else {
				parsed := boolValue(value, false)
				m.qqRuntime.LastMentionOK = &parsed
			}
		case "last_mention_error":
			m.qqRuntime.LastMentionError = stringValue(value)
		}
	}
}

func (m *Monitor) qqRuntimePayload() map[string]any {
	m.stateMu.RLock()
	runtime := m.qqRuntime
	m.stateMu.RUnlock()
	return map[string]any{
		"capture_status":     runtime.CaptureStatus,
		"capture_message":    runtime.CaptureMessage,
		"capture_code":       valueOrStringNil(runtime.CaptureCode),
		"capture_started_at": formatTime(runtime.CaptureStartedAt),
		"last_push_at":       formatTime(runtime.LastPushAt),
		"last_push_ok":       pointerBoolValue(runtime.LastPushOK),
		"last_push_error":    valueOrStringNil(runtime.LastPushError),
		"next_push_at":       formatTime(runtime.NextPushAt),
		"mention_status":     runtime.MentionStatus,
		"mention_message":    runtime.MentionMessage,
		"last_mention_at":    formatTime(runtime.LastMentionAt),
		"last_mention_ok":    pointerBoolValue(runtime.LastMentionOK),
		"last_mention_error": valueOrStringNil(runtime.LastMentionError),
	}
}

func valueOrStringNil(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func pointerBoolValue(value *bool) any {
	if value == nil {
		return nil
	}
	return *value
}

func (m *Monitor) updateQQGroupOpenID(groupOpenID string) error {
	groupOpenID = strings.TrimSpace(groupOpenID)
	if groupOpenID == "" {
		return errors.New("未获取到目标群标识")
	}
	current := m.snapshotConfig()
	current.QQPush.GroupOpenID = groupOpenID
	_, err := m.saveConfig(current)
	return err
}

func (m *Monitor) unbindQQGroup() error {
	m.cancelQQCapture()
	current := m.snapshotConfig()
	current.QQPush.GroupOpenID = ""
	if _, err := m.saveConfig(current); err != nil {
		return err
	}
	m.updateQQRuntime(map[string]any{
		"capture_status": "idle", "capture_message": "尚未绑定目标群", "capture_code": nil,
		"next_push_at": nil, "mention_status": "disabled", "mention_message": "尚未绑定目标群",
	})
	m.signalQQ()
	return nil
}

func qqErrorText(status int, payload map[string]any) string {
	detail := firstNonEmpty(stringValue(payload["message"]), stringValue(payload["msg"]), stringValue(payload["error"]))
	code := stringValue(payload["code"])
	if detail != "" && code != "" {
		return fmt.Sprintf("HTTP %d / %s: %s", status, code, detail)
	}
	if detail != "" {
		return fmt.Sprintf("HTTP %d: %s", status, detail)
	}
	return fmt.Sprintf("HTTP %d: QQ 接口调用失败", status)
}

var qqHTTPClient = &http.Client{Timeout: 30 * time.Second}

func qqJSON(ctx context.Context, method, target string, payload any, headers http.Header) (int, map[string]any, error) {
	var body io.Reader
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
		body = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return 0, nil, err
	}
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := qqHTTPClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, err := readBodyLimit(resp.Body, 256*1024)
	if err != nil {
		return resp.StatusCode, nil, err
	}
	result := map[string]any{}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &result); err != nil {
			result["message"] = detailFromBody(raw)
		}
	}
	return resp.StatusCode, result, nil
}

func (m *Monitor) invalidateQQToken() {
	m.qqTokenMu.Lock()
	m.qqToken = ""
	m.qqTokenExp = time.Time{}
	m.qqTokenID = ""
	m.qqTokenMu.Unlock()
}

func (m *Monitor) qqAccessToken(ctx context.Context, appID, appSecret string, force bool) (string, error) {
	appID, appSecret = strings.TrimSpace(appID), strings.TrimSpace(appSecret)
	if appID == "" || appSecret == "" {
		return "", errors.New("请先填写机器人 AppID 和 AppSecret")
	}
	identity := appID + "\x00" + appSecret
	m.qqTokenMu.Lock()
	if !force && m.qqToken != "" && m.qqTokenID == identity && time.Now().Before(m.qqTokenExp.Add(-time.Minute)) {
		token := m.qqToken
		m.qqTokenMu.Unlock()
		return token, nil
	}
	m.qqTokenMu.Unlock()

	m.qqTokenMu.Lock()
	defer m.qqTokenMu.Unlock()
	if !force && m.qqToken != "" && m.qqTokenID == identity && time.Now().Before(m.qqTokenExp.Add(-time.Minute)) {
		return m.qqToken, nil
	}
	status, payload, err := qqJSON(ctx, http.MethodPost, "https://bots.qq.com/app/getAppAccessToken", map[string]string{
		"appId": appID, "clientSecret": appSecret,
	}, http.Header{})
	if err != nil {
		return "", err
	}
	token := stringValue(payload["access_token"])
	if status != http.StatusOK || token == "" {
		return "", errors.New(qqErrorText(status, payload))
	}
	expires := clamp(intValue(payload["expires_in"], 7200), 7200, 60, 86400)
	m.qqToken, m.qqTokenID, m.qqTokenExp = token, identity, time.Now().Add(time.Duration(expires)*time.Second)
	return token, nil
}

func (m *Monitor) qqGatewayURL(ctx context.Context, appID, appSecret string) (string, string, error) {
	token, err := m.qqAccessToken(ctx, appID, appSecret, false)
	if err != nil {
		return "", "", err
	}
	status, payload, err := qqJSON(ctx, http.MethodGet, "https://api.sgroup.qq.com/gateway", nil, http.Header{
		"Authorization": []string{"QQBot " + token},
	})
	if err != nil {
		return "", "", err
	}
	gateway := stringValue(payload["url"])
	if status < 200 || status >= 300 || gateway == "" {
		return "", "", errors.New(qqErrorText(status, payload))
	}
	return gateway, token, nil
}

func (m *Monitor) qqSendText(snapshot Config, content, replyTo string) error {
	settings := snapshot.QQPush
	if settings.GroupOpenID == "" {
		return errors.New("请先绑定目标群")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	token, err := m.qqAccessToken(ctx, settings.AppID, settings.AppSecret, false)
	if err != nil {
		return err
	}
	path := "https://api.sgroup.qq.com/v2/groups/" + url.PathEscape(settings.GroupOpenID) + "/messages"
	messageSeq := int(time.Now().UnixMilli() % 65536)
	if replyTo != "" {
		messageSeq = 1
	}
	payload := map[string]any{"content": truncateRunes(content, maxQQMessageLength), "msg_type": 0, "msg_seq": messageSeq}
	if replyTo != "" {
		payload["msg_id"] = truncateRunes(replyTo, 200)
	}
	headers := http.Header{"Authorization": []string{"QQBot " + token}}
	status, response, err := qqJSON(ctx, http.MethodPost, path, payload, headers)
	if err != nil {
		return err
	}
	if status == http.StatusUnauthorized {
		m.invalidateQQToken()
		token, err = m.qqAccessToken(ctx, settings.AppID, settings.AppSecret, true)
		if err != nil {
			return err
		}
		headers.Set("Authorization", "QQBot "+token)
		status, response, err = qqJSON(ctx, http.MethodPost, path, payload, headers)
		if err != nil {
			return err
		}
	}
	if status < 200 || status >= 300 {
		return errors.New(qqErrorText(status, response))
	}
	return nil
}

func truncateRunes(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit])
}

func (m *Monitor) buildQQStatusMessage(snapshot Config, records []Record, test bool) (string, error) {
	selected := snapshot.QQPush.SelectedModels
	if len(selected) == 0 {
		return "", errors.New("请至少选择一个推送模型")
	}
	if records == nil {
		m.stateMu.RLock()
		records = make([]Record, 0, len(m.latestResults))
		for _, record := range m.latestResults {
			records = append(records, record)
		}
		m.stateMu.RUnlock()
	}
	byKey := map[string]Record{}
	for _, record := range records {
		byKey[modelKey(record.EndpointID, record.Model)] = record
	}
	endpointNames := map[string]string{}
	for _, endpoint := range snapshot.Endpoints {
		endpointNames[endpoint.ID] = endpoint.Name
	}
	lines := []string{}
	okCount, fluctuationCount, timeoutCount, errorCount, waitingCount := 0, 0, 0, 0, 0
	for _, item := range selected {
		endpointName := firstNonEmpty(endpointNames[item.EndpointID], item.EndpointID, "未知 API")
		label := endpointName + " / " + item.ModelID
		record, exists := byKey[modelKey(item.EndpointID, item.ModelID)]
		if !exists {
			waitingCount++
			lines = append(lines, "[等待] "+label)
			continue
		}
		switch record.Status {
		case "ok":
			okCount++
			suffix := ""
			if record.TTFTMs != nil {
				suffix = fmt.Sprintf(" %.0fms", *record.TTFTMs)
			}
			lines = append(lines, "[正常] "+label+suffix)
		case "fluctuation":
			fluctuationCount++
			suffix := ""
			if record.TTFTMs != nil {
				suffix = fmt.Sprintf(" - 延迟 %.1f秒", *record.TTFTMs/1000)
			}
			lines = append(lines, "[波动] "+label+suffix)
		case "timeout":
			timeoutCount++
			lines = append(lines, "[超时] "+label+" - "+firstNonEmpty(strings.ReplaceAll(record.Error, "\n", " "), "检测超时"))
		default:
			errorCount++
			lines = append(lines, "[异常] "+label+" - "+firstNonEmpty(strings.ReplaceAll(record.Error, "\n", " "), "检测失败"))
		}
	}
	title := "模型渠道状态"
	if test {
		title = "[测试推送] " + title
	}
	messageLines := []string{
		title,
		formatTimeString(nowUnix()),
		fmt.Sprintf("正常 %d | 波动 %d | 超时 %d | 异常 %d | 等待 %d", okCount, fluctuationCount, timeoutCount, errorCount, waitingCount),
		"",
	}
	messageLines = append(messageLines, lines...)
	message := strings.Join(messageLines, "\n")
	if len([]rune(message)) > maxQQMessageLength {
		message = truncateRunes(message, maxQQMessageLength-25) + "\n...(内容已截断)"
	}
	return message, nil
}

func (m *Monitor) pushQQStatus(test bool) error {
	snapshot := m.snapshotConfig()
	message, err := m.buildQQStatusMessage(snapshot, nil, test)
	if err == nil {
		err = m.qqSendText(snapshot, message, "")
	}
	now := nowUnix()
	if err != nil {
		m.updateQQRuntime(map[string]any{"last_push_at": now, "last_push_ok": false, "last_push_error": truncateError(err.Error())})
		log.Printf("[ERROR] QQ group status push failed: %v", err)
		return err
	}
	m.updateQQRuntime(map[string]any{"last_push_at": now, "last_push_ok": true, "last_push_error": nil})
	log.Printf("[INFO] QQ group status push succeeded (%d chars)", len([]rune(message)))
	return nil
}

func qqReady(settings QQPushSettings) bool {
	return strings.TrimSpace(settings.AppID) != "" && strings.TrimSpace(settings.AppSecret) != "" && strings.TrimSpace(settings.GroupOpenID) != "" && len(settings.SelectedModels) > 0
}

func (m *Monitor) qqPushLoop() {
	var signature string
	var nextDue time.Time
	for {
		settings := m.snapshotConfig().QQPush
		selectedEncoded, _ := json.Marshal(settings.SelectedModels)
		currentSignature := fmt.Sprintf("%t|%s|%s|%s|%d|%s", settings.Enabled, settings.AppID, settings.AppSecret, settings.GroupOpenID, settings.IntervalMinutes, selectedEncoded)
		interval := time.Duration(clamp(settings.IntervalMinutes, defaultQQPushIntervalMinutes, 1, 1440)) * time.Minute
		ready := qqReady(settings)
		if currentSignature != signature {
			signature = currentSignature
			if settings.Enabled && ready {
				nextDue = time.Now().Add(interval)
			} else {
				nextDue = time.Time{}
			}
		}
		if !settings.Enabled || !ready {
			nextDue = time.Time{}
		} else if nextDue.IsZero() {
			nextDue = time.Now().Add(interval)
		} else if !time.Now().Before(nextDue) {
			_ = m.pushQQStatus(false)
			nextDue = time.Now().Add(interval)
		}
		if nextDue.IsZero() {
			m.updateQQRuntime(map[string]any{"next_push_at": nil})
		} else {
			m.updateQQRuntime(map[string]any{"next_push_at": float64(nextDue.UnixNano()) / 1e9})
		}
		wait := 5 * time.Second
		if !nextDue.IsZero() && time.Until(nextDue) < wait {
			wait = maxDuration(200*time.Millisecond, time.Until(nextDue))
		}
		select {
		case <-m.qqPushWake:
		case <-time.After(wait):
		}
	}
}

func (m *Monitor) claimQQMention(groupOpenID, messageID string) bool {
	key := groupOpenID + "|" + messageID
	now := time.Now()
	m.qqMentionSeenMu.Lock()
	defer m.qqMentionSeenMu.Unlock()
	cutoff := now.Add(-qqMentionDedupWindow)
	for seenKey, timestamp := range m.qqMentionSeen {
		if timestamp.Before(cutoff) {
			delete(m.qqMentionSeen, seenKey)
		}
	}
	if _, exists := m.qqMentionSeen[key]; exists {
		return false
	}
	m.qqMentionSeen[key] = now
	return true
}

func (m *Monitor) qqMentionTarget(settings QQPushSettings, eventType string, raw map[string]any) (string, string, bool) {
	if eventType != "GROUP_AT_MESSAGE_CREATE" || !settings.MentionEnabled || raw == nil {
		return "", "", false
	}
	groupOpenID := stringValue(raw["group_openid"])
	messageID := stringValue(raw["id"])
	if groupOpenID == "" || groupOpenID != settings.GroupOpenID || messageID == "" {
		return "", "", false
	}
	return groupOpenID, messageID, true
}

type qqMessageHandler func(string, map[string]any) bool

func (m *Monitor) runQQSession(ctx context.Context, appID, appSecret, logTag string, onMessage qqMessageHandler, onConnected func()) error {
	var seq int
	var seqMu sync.RWMutex
	currentSeq := func() int {
		seqMu.RLock()
		defer seqMu.RUnlock()
		return seq
	}
	updateSeq := func(value int) {
		seqMu.Lock()
		if value > seq {
			seq = value
		}
		seqMu.Unlock()
	}
	backoff := []time.Duration{2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second, 60 * time.Second}
	backoffIndex := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		gatewayCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		gateway, token, err := m.qqGatewayURL(gatewayCtx, appID, appSecret)
		cancel()
		if err != nil {
			if !sleepContext(ctx, backoff[backoffIndex]) {
				return ctx.Err()
			}
			backoffIndex = minInt(backoffIndex+1, len(backoff)-1)
			log.Printf("[ERROR] %s gateway lookup failed: %v", logTag, err)
			continue
		}
		dialer := *websocket.DefaultDialer
		dialer.HandshakeTimeout = 20 * time.Second
		conn, _, err := dialer.DialContext(ctx, gateway, http.Header{"User-Agent": []string{"model-monitor/3.0"}})
		if err != nil {
			if !sleepContext(ctx, backoff[backoffIndex]) {
				return ctx.Err()
			}
			backoffIndex = minInt(backoffIndex+1, len(backoff)-1)
			log.Printf("[ERROR] %s gateway connection failed: %v", logTag, err)
			continue
		}
		connectionCtx, connectionCancel := context.WithCancel(ctx)
		heartbeatDone := make(chan struct{})
		heartbeatInterval := make(chan time.Duration, 1)
		connectionErr := make(chan error, 1)
		connectionWatchDone := make(chan struct{})
		var writeMu sync.Mutex
		writeJSON := func(payload any) error {
			writeMu.Lock()
			defer writeMu.Unlock()
			return conn.WriteJSON(payload)
		}
		go func() {
			select {
			case <-connectionCtx.Done():
				_ = conn.Close()
			case <-connectionWatchDone:
			}
		}()
		go func() {
			defer close(heartbeatDone)
			interval := 30 * time.Second
			timer := time.NewTimer(interval)
			defer timer.Stop()
			for {
				select {
				case <-connectionCtx.Done():
					return
				case nextInterval := <-heartbeatInterval:
					if nextInterval >= time.Second {
						interval = nextInterval
					}
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					timer.Reset(interval)
				case <-timer.C:
					_ = writeJSON(map[string]any{"op": 1, "d": nullableSeq(currentSeq())})
					timer.Reset(interval)
				}
			}
		}()
		connected := false
		for {
			var payload struct {
				Op int             `json:"op"`
				D  json.RawMessage `json:"d"`
				S  *int            `json:"s"`
				T  string          `json:"t"`
			}
			if err := conn.ReadJSON(&payload); err != nil {
				connectionErr <- err
				break
			}
			if payload.S != nil {
				updateSeq(*payload.S)
			}
			switch payload.Op {
			case 10:
				var hello struct {
					HeartbeatInterval int `json:"heartbeat_interval"`
				}
				_ = json.Unmarshal(payload.D, &hello)
				interval := time.Duration(float64(hello.HeartbeatInterval)*0.8) * time.Millisecond
				if interval < 5*time.Second {
					interval = 5 * time.Second
				}
				// The goroutine uses a time.After per tick. Setting a connection
				// deadline here would interfere with long-lived gateway reads.
				_ = writeJSON(map[string]any{"op": 2, "d": map[string]any{
					"token": "QQBot " + token, "intents": 33554432, "shard": []int{0, 1},
					"properties": map[string]string{"$os": "linux", "$browser": "model-monitor", "$device": "model-monitor"},
				}})
				select {
				case heartbeatInterval <- interval:
				default:
				}
			case 0:
				if payload.T == "READY" {
					var ready struct {
						SessionID string `json:"session_id"`
					}
					_ = json.Unmarshal(payload.D, &ready)
					connected = true
					if onConnected != nil {
						onConnected()
					}
				} else if payload.T == "RESUMED" {
					connected = true
					if onConnected != nil {
						onConnected()
					}
				} else if connected && payload.T != "" {
					var raw map[string]any
					if json.Unmarshal(payload.D, &raw) == nil && onMessage != nil && onMessage(payload.T, raw) {
						_ = conn.Close()
						close(connectionWatchDone)
						connectionCancel()
						<-heartbeatDone
						return nil
					}
				}
			case 7:
				_ = conn.Close()
			case 9:
				seqMu.Lock()
				seq = 0
				seqMu.Unlock()
				_ = conn.Close()
			case 11:
				// Heartbeat acknowledged.
			}
		}
		_ = conn.Close()
		close(connectionWatchDone)
		connectionCancel()
		select {
		case <-heartbeatDone:
		case <-time.After(time.Second):
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := <-connectionErr; err != nil {
			log.Printf("[ERROR] %s gateway disconnected: %v", logTag, err)
		}
		backoffIndex = minInt(backoffIndex+1, len(backoff)-1)
		if !sleepContext(ctx, backoff[backoffIndex]) {
			return ctx.Err()
		}
	}
}

func nullableSeq(value int) any {
	if value == 0 {
		return nil
	}
	return value
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (m *Monitor) startQQCapture() bool {
	snapshot := m.snapshotConfig()
	if snapshot.QQPush.AppID == "" || snapshot.QQPush.AppSecret == "" {
		return false
	}
	m.qqCaptureMu.Lock()
	if m.qqCaptureCancel != nil {
		m.qqCaptureMu.Unlock()
		return false
	}
	code := fmt.Sprintf("%06d", time.Now().UnixNano()%900000+100000)
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	m.qqCaptureCancel = cancel
	m.qqCaptureMu.Unlock()
	m.updateQQRuntime(map[string]any{"capture_status": "connecting", "capture_message": "正在连接 QQ 机器人网关", "capture_code": code, "capture_started_at": nowUnix()})
	m.signalQQ()
	go func() {
		defer func() {
			m.qqCaptureMu.Lock()
			m.qqCaptureCancel = nil
			m.qqCaptureMu.Unlock()
		}()
		connected := func() {
			m.updateQQRuntime(map[string]any{"capture_status": "waiting_message", "capture_message": "已连接，请在目标群 @机器人 发送：绑定 " + code})
		}
		handler := func(eventType string, raw map[string]any) bool {
			if eventType != "GROUP_AT_MESSAGE_CREATE" {
				return false
			}
			if !strings.Contains(stringValue(raw["content"]), code) || stringValue(raw["group_openid"]) == "" {
				return false
			}
			if err := m.updateQQGroupOpenID(stringValue(raw["group_openid"])); err != nil {
				m.updateQQRuntime(map[string]any{"capture_status": "error", "capture_message": err.Error(), "capture_code": nil})
				return true
			}
			m.updateQQRuntime(map[string]any{"capture_status": "bound", "capture_message": "目标群绑定成功", "capture_code": nil})
			return true
		}
		err := m.runQQSession(ctx, snapshot.QQPush.AppID, snapshot.QQPush.AppSecret, "ModelMonitorBind", handler, connected)
		if errors.Is(err, context.DeadlineExceeded) {
			m.updateQQRuntime(map[string]any{"capture_status": "error", "capture_message": "绑定超时，请重新开始绑定", "capture_code": nil})
		} else if errors.Is(err, context.Canceled) {
			m.stateMu.RLock()
			status := m.qqRuntime.CaptureStatus
			m.stateMu.RUnlock()
			if status != "bound" && status != "error" {
				m.updateQQRuntime(map[string]any{"capture_status": "cancelled", "capture_message": "已停止绑定", "capture_code": nil})
			}
		} else if err != nil {
			m.updateQQRuntime(map[string]any{"capture_status": "error", "capture_message": truncateError(err.Error()), "capture_code": nil})
		}
		m.signalQQ()
	}()
	return true
}

func (m *Monitor) cancelQQCapture() bool {
	m.qqCaptureMu.Lock()
	cancel := m.qqCaptureCancel
	m.qqCaptureMu.Unlock()
	if cancel == nil {
		return false
	}
	cancel()
	return true
}

func (m *Monitor) qqMentionLoop() {
	for {
		settings := m.snapshotConfig().QQPush
		m.qqCaptureMu.Lock()
		captureActive := m.qqCaptureCancel != nil
		m.qqCaptureMu.Unlock()
		ready := qqReady(settings)
		if !settings.MentionEnabled {
			m.updateQQRuntime(map[string]any{"mention_status": "disabled", "mention_message": "未启用 @ 查询"})
		} else if captureActive {
			m.updateQQRuntime(map[string]any{"mention_status": "paused", "mention_message": "绑定目标群期间暂停监听"})
		} else if !ready {
			m.updateQQRuntime(map[string]any{"mention_status": "incomplete", "mention_message": "@ 查询配置未完成"})
		} else {
			mentionConfig := m.snapshotConfig()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			handler := func(eventType string, raw map[string]any) bool {
				groupID, messageID, ok := m.qqMentionTarget(settings, eventType, raw)
				if !ok || !m.claimQQMention(groupID, messageID) {
					return false
				}
				message, err := m.buildQQStatusMessage(mentionConfig, nil, false)
				if err == nil {
					err = m.qqSendText(mentionConfig, message, messageID)
				}
				now := nowUnix()
				if err != nil {
					m.updateQQRuntime(map[string]any{"last_mention_at": now, "last_mention_ok": false, "last_mention_error": truncateError(err.Error())})
					log.Printf("[ERROR] QQ mention status reply failed: %v", err)
				} else {
					m.updateQQRuntime(map[string]any{"last_mention_at": now, "last_mention_ok": true, "last_mention_error": nil})
					log.Printf("[INFO] QQ mention status reply succeeded (%d chars)", len([]rune(message)))
				}
				return false
			}
			onConnected := func() {
				m.updateQQRuntime(map[string]any{"mention_status": "listening", "mention_message": "监听中，群成员 @机器人 时回复状态"})
			}
			go func() {
				done <- m.runQQSession(ctx, settings.AppID, settings.AppSecret, "ModelMonitorMention", handler, onConnected)
			}()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, context.Canceled) {
					m.updateQQRuntime(map[string]any{"mention_status": "error", "mention_message": truncateError(err.Error())})
				}
			case <-m.qqMentionWake:
				cancel()
				<-done
			}
			continue
		}
		select {
		case <-m.qqMentionWake:
		case <-time.After(5 * time.Second):
		}
	}
}

func (m *Monitor) setModelIgnored(endpointID, modelID string, shouldIgnore bool) (Config, error) {
	endpointID, modelID = strings.TrimSpace(endpointID), strings.TrimSpace(modelID)
	if endpointID == "" {
		return Config{}, errors.New("API 不存在")
	}
	if modelID == "" {
		return Config{}, errors.New("模型名称不能为空")
	}
	current := m.snapshotConfig()
	found := false
	for _, endpoint := range current.Endpoints {
		if endpoint.ID == endpointID {
			found = true
			break
		}
	}
	if !found {
		return Config{}, errors.New("API 不存在")
	}
	filtered := make([]ModelRef, 0, len(current.IgnoredModels)+1)
	for _, item := range current.IgnoredModels {
		if item.EndpointID == endpointID && item.ModelID == modelID {
			continue
		}
		filtered = append(filtered, item)
	}
	if shouldIgnore {
		filtered = append(filtered, ModelRef{EndpointID: endpointID, ModelID: modelID})
		selected := make([]ModelRef, 0, len(current.QQPush.SelectedModels))
		for _, item := range current.QQPush.SelectedModels {
			if item.EndpointID != endpointID || item.ModelID != modelID {
				selected = append(selected, item)
			}
		}
		current.QQPush.SelectedModels = selected
	}
	current.IgnoredModels = filtered
	saved, err := m.saveConfig(current)
	if err != nil {
		return Config{}, err
	}
	if shouldIgnore {
		m.stateMu.Lock()
		delete(m.latestResults, modelKey(endpointID, modelID))
		m.stateMu.Unlock()
	}
	return saved, nil
}

func (m *Monitor) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	if m.adminPassword == "" {
		return true
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Basic ") {
		m.sendAuthRequired(w)
		return false
	}
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(strings.TrimPrefix(auth, "Basic ")))
	if err != nil {
		m.sendAuthRequired(w)
		return false
	}
	username, password, ok := strings.Cut(string(decoded), ":")
	if !ok || username != m.adminUser || password != m.adminPassword {
		m.sendAuthRequired(w)
		return false
	}
	return true
}

func (m *Monitor) sendAuthRequired(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Basic realm="model-monitor-admin"`)
	m.writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "Authentication required"})
}

func (m *Monitor) writeJSON(w http.ResponseWriter, status int, payload any) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		status = http.StatusInternalServerError
		encoded = []byte(`{"error":"响应编码失败"}`)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(encoded)
}

func (m *Monitor) writePage(w http.ResponseWriter, page string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, page)
}

func decodeJSON(r *http.Request) (map[string]any, error) {
	if r.ContentLength > maxRequestBody {
		return nil, errors.New("请求体过大")
	}
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxRequestBody))
	decoder.UseNumber()
	var payload map[string]any
	if err := decoder.Decode(&payload); err != nil {
		return nil, err
	}
	if payload == nil {
		return map[string]any{}, nil
	}
	return payload, nil
}

func configFromMap(value map[string]any) (Config, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return Config{}, err
	}
	var config Config
	if err := json.Unmarshal(encoded, &config); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (m *Monitor) handleGET(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/api/dashboard":
		payload, err := m.buildDashboardPayload()
		if err != nil {
			m.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		m.writeJSON(w, http.StatusOK, payload)
	case "/api/results":
		m.writeJSON(w, http.StatusOK, m.legacyResultsPayload())
	case "/api/admin/config":
		if !m.requireAdmin(w, r) {
			return
		}
		payload, err := m.buildAdminPayload()
		if err != nil {
			m.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}
		m.writeJSON(w, http.StatusOK, payload)
	case "/api/admin/qq/status":
		if !m.requireAdmin(w, r) {
			return
		}
		snapshot := m.snapshotConfig()
		m.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "qq_push": m.adminConfigView(snapshot)["qq_push"], "runtime": m.qqRuntimePayload()})
	case "/healthz":
		m.writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case "/admin":
		if !m.requireAdmin(w, r) {
			return
		}
		m.writePage(w, adminPage)
	default:
		m.writePage(w, dashboardPage)
	}
}

func (m *Monitor) handlePOST(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/api/admin/") && !m.requireAdmin(w, r) {
		return
	}
	switch r.URL.Path {
	case "/api/admin/config":
		payload, err := decodeJSON(r)
		if err != nil {
			m.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "JSON 格式错误"})
			return
		}
		nextValue := payload
		if nested := mapValue(payload["config"]); nested != nil {
			nextValue = nested
		}
		nextConfig, err := configFromMap(nextValue)
		if err != nil {
			m.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "配置格式错误"})
			return
		}
		saved, err := m.saveAdminConfig(nextConfig)
		if err != nil {
			m.writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		m.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "config": m.adminConfigView(saved)})
	case "/api/admin/ignore":
		payload, err := decodeJSON(r)
		if err != nil {
			m.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "JSON 格式错误"})
			return
		}
		saved, err := m.setModelIgnored(stringValue(payload["endpoint_id"]), stringValue(payload["model_id"]), boolValue(payload["ignored"], true))
		if err != nil {
			m.writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		m.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "config": saved})
	case "/api/admin/run":
		m.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "started": m.triggerCheck()})
	case "/api/admin/clear-group":
		payload, err := decodeJSON(r)
		if err != nil {
			m.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "JSON 格式错误"})
			return
		}
		if err := m.clearGroupHistory(stringValue(payload["group_id"])); err != nil {
			m.writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		m.writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	case "/api/admin/qq/capture":
		snapshot := m.snapshotConfig()
		if snapshot.QQPush.AppID == "" || snapshot.QQPush.AppSecret == "" {
			m.writeJSON(w, http.StatusBadRequest, map[string]string{"error": "请先保存机器人 AppID 和 AppSecret"})
			return
		}
		started := m.startQQCapture()
		m.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "started": started, "runtime": m.qqRuntimePayload()})
	case "/api/admin/qq/cancel-capture":
		m.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "stopped": m.cancelQQCapture()})
	case "/api/admin/qq/unbind":
		if err := m.unbindQQGroup(); err != nil {
			m.writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		m.writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	case "/api/admin/qq/test":
		if err := m.pushQQStatus(true); err != nil {
			m.writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		m.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "runtime": m.qqRuntimePayload()})
	default:
		m.writeJSON(w, http.StatusNotFound, map[string]string{"error": "Not found"})
	}
}

func (m *Monitor) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if recovered := recover(); recovered != nil {
			log.Printf("[ERROR] HTTP panic: %v", recovered)
			m.writeJSON(w, http.StatusInternalServerError, map[string]string{"error": fmt.Sprint(recovered)})
		}
	}()
	switch r.Method {
	case http.MethodGet:
		m.handleGET(w, r)
	case http.MethodPost:
		m.handlePOST(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		m.writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "Method not allowed"})
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	monitor := newMonitor()
	if err := monitor.loadConfig(); err != nil {
		log.Fatalf("load config: %v", err)
	}
	if err := monitor.initMetricState(); err != nil {
		log.Fatalf("init metric state: %v", err)
	}
	if err := monitor.initDB(); err != nil {
		log.Fatalf("init database: %v", err)
	}
	go monitor.checkerLoop()
	go monitor.qqPushLoop()
	go monitor.qqMentionLoop()
	address := net.JoinHostPort(monitor.listenHost, strconv.Itoa(monitor.listenPort))
	server := &http.Server{Addr: address, Handler: monitor, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 120 * time.Second}
	log.Printf("[INFO] Model Monitor Go v3.0.2 listening on http://%s", address)
	log.Printf("[INFO] Data dir: %s", monitor.dataDir)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP server: %v", err)
	}
}
