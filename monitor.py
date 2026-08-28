#!/usr/bin/env python3
"""
Model Health Monitor - OpenAI-compatible API availability dashboard.
"""

import asyncio
import html
import http.client
import http.server
import base64
import socket
import socketserver
import json
import os
import re
import secrets
import sqlite3
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlparse


# ---------------------------------------------------------------------------
# Environment and defaults

TZ_CN = timezone(timedelta(hours=8))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def getenv_int(name, default):
    value = os.getenv(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def getenv_set(name, default):
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return set(default)
    return {item.strip() for item in raw.split(",") if item.strip()}


def default_data_dir():
    return os.path.join(BASE_DIR, "data")


DATA_DIR = os.getenv("DATA_DIR", default_data_dir())
CONFIG_PATH = os.path.join(DATA_DIR, "config.json")
DB_PATH = os.path.join(DATA_DIR, "history.db")
METRIC_STATE_PATH = os.path.join(DATA_DIR, "metric_state.json")

LISTEN_PORT = getenv_int("LISTEN_PORT", 8020)
LISTEN_HOST = os.getenv("LISTEN_HOST", "0.0.0.0")
DEFAULT_CHECK_INTERVAL = getenv_int("CHECK_INTERVAL", 60)
DEFAULT_TIMEOUT = getenv_int("TIMEOUT", 180)
DEFAULT_TEST_PROMPT = os.getenv("TEST_PROMPT", "Hi")
DEFAULT_MAX_WORKERS = getenv_int("MAX_WORKERS", 16)
DEFAULT_RETENTION_HOURS = getenv_int("HISTORY_RETENTION_HOURS", 72)
DEFAULT_QQ_PUSH_INTERVAL_MINUTES = getenv_int("QQ_PUSH_INTERVAL_MINUTES", 5)
FLUCTUATION_THRESHOLD_SECONDS = 10
MODEL_RETRY_DELAY_SECONDS = 0.5
# HTTP failures are normally returned immediately by the upstream proxy.  A
# small bounded retry window handles short-lived 5xx/429 responses without
# turning an upstream error into a full group-timeout retry storm.
MAX_HTTP_RETRY_ATTEMPTS = 3
MAX_HTTP_RETRY_DELAY_SECONDS = 5.0
# Some OpenAI-compatible gateways expose newer models through the Responses
# API only.  Try that protocol once after an explicit Chat Completions HTTP
# failure before retrying/classifying the probe.
RESPONSES_FALLBACK_STATUSES = frozenset({404, 405})
QQ_MENTION_DEDUP_SECONDS = 10 * 60
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "")
DEFAULT_EXCLUDED_MODELS = getenv_set(
    "EXCLUDED_MODELS", {"mimo-v2.5-tts", "qwen3.5", "minimax-m2.5"}
)

WINDOWS = (
    {"key": "1h", "label": "近1小时", "seconds": 3600},
    {"key": "3h", "label": "近3小时", "seconds": 10800},
    {"key": "24h", "label": "近1天", "seconds": 86400},
)
TTFT_METRIC_VERSION = "first-output-serial-v1"


# ---------------------------------------------------------------------------
# Shared state

config_lock = threading.RLock()
state_lock = threading.RLock()
db_lock = threading.RLock()
run_lock = threading.Lock()
scheduler_wakeup = threading.Event()
qq_push_wakeup = threading.Event()
qq_mention_wakeup = threading.Event()
qq_token_lock = threading.RLock()
qq_capture_lock = threading.Lock()
qq_mention_dedupe_lock = threading.Lock()

config = None
latest_results = {}
known_models = {}
endpoint_errors = {}
endpoint_ping_ms = {}
group_reset_after = {}
last_check_started_at = None
last_check_finished_at = None
check_running = False
history_valid_after = 0
scheduler_force_check = False
scheduler_refresh_requested = False
qq_access_token = None
qq_access_token_expires_at = 0
qq_access_token_identity = None
qq_capture_thread = None
qq_capture_cancel = None
qq_mention_seen = {}
qq_runtime = {
    "capture_status": "idle",
    "capture_message": "尚未开始绑定",
    "capture_code": None,
    "capture_started_at": None,
    "last_push_at": None,
    "last_push_ok": None,
    "last_push_error": None,
    "next_push_at": None,
    "mention_status": "disabled",
    "mention_message": "未启用 @ 查询",
    "last_mention_at": None,
    "last_mention_ok": None,
    "last_mention_error": None,
}


# ---------------------------------------------------------------------------
# Helpers


def now_ts():
    return time.time()


def format_time(ts):
    if not ts:
        return None
    return datetime.fromtimestamp(ts, TZ_CN).strftime("%Y-%m-%d %H:%M:%S")


def make_id(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def normalize_id(value, prefix):
    if value is None:
        return make_id(prefix)
    cleaned = re.sub(r"[^A-Za-z0-9_-]", "_", str(value).strip())
    if not cleaned:
        return make_id(prefix)
    return cleaned[:64]


def clamp_int(value, default, minimum, maximum):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def safe_str(value, default=""):
    if value is None:
        return default
    return str(value).strip()


def normalize_base_url(value):
    raw = safe_str(value)
    if not raw:
        return ""
    if not raw.startswith(("http://", "https://")):
        raw = "http://" + raw
    return raw.rstrip("/")


def openai_path(base_url, suffix):
    parsed = urlparse(base_url)
    prefix = (parsed.path or "").rstrip("/")
    if prefix.endswith("/v1") or prefix == "/v1":
        return prefix + suffix
    return prefix + "/v1" + suffix


def result_key(endpoint_id, model_id):
    return f"{endpoint_id}|{model_id}"


def ignored_key(endpoint_id, model_id):
    return f"{endpoint_id}|{model_id}"


def model_ref(endpoint_id, model_id):
    return f"{endpoint_id}|{model_id}"


def normalize_model_reference(value):
    endpoint_id = None
    model_id = None
    if isinstance(value, dict):
        endpoint_id = safe_str(value.get("endpoint_id"))
        model_id = safe_str(value.get("model_id"))
    elif isinstance(value, str) and "|" in value:
        endpoint_id, model_id = value.split("|", 1)
        endpoint_id = safe_str(endpoint_id)
        model_id = safe_str(model_id)
    if not endpoint_id or not model_id:
        return None
    return {
        "endpoint_id": normalize_id(endpoint_id, "api"),
        "model_id": model_id[:200],
    }


def availability(ok_count, total_count):
    if not total_count:
        return None
    return round((ok_count / total_count) * 100, 2)


def escape_attr(value):
    return html.escape(str(value), quote=True)


# ---------------------------------------------------------------------------
# Configuration


def build_default_config():
    host = os.getenv("SUB2API_HOST", "127.0.0.1")
    port = getenv_int("SUB2API_PORT", 8080)
    base_url = normalize_base_url(os.getenv("API_BASE_URL", f"http://{host}:{port}"))
    group_id = "grp_default"
    endpoint_id = "api_default"
    return {
        "version": 3,
        "check_interval": DEFAULT_CHECK_INTERVAL,
        "max_workers": DEFAULT_MAX_WORKERS,
        "history_retention_hours": DEFAULT_RETENTION_HOURS,
        "groups": [
            {
                "id": group_id,
                "name": os.getenv("DEFAULT_GROUP_NAME", "默认分组"),
                "description": "",
                "enabled": True,
                "check_interval": DEFAULT_CHECK_INTERVAL,
                "timeout": DEFAULT_TIMEOUT,
                "default_model": None,
            }
        ],
        "endpoints": [
            {
                "id": endpoint_id,
                "name": os.getenv("DEFAULT_API_NAME", "sub2api"),
                "base_url": base_url,
                "api_key": os.getenv(
                    "API_KEY", ""
                ),
                "group_id": group_id,
                "enabled": True,
                "test_prompt": DEFAULT_TEST_PROMPT,
                "max_tokens": 16,
            }
        ],
        "ignored_models": [
            {"endpoint_id": endpoint_id, "model_id": model_id}
            for model_id in sorted(DEFAULT_EXCLUDED_MODELS)
        ],
        "qq_push": {
            "enabled": False,
            "mention_enabled": False,
            "app_id": safe_str(os.getenv("QQ_BOT_APP_ID"))[:80],
            "app_secret": safe_str(os.getenv("QQ_BOT_APP_SECRET"))[:500],
            "group_openid": safe_str(os.getenv("QQ_GROUP_OPENID"))[:300],
            "interval_minutes": clamp_int(
                DEFAULT_QQ_PUSH_INTERVAL_MINUTES, 5, 1, 1440
            ),
            "selected_models": [],
        },
    }


def normalize_config(raw_config):
    raw = raw_config if isinstance(raw_config, dict) else {}
    fallback = build_default_config()
    global_check_interval = clamp_int(raw.get("check_interval"), DEFAULT_CHECK_INTERVAL, 10, 86400)

    # Version 3 stored timeouts on API endpoints. Use those values when a
    # group has not yet received its own timeout during migration.
    legacy_group_timeouts = {}
    for endpoint in raw.get("endpoints", []):
        if not isinstance(endpoint, dict) or endpoint.get("timeout") is None:
            continue
        legacy_group_id = normalize_id(endpoint.get("group_id"), "grp")
        legacy_group_timeouts.setdefault(
            legacy_group_id,
            clamp_int(endpoint.get("timeout"), DEFAULT_TIMEOUT, 5, 600),
        )

    groups = []
    seen_groups = set()
    for item in raw.get("groups", []):
        if not isinstance(item, dict):
            continue
        group_id = normalize_id(item.get("id"), "grp")
        if group_id in seen_groups:
            group_id = make_id("grp")
        seen_groups.add(group_id)
        name = safe_str(item.get("name"), "未命名分组") or "未命名分组"
        groups.append(
            {
                "id": group_id,
                "name": name[:80],
                "description": safe_str(item.get("description"))[:200],
                "enabled": bool(item.get("enabled", True)),
                "check_interval": clamp_int(item.get("check_interval"), global_check_interval, 10, 86400),
                "timeout": clamp_int(
                    item.get("timeout"),
                    legacy_group_timeouts.get(group_id, DEFAULT_TIMEOUT),
                    5,
                    600,
                ),
                "default_model": normalize_model_reference(item.get("default_model")),
            }
        )

    if not groups:
        groups = fallback["groups"]

    group_ids = {group["id"] for group in groups}
    default_group_id = groups[0]["id"]

    endpoints = []
    seen_endpoints = set()
    for item in raw.get("endpoints", []):
        if not isinstance(item, dict):
            continue
        endpoint_id = normalize_id(item.get("id"), "api")
        if endpoint_id in seen_endpoints:
            endpoint_id = make_id("api")
        seen_endpoints.add(endpoint_id)
        group_id = normalize_id(item.get("group_id"), "grp")
        if group_id not in group_ids:
            group_id = default_group_id
        base_url = normalize_base_url(item.get("base_url"))
        if not base_url:
            continue
        endpoints.append(
            {
                "id": endpoint_id,
                "name": (safe_str(item.get("name"), "API") or "API")[:80],
                "base_url": base_url[:300],
                "api_key": safe_str(item.get("api_key"))[:500],
                "group_id": group_id,
                "enabled": bool(item.get("enabled", True)),
                "test_prompt": safe_str(item.get("test_prompt"), DEFAULT_TEST_PROMPT)[:500] or DEFAULT_TEST_PROMPT,
                "max_tokens": max(16, clamp_int(item.get("max_tokens"), 16, 1, 256)),
            }
        )

    if not endpoints:
        endpoints = fallback["endpoints"]

    endpoint_ids = {endpoint["id"] for endpoint in endpoints}
    ignored_models = []
    seen_ignored = set()
    for item in raw.get("ignored_models", []):
        endpoint_id = None
        model_id = None
        if isinstance(item, dict):
            endpoint_id = normalize_id(item.get("endpoint_id"), "api")
            model_id = safe_str(item.get("model_id"))
        elif isinstance(item, str) and "|" in item:
            endpoint_id, model_id = item.split("|", 1)
            endpoint_id = normalize_id(endpoint_id, "api")
            model_id = safe_str(model_id)
        if not endpoint_id or endpoint_id not in endpoint_ids or not model_id:
            continue
        key = ignored_key(endpoint_id, model_id)
        if key in seen_ignored:
            continue
        seen_ignored.add(key)
        ignored_models.append({"endpoint_id": endpoint_id, "model_id": model_id[:200]})

    endpoint_by_id = {endpoint["id"]: endpoint for endpoint in endpoints}
    ignored_keys = {ignored_key(item["endpoint_id"], item["model_id"]) for item in ignored_models}
    for group in groups:
        reference = group.get("default_model")
        endpoint = endpoint_by_id.get(reference.get("endpoint_id")) if reference else None
        if (
            not endpoint
            or endpoint.get("group_id") != group["id"]
            or ignored_key(reference["endpoint_id"], reference["model_id"]) in ignored_keys
        ):
            group["default_model"] = None

    qq_raw = raw.get("qq_push") if isinstance(raw.get("qq_push"), dict) else {}
    selected_models = []
    seen_selected = set()
    for item in qq_raw.get("selected_models", []):
        endpoint_id = None
        model_id = None
        if isinstance(item, dict):
            endpoint_id = normalize_id(item.get("endpoint_id"), "api")
            model_id = safe_str(item.get("model_id"))
        elif isinstance(item, str) and "|" in item:
            endpoint_id, model_id = item.split("|", 1)
            endpoint_id = normalize_id(endpoint_id, "api")
            model_id = safe_str(model_id)
        if endpoint_id not in endpoint_ids or not model_id:
            continue
        key = model_ref(endpoint_id, model_id)
        if key in seen_selected:
            continue
        seen_selected.add(key)
        selected_models.append({"endpoint_id": endpoint_id, "model_id": model_id[:200]})

    return {
        "version": 3,
        "check_interval": global_check_interval,
        "max_workers": clamp_int(raw.get("max_workers"), DEFAULT_MAX_WORKERS, 1, 128),
        "history_retention_hours": clamp_int(
            raw.get("history_retention_hours"), DEFAULT_RETENTION_HOURS, 24, 2160
        ),
        "groups": groups,
        "endpoints": endpoints,
        "ignored_models": ignored_models,
        "qq_push": {
            "enabled": bool(qq_raw.get("enabled", False)),
            "mention_enabled": bool(qq_raw.get("mention_enabled", False)),
            "app_id": safe_str(qq_raw.get("app_id"))[:80],
            "app_secret": safe_str(qq_raw.get("app_secret"))[:500],
            "group_openid": safe_str(qq_raw.get("group_openid"))[:300],
            "interval_minutes": clamp_int(
                qq_raw.get("interval_minutes"), DEFAULT_QQ_PUSH_INTERVAL_MINUTES, 1, 1440
            ),
            "selected_models": selected_models,
        },
    }


def ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def write_config_file(next_config):
    ensure_data_dir()
    tmp_path = CONFIG_PATH + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as file_obj:
        json.dump(next_config, file_obj, ensure_ascii=False, indent=2)
        file_obj.write("\n")
    os.replace(tmp_path, CONFIG_PATH)
    try:
        os.chmod(CONFIG_PATH, 0o600)
    except OSError:
        pass


def load_config():
    global config
    ensure_data_dir()
    if not os.path.exists(CONFIG_PATH):
        loaded = build_default_config()
        loaded = normalize_config(loaded)
        write_config_file(loaded)
    else:
        with open(CONFIG_PATH, "r", encoding="utf-8") as file_obj:
            loaded = json.load(file_obj)
        loaded = normalize_config(loaded)
        write_config_file(loaded)
    with config_lock:
        config = loaded
    return loaded


def init_metric_state():
    global history_valid_after
    ensure_data_dir()
    state = {}
    if os.path.exists(METRIC_STATE_PATH):
        try:
            with open(METRIC_STATE_PATH, "r", encoding="utf-8") as file_obj:
                state = json.load(file_obj)
        except (OSError, json.JSONDecodeError):
            state = {}

    if state.get("ttft_metric_version") != TTFT_METRIC_VERSION:
        state = {
            "ttft_metric_version": TTFT_METRIC_VERSION,
            "valid_after": now_ts(),
        }
        tmp_path = METRIC_STATE_PATH + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as file_obj:
            json.dump(state, file_obj, ensure_ascii=False, indent=2)
            file_obj.write("\n")
        os.replace(tmp_path, METRIC_STATE_PATH)

    history_valid_after = float(state.get("valid_after") or 0)


def wake_scheduler(refresh=False, force=False):
    global scheduler_force_check, scheduler_refresh_requested
    with state_lock:
        if refresh:
            scheduler_refresh_requested = True
        if force:
            scheduler_force_check = True
    scheduler_wakeup.set()


def get_config_snapshot():
    with config_lock:
        return json.loads(json.dumps(config, ensure_ascii=False))


def save_config(next_config):
    global config
    normalized = normalize_config(next_config)
    write_config_file(normalized)
    with config_lock:
        config = normalized
    wake_scheduler(refresh=True)
    qq_push_wakeup.set()
    qq_mention_wakeup.set()
    return normalized


def save_admin_config(next_config):
    if not isinstance(next_config, dict):
        raise ValueError("配置必须是 JSON 对象")
    incoming = json.loads(json.dumps(next_config, ensure_ascii=False))
    existing = get_config_snapshot().get("qq_push", {})
    incoming_qq = incoming.get("qq_push")
    if not isinstance(incoming_qq, dict):
        incoming_qq = {}
        incoming["qq_push"] = incoming_qq
    if not safe_str(incoming_qq.get("app_secret")):
        incoming_qq["app_secret"] = existing.get("app_secret", "")
    if not safe_str(incoming_qq.get("group_openid")):
        incoming_qq["group_openid"] = existing.get("group_openid", "")
    return save_config(incoming)


def admin_config_view(config_snapshot):
    public = json.loads(json.dumps(config_snapshot, ensure_ascii=False))
    qq_settings = public.setdefault("qq_push", {})
    secret_set = bool(qq_settings.get("app_secret"))
    group_bound = bool(qq_settings.get("group_openid"))
    qq_settings["app_secret"] = ""
    qq_settings["group_openid"] = ""
    qq_settings["app_secret_set"] = secret_set
    qq_settings["group_bound"] = group_bound
    return public


def qq_selected_set(config_snapshot):
    return {
        model_ref(item.get("endpoint_id"), item.get("model_id"))
        for item in config_snapshot.get("qq_push", {}).get("selected_models", [])
    }


def ignored_set(config_snapshot):
    return {
        ignored_key(item["endpoint_id"], item["model_id"])
        for item in config_snapshot.get("ignored_models", [])
    }


def group_check_interval(config_snapshot, group_id):
    global_interval = config_snapshot.get("check_interval", DEFAULT_CHECK_INTERVAL)
    for group in config_snapshot.get("groups", []):
        if group.get("id") == group_id:
            return clamp_int(group.get("check_interval"), global_interval, 10, 86400)
    return clamp_int(global_interval, DEFAULT_CHECK_INTERVAL, 10, 86400)


def active_check_intervals(config_snapshot):
    intervals = [config_snapshot.get("check_interval", DEFAULT_CHECK_INTERVAL)]
    intervals.extend(
        group.get("check_interval", config_snapshot.get("check_interval", DEFAULT_CHECK_INTERVAL))
        for group in config_snapshot.get("groups", [])
        if group.get("enabled", True)
    )
    return [clamp_int(value, DEFAULT_CHECK_INTERVAL, 10, 86400) for value in intervals]


def model_is_monitorable(config_snapshot, endpoint_id, group_id, model_id):
    groups = {group["id"]: group for group in config_snapshot.get("groups", [])}
    endpoints = {endpoint["id"]: endpoint for endpoint in config_snapshot.get("endpoints", [])}
    endpoint = endpoints.get(endpoint_id)
    if not endpoint or not endpoint.get("enabled", True):
        return False
    if endpoint.get("group_id") != group_id:
        return False
    group = groups.get(group_id)
    if not group or not group.get("enabled", True):
        return False
    return ignored_key(endpoint_id, model_id) not in ignored_set(config_snapshot)


def set_model_ignored(endpoint_id, model_id, should_ignore):
    current = get_config_snapshot()
    endpoint_ids = {endpoint["id"] for endpoint in current.get("endpoints", [])}
    if endpoint_id not in endpoint_ids:
        raise ValueError("API 不存在")
    if not model_id:
        raise ValueError("模型名称不能为空")

    target = ignored_key(endpoint_id, model_id)
    items = []
    seen = set()
    for item in current.get("ignored_models", []):
        key = ignored_key(item.get("endpoint_id"), item.get("model_id"))
        if key == target:
            continue
        if key in seen:
            continue
        seen.add(key)
        items.append(item)
    if should_ignore:
        items.append({"endpoint_id": endpoint_id, "model_id": model_id})
    current["ignored_models"] = items
    if should_ignore:
        qq_settings = current.setdefault("qq_push", {})
        qq_settings["selected_models"] = [
            item
            for item in qq_settings.get("selected_models", [])
            if model_ref(item.get("endpoint_id"), item.get("model_id")) != target
        ]
    saved = save_config(current)
    if should_ignore:
        with state_lock:
            latest_results.pop(target, None)
    return saved


# ---------------------------------------------------------------------------
# QQ group push


def update_qq_runtime(**values):
    with state_lock:
        qq_runtime.update(values)


def qq_runtime_payload():
    with state_lock:
        runtime = dict(qq_runtime)
    for key in ("capture_started_at", "last_push_at", "next_push_at", "last_mention_at"):
        runtime[key] = format_time(runtime.get(key))
    return runtime


def update_qq_group_openid(group_openid):
    global config
    group_openid = safe_str(group_openid)
    if not group_openid:
        raise ValueError("未获取到目标群标识")
    with config_lock:
        current = json.loads(json.dumps(config, ensure_ascii=False))
        current.setdefault("qq_push", {})["group_openid"] = group_openid
        normalized = normalize_config(current)
        write_config_file(normalized)
        config = normalized
    qq_push_wakeup.set()
    qq_mention_wakeup.set()


def unbind_qq_group():
    global config
    cancel_qq_group_capture()
    with config_lock:
        current = json.loads(json.dumps(config, ensure_ascii=False))
        current.setdefault("qq_push", {})["group_openid"] = ""
        normalized = normalize_config(current)
        write_config_file(normalized)
        config = normalized
    update_qq_runtime(
        capture_status="idle",
        capture_message="尚未绑定目标群",
        capture_code=None,
        next_push_at=None,
        mention_status="disabled",
        mention_message="尚未绑定目标群",
    )
    qq_push_wakeup.set()
    qq_mention_wakeup.set()


def qq_https_json(host, method, path, payload=None, headers=None, timeout=15):
    body = None
    request_headers = dict(headers or {})
    if payload is not None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    conn = http.client.HTTPSConnection(host, timeout=timeout)
    try:
        conn.request(method, path, body=body, headers=request_headers)
        response = conn.getresponse()
        raw = response.read(256 * 1024)
        try:
            data = json.loads(raw.decode("utf-8")) if raw else {}
        except (UnicodeDecodeError, json.JSONDecodeError):
            data = {"message": raw.decode("utf-8", errors="replace")[:500]}
        return response.status, data
    finally:
        conn.close()


def qq_error_text(status, data):
    if isinstance(data, dict):
        detail = data.get("message") or data.get("msg") or data.get("error")
        code = data.get("code")
        if detail and code is not None:
            return f"HTTP {status} / {code}: {detail}"
        if detail:
            return f"HTTP {status}: {detail}"
    return f"HTTP {status}: QQ 接口调用失败"


def invalidate_qq_access_token():
    global qq_access_token, qq_access_token_expires_at, qq_access_token_identity
    with qq_token_lock:
        qq_access_token = None
        qq_access_token_expires_at = 0
        qq_access_token_identity = None


def get_qq_access_token(app_id, app_secret, force=False):
    global qq_access_token, qq_access_token_expires_at, qq_access_token_identity
    app_id = safe_str(app_id)
    app_secret = safe_str(app_secret)
    if not app_id or not app_secret:
        raise ValueError("请先填写机器人 AppID 和 AppSecret")
    identity = (app_id, app_secret)
    with qq_token_lock:
        if (
            not force
            and qq_access_token
            and qq_access_token_identity == identity
            and now_ts() < qq_access_token_expires_at - 60
        ):
            return qq_access_token
        status, data = qq_https_json(
            "bots.qq.com",
            "POST",
            "/app/getAppAccessToken",
            {"appId": app_id, "clientSecret": app_secret},
        )
        token = data.get("access_token") if isinstance(data, dict) else None
        if status != 200 or not token:
            raise RuntimeError(qq_error_text(status, data))
        expires_in = clamp_int(data.get("expires_in"), 7200, 60, 86400)
        qq_access_token = safe_str(token)
        qq_access_token_expires_at = now_ts() + expires_in
        qq_access_token_identity = identity
        return qq_access_token


def qq_send_text(config_snapshot, content, reply_to=None):
    settings = config_snapshot.get("qq_push", {})
    app_id = safe_str(settings.get("app_id"))
    app_secret = safe_str(settings.get("app_secret"))
    group_openid = safe_str(settings.get("group_openid"))
    if not group_openid:
        raise ValueError("请先绑定目标群")
    token = get_qq_access_token(app_id, app_secret)
    path = f"/v2/groups/{quote(group_openid, safe='')}/messages"
    payload = {
        "content": safe_str(content)[:4000],
        "msg_type": 0,
        "msg_seq": 1 if reply_to else int(time.time() * 1000) % 65536,
    }
    if reply_to:
        payload["msg_id"] = safe_str(reply_to)[:200]
    status, data = qq_https_json(
        "api.sgroup.qq.com",
        "POST",
        path,
        payload,
        {"Authorization": f"QQBot {token}"},
    )
    if status == 401:
        token = get_qq_access_token(app_id, app_secret, force=True)
        status, data = qq_https_json(
            "api.sgroup.qq.com",
            "POST",
            path,
            payload,
            {"Authorization": f"QQBot {token}"},
        )
    if status < 200 or status >= 300:
        raise RuntimeError(qq_error_text(status, data))
    return data


def build_qq_status_message(config_snapshot, records=None, test=False):
    settings = config_snapshot.get("qq_push", {})
    selected = settings.get("selected_models", [])
    if not selected:
        raise ValueError("请至少选择一个推送模型")
    if records is None:
        with state_lock:
            records = list(latest_results.values())
    records_by_key = {
        model_ref(record.get("endpoint_id"), record.get("model")): record
        for record in records
    }
    endpoints = {item["id"]: item for item in config_snapshot.get("endpoints", [])}

    rows = []
    ok_count = 0
    fluctuation_count = 0
    timeout_count = 0
    error_count = 0
    waiting_count = 0
    for item in selected:
        endpoint_id = item.get("endpoint_id")
        model_id = item.get("model_id")
        endpoint_name = endpoints.get(endpoint_id, {}).get("name", endpoint_id or "未知 API")
        record = records_by_key.get(model_ref(endpoint_id, model_id))
        label = f"{endpoint_name} / {model_id}"
        if not record:
            waiting_count += 1
            rows.append(f"[等待] {label}")
        elif record.get("status") == "ok":
            ok_count += 1
            ttft = record.get("ttft_ms")
            suffix = f" {ttft:.0f}ms" if isinstance(ttft, (int, float)) else ""
            rows.append(f"[正常] {label}{suffix}")
        elif record.get("status") == "fluctuation":
            fluctuation_count += 1
            ttft = record.get("ttft_ms")
            suffix = f" - 延迟 {ttft / 1000:.1f}秒" if isinstance(ttft, (int, float)) else ""
            rows.append(f"[波动] {label}{suffix}")
        elif record.get("status") == "timeout":
            timeout_count += 1
            error = safe_str(record.get("error"), "检测超时").replace("\n", " ")[:100]
            rows.append(f"[超时] {label} - {error}")
        else:
            error_count += 1
            error = safe_str(record.get("error"), "检测失败").replace("\n", " ")[:100]
            rows.append(f"[异常] {label} - {error}")

    title = "[测试推送] 模型渠道状态" if test else "模型渠道状态"
    lines = [
        title,
        format_time(now_ts()),
        (
            f"正常 {ok_count} | 波动 {fluctuation_count} | 超时 {timeout_count} | "
            f"异常 {error_count} | 等待 {waiting_count}"
        ),
        "",
        *rows,
    ]
    message = "\n".join(lines)
    if len(message) > 4000:
        message = message[:3975].rstrip() + "\n...(内容已截断)"
    return message


def push_qq_status(test=False):
    snapshot = get_config_snapshot()
    try:
        message = build_qq_status_message(snapshot, test=test)
        result = qq_send_text(snapshot, message)
        update_qq_runtime(
            last_push_at=now_ts(),
            last_push_ok=True,
            last_push_error=None,
        )
        print(f"[INFO] QQ group status push succeeded ({len(message)} chars)", flush=True)
        return result
    except Exception as exc:
        update_qq_runtime(
            last_push_at=now_ts(),
            last_push_ok=False,
            last_push_error=str(exc)[:300],
        )
        print(f"[ERROR] QQ group status push failed: {exc}", flush=True)
        raise


def qq_push_config_signature(settings):
    selected = tuple(
        model_ref(item.get("endpoint_id"), item.get("model_id"))
        for item in settings.get("selected_models", [])
    )
    return (
        bool(settings.get("enabled")),
        settings.get("app_id"),
        settings.get("app_secret"),
        settings.get("group_openid"),
        settings.get("interval_minutes"),
        selected,
    )


def qq_push_loop():
    signature = None
    next_due = None
    while True:
        snapshot = get_config_snapshot()
        settings = snapshot.get("qq_push", {})
        current_signature = qq_push_config_signature(settings)
        interval_seconds = clamp_int(
            settings.get("interval_minutes"), DEFAULT_QQ_PUSH_INTERVAL_MINUTES, 1, 1440
        ) * 60
        ready = all(
            (
                settings.get("app_id"),
                settings.get("app_secret"),
                settings.get("group_openid"),
                settings.get("selected_models"),
            )
        )
        enabled = bool(settings.get("enabled"))

        if current_signature != signature:
            signature = current_signature
            next_due = now_ts() + interval_seconds if enabled and ready else None

        if not enabled or not ready:
            next_due = None
        elif next_due is None:
            next_due = now_ts() + interval_seconds
        elif now_ts() >= next_due:
            try:
                push_qq_status()
            except Exception:
                pass
            next_due = now_ts() + interval_seconds

        update_qq_runtime(next_push_at=next_due)
        wait_seconds = 5 if next_due is None else min(5, max(0.2, next_due - now_ts()))
        qq_push_wakeup.wait(wait_seconds)
        qq_push_wakeup.clear()


def qq_mention_target(settings, event_type, raw):
    if event_type != "GROUP_AT_MESSAGE_CREATE" or not settings.get("mention_enabled"):
        return None
    if not isinstance(raw, dict):
        return None
    group_openid = safe_str(raw.get("group_openid"))
    message_id = safe_str(raw.get("id"))
    if not group_openid or group_openid != safe_str(settings.get("group_openid")):
        return None
    if not message_id:
        return None
    return group_openid, message_id


def claim_qq_mention(group_openid, message_id, current_time=None):
    current = now_ts() if current_time is None else float(current_time)
    key = (safe_str(group_openid), safe_str(message_id))
    if not all(key):
        return False
    cutoff = current - QQ_MENTION_DEDUP_SECONDS
    with qq_mention_dedupe_lock:
        for seen_key, seen_at in list(qq_mention_seen.items()):
            if seen_at < cutoff:
                qq_mention_seen.pop(seen_key, None)
        if key in qq_mention_seen:
            return False
        qq_mention_seen[key] = current
    return True


async def run_qq_mention_listener(app_id, app_secret, group_openid, stop_event):
    from qqbot_agent_sdk import Intent, QQApiClient, QQWebSocket, WSCallbacks
    import qqbot_agent_sdk.websocket as qq_sdk_websocket

    qq_sdk_websocket.DEFAULT_INTENTS = Intent.GROUP_MESSAGES
    api = QQApiClient(app_id=app_id, client_secret=app_secret, log_tag="ModelMonitorMention")
    result = {"fatal_error": None, "disconnected": False}
    session = {"id": None, "seq": None}
    session_lock = threading.Lock()

    async def on_message(event_type, raw):
        snapshot = get_config_snapshot()
        target = qq_mention_target(snapshot.get("qq_push", {}), event_type, raw)
        if not target:
            return
        target_group_openid, message_id = target
        if target_group_openid != group_openid:
            return
        if not claim_qq_mention(target_group_openid, message_id):
            print("[INFO] Duplicate QQ mention ignored", flush=True)
            return
        try:
            message = build_qq_status_message(snapshot)
            await asyncio.to_thread(
                qq_send_text,
                snapshot,
                message,
                message_id,
            )
            update_qq_runtime(
                last_mention_at=now_ts(),
                last_mention_ok=True,
                last_mention_error=None,
            )
            print(f"[INFO] QQ mention status reply succeeded ({len(message)} chars)", flush=True)
        except Exception as exc:
            update_qq_runtime(
                last_mention_at=now_ts(),
                last_mention_ok=False,
                last_mention_error=str(exc)[:300],
            )
            print(f"[ERROR] QQ mention status reply failed: {exc}", flush=True)

    def get_session():
        with session_lock:
            return session["id"], session["seq"]

    def set_session(session_id, seq):
        with session_lock:
            session["id"] = session_id
            session["seq"] = seq

    def on_connected():
        result["disconnected"] = False
        update_qq_runtime(
            mention_status="listening",
            mention_message="监听中，群成员 @机器人 时回复状态",
        )

    def on_disconnected():
        result["disconnected"] = True

    def on_fatal_error(_code, message):
        result["fatal_error"] = safe_str(message, "WebSocket 连接失败")

    callbacks = WSCallbacks(
        on_message_event=on_message,
        on_connected=on_connected,
        on_disconnected=on_disconnected,
        on_fatal_error=on_fatal_error,
        get_token=api.ensure_token_sync,
        get_session=get_session,
        set_session=set_session,
        set_heartbeat_interval=lambda _interval: None,
        clear_token=api.clear_token,
        fail_pending=lambda _reason: None,
        get_gateway_url=api.get_gateway_url_sync,
    )
    ws = QQWebSocket(callbacks=callbacks, log_tag="ModelMonitorMention")
    update_qq_runtime(
        mention_status="connecting",
        mention_message="正在连接 QQ 机器人网关",
    )
    gateway_url = await asyncio.to_thread(api.get_gateway_url_sync)
    ws.start(gateway_url, asyncio.get_running_loop())
    try:
        while not stop_event.is_set():
            if result["fatal_error"]:
                raise RuntimeError(result["fatal_error"])
            if result["disconnected"]:
                raise ConnectionError("QQ 机器人网关连接已断开")
            await asyncio.sleep(0.25)
    finally:
        await ws.async_stop()


def qq_mention_loop():
    while True:
        qq_mention_wakeup.clear()
        snapshot = get_config_snapshot()
        settings = snapshot.get("qq_push", {})
        with state_lock:
            capture_active = qq_runtime.get("capture_status") in (
                "connecting",
                "waiting_message",
            )
        ready = all(
            (
                settings.get("app_id"),
                settings.get("app_secret"),
                settings.get("group_openid"),
                settings.get("selected_models"),
            )
        )

        if not settings.get("mention_enabled"):
            update_qq_runtime(
                mention_status="disabled",
                mention_message="未启用 @ 查询",
            )
        elif capture_active:
            update_qq_runtime(
                mention_status="paused",
                mention_message="绑定目标群期间暂停监听",
            )
        elif not ready:
            update_qq_runtime(
                mention_status="incomplete",
                mention_message="@ 查询配置未完成",
            )
        else:
            try:
                asyncio.run(
                    run_qq_mention_listener(
                        safe_str(settings.get("app_id")),
                        safe_str(settings.get("app_secret")),
                        safe_str(settings.get("group_openid")),
                        qq_mention_wakeup,
                    )
                )
            except Exception as exc:
                update_qq_runtime(
                    mention_status="error",
                    mention_message=str(exc)[:300],
                )
                print(f"[ERROR] QQ mention listener failed: {exc}", flush=True)

        if qq_mention_wakeup.is_set():
            continue
        qq_mention_wakeup.wait(5)


async def run_qq_capture(app_id, app_secret, capture_code, cancel_event):
    from qqbot_agent_sdk import Intent, QQApiClient, QQWebSocket, WSCallbacks
    import qqbot_agent_sdk.websocket as qq_sdk_websocket

    qq_sdk_websocket.DEFAULT_INTENTS = Intent.GROUP_MESSAGES
    api = QQApiClient(app_id=app_id, client_secret=app_secret, log_tag="ModelMonitor")
    result = {"group_openid": None, "fatal_error": None}
    session = {"id": None, "seq": None}
    session_lock = threading.Lock()
    done = asyncio.Event()

    async def on_message(event_type, raw):
        if event_type != "GROUP_AT_MESSAGE_CREATE":
            return
        content = safe_str(raw.get("content"))
        group_openid = safe_str(raw.get("group_openid"))
        if capture_code not in content or not group_openid:
            return
        result["group_openid"] = group_openid
        done.set()

    def get_session():
        with session_lock:
            return session["id"], session["seq"]

    def set_session(session_id, seq):
        with session_lock:
            session["id"] = session_id
            session["seq"] = seq

    def on_connected():
        update_qq_runtime(
            capture_status="waiting_message",
            capture_message=f"已连接，请在目标群 @机器人 发送：绑定 {capture_code}",
        )

    def on_fatal_error(_code, message):
        result["fatal_error"] = safe_str(message, "WebSocket 连接失败")

    callbacks = WSCallbacks(
        on_message_event=on_message,
        on_connected=on_connected,
        on_disconnected=lambda: None,
        on_fatal_error=on_fatal_error,
        get_token=api.ensure_token_sync,
        get_session=get_session,
        set_session=set_session,
        set_heartbeat_interval=lambda _interval: None,
        clear_token=api.clear_token,
        fail_pending=lambda _reason: None,
        get_gateway_url=api.get_gateway_url_sync,
    )
    ws = QQWebSocket(callbacks=callbacks, log_tag="ModelMonitorBind")
    gateway_url = await asyncio.to_thread(api.get_gateway_url_sync)
    ws.start(gateway_url, asyncio.get_running_loop())
    deadline = time.monotonic() + 180
    try:
        while not done.is_set() and not cancel_event.is_set():
            if result["fatal_error"]:
                raise RuntimeError(result["fatal_error"])
            if time.monotonic() >= deadline:
                raise TimeoutError("绑定超时，请重新开始绑定")
            await asyncio.sleep(0.25)
    finally:
        await ws.async_stop()
    return result["group_openid"]


def qq_capture_worker(app_id, app_secret, capture_code, cancel_event):
    try:
        group_openid = asyncio.run(
            run_qq_capture(app_id, app_secret, capture_code, cancel_event)
        )
        if cancel_event.is_set():
            update_qq_runtime(
                capture_status="cancelled",
                capture_message="已停止绑定",
                capture_code=None,
            )
            return
        update_qq_group_openid(group_openid)
        update_qq_runtime(
            capture_status="bound",
            capture_message="目标群绑定成功",
            capture_code=None,
        )
    except Exception as exc:
        update_qq_runtime(
            capture_status="error",
            capture_message=str(exc)[:300],
            capture_code=None,
        )
    finally:
        qq_mention_wakeup.set()


def start_qq_group_capture():
    global qq_capture_thread, qq_capture_cancel
    settings = get_config_snapshot().get("qq_push", {})
    app_id = safe_str(settings.get("app_id"))
    app_secret = safe_str(settings.get("app_secret"))
    if not app_id or not app_secret:
        raise ValueError("请先保存机器人 AppID 和 AppSecret")
    with qq_capture_lock:
        if qq_capture_thread and qq_capture_thread.is_alive():
            return False
        capture_code = str(secrets.randbelow(900000) + 100000)
        qq_capture_cancel = threading.Event()
        update_qq_runtime(
            capture_status="connecting",
            capture_message="正在连接 QQ 机器人网关",
            capture_code=capture_code,
            capture_started_at=now_ts(),
        )
        qq_mention_wakeup.set()
        qq_capture_thread = threading.Thread(
            target=qq_capture_worker,
            args=(app_id, app_secret, capture_code, qq_capture_cancel),
            name="qq-group-bind",
            daemon=True,
        )
        qq_capture_thread.start()
    return True


def cancel_qq_group_capture():
    with qq_capture_lock:
        if qq_capture_cancel:
            qq_capture_cancel.set()
            return True
    return False


# ---------------------------------------------------------------------------
# SQLite history


def init_db():
    ensure_data_dir()
    with db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS checks (
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
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_checks_time ON checks(checked_at)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_checks_model_time ON checks(endpoint_id, model_id, checked_at)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_checks_group_time ON checks(group_id, checked_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_checks_endpoint_time ON checks(endpoint_id, checked_at)")


def insert_history(records):
    if not records:
        return
    rows = [
        (
            record["endpoint_id"],
            record["endpoint_name"],
            record["group_id"],
            record["group_name"],
            record["model"],
            record["status"],
            record.get("ttft_ms"),
            record.get("error"),
            record["checked_at_ts"],
        )
        for record in records
    ]
    with db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.executemany(
            """
            INSERT INTO checks (
                endpoint_id, endpoint_name, group_id, group_name, model_id,
                status, ttft_ms, error, checked_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def prune_history(retention_hours):
    cutoff = now_ts() - retention_hours * 3600
    with db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.execute("DELETE FROM checks WHERE checked_at < ?", (cutoff,))


def clear_group_history(group_id):
    snapshot = get_config_snapshot()
    group_ids = {group["id"] for group in snapshot.get("groups", [])}
    if group_id not in group_ids:
        raise ValueError("分组不存在")
    reset_at = now_ts()
    with db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.execute("DELETE FROM checks WHERE group_id = ?", (group_id,))
    with state_lock:
        group_reset_after[group_id] = reset_at
        for key, record in list(latest_results.items()):
            if record.get("group_id") == group_id:
                latest_results.pop(key, None)
    return True


def history_filter_clause(endpoint_ids, ignored_keys, valid_after=None):
    parts = []
    params = []
    if valid_after:
        parts.append("checked_at >= ?")
        params.append(float(valid_after))
    if endpoint_ids is not None:
        if not endpoint_ids:
            return " AND 1 = 0", []
        placeholders = ",".join("?" for _ in endpoint_ids)
        parts.append(f"endpoint_id IN ({placeholders})")
        params.extend(sorted(endpoint_ids))
    if ignored_keys:
        placeholders = ",".join("?" for _ in ignored_keys)
        parts.append(f"(endpoint_id || '|' || model_id) NOT IN ({placeholders})")
        params.extend(sorted(ignored_keys))
    if not parts:
        return "", []
    return " AND " + " AND ".join(parts), params


def query_global_windows(endpoint_ids=None, ignored_keys=None, valid_after=None):
    output = {}
    current = now_ts()
    extra_where, extra_params = history_filter_clause(endpoint_ids, ignored_keys or set(), valid_after)
    with db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        for window in WINDOWS:
            row = conn.execute(
                f"""
                SELECT COUNT(*) AS total_count,
                       SUM(CASE WHEN status IN ('ok', 'fluctuation') THEN 1 ELSE 0 END) AS ok_count,
                       AVG(CASE WHEN status IN ('ok', 'fluctuation') THEN ttft_ms ELSE NULL END) AS avg_ttft
                FROM checks
                WHERE checked_at >= ?{extra_where}
                """,
                [current - window["seconds"], *extra_params],
            ).fetchone()
            total_count = int(row["total_count"] or 0)
            ok_count = int(row["ok_count"] or 0)
            output[window["key"]] = {
                "ok": ok_count,
                "total": total_count,
                "availability": availability(ok_count, total_count),
                "avg_ttft_ms": round(row["avg_ttft"], 1) if row["avg_ttft"] is not None else None,
            }
    return output


def query_grouped_windows(group_fields, endpoint_ids=None, ignored_keys=None, valid_after=None):
    output = {window["key"]: {} for window in WINDOWS}
    select_fields = ", ".join(group_fields)
    group_by = ", ".join(group_fields)
    current = now_ts()
    extra_where, extra_params = history_filter_clause(endpoint_ids, ignored_keys or set(), valid_after)
    with db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        for window in WINDOWS:
            rows = conn.execute(
                f"""
                SELECT {select_fields}, COUNT(*) AS total_count,
                       SUM(CASE WHEN status IN ('ok', 'fluctuation') THEN 1 ELSE 0 END) AS ok_count,
                       AVG(CASE WHEN status IN ('ok', 'fluctuation') THEN ttft_ms ELSE NULL END) AS avg_ttft
                FROM checks
                WHERE checked_at >= ?{extra_where}
                GROUP BY {group_by}
                """,
                [current - window["seconds"], *extra_params],
            ).fetchall()
            for row in rows:
                key = "|".join(str(row[field]) for field in group_fields)
                total_count = int(row["total_count"] or 0)
                ok_count = int(row["ok_count"] or 0)
                output[window["key"]][key] = {
                    "ok": ok_count,
                    "total": total_count,
                    "availability": availability(ok_count, total_count),
                    "avg_ttft_ms": round(row["avg_ttft"], 1) if row["avg_ttft"] is not None else None,
                }
    return output


def query_recent_model_results(endpoint_ids=None, ignored_keys=None, limit=5, valid_after=None):
    output = {}
    extra_where, extra_params = history_filter_clause(endpoint_ids, ignored_keys or set(), valid_after)
    with db_lock, sqlite3.connect(DB_PATH, timeout=30) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            f"""
            SELECT endpoint_id, model_id, status, ttft_ms, error, checked_at
            FROM checks
            WHERE 1 = 1{extra_where}
            ORDER BY checked_at DESC
            """,
            extra_params,
        ).fetchall()
        for row in rows:
            key = result_key(row["endpoint_id"], row["model_id"])
            items = output.setdefault(key, [])
            if len(items) >= limit:
                continue
            items.append(
                {
                    "status": row["status"],
                    "ttft_ms": round(row["ttft_ms"], 1) if row["ttft_ms"] is not None else None,
                    "error": row["error"],
                    "checked_at": format_time(row["checked_at"]),
                }
            )
    return output


# ---------------------------------------------------------------------------
# API checks


def make_connection(endpoint, timeout=None):
    parsed = urlparse(endpoint["base_url"])
    if parsed.scheme not in ("http", "https"):
        raise ValueError("仅支持 http/https API 地址")
    if not parsed.hostname:
        raise ValueError("API 地址缺少主机名")
    conn_class = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    connection_timeout = DEFAULT_TIMEOUT if timeout is None else timeout
    return conn_class(parsed.hostname, parsed.port, timeout=max(0.001, float(connection_timeout)))


def auth_headers(endpoint, extra=None):
    headers = dict(extra or {})
    api_key = endpoint.get("api_key") or ""
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    return headers


def fetch_models(endpoint):
    started = time.monotonic()
    conn = None
    try:
        conn = make_connection(endpoint)
        conn.request("GET", openai_path(endpoint["base_url"], "/models"), headers=auth_headers(endpoint))
        resp = conn.getresponse()
        body = resp.read()
        if resp.status != 200:
            detail = body[:200].decode("utf-8", errors="replace")
            return None, f"HTTP {resp.status}: {detail}"
        payload = json.loads(body.decode("utf-8"))
        models = []
        for item in payload.get("data", []):
            if isinstance(item, dict) and item.get("id"):
                models.append(str(item["id"]))
        return sorted(set(models)), None
    except Exception as exc:
        return None, str(exc)[:240]
    finally:
        with state_lock:
            endpoint_ping_ms[endpoint["id"]] = round((time.monotonic() - started) * 1000, 1)
        if conn:
            conn.close()


def first_reasoning_delta(choice):
    if not isinstance(choice, dict):
        return None

    delta = choice.get("delta")
    if not isinstance(delta, dict):
        return None

    for field in ("reasoning_content", "reasoning", "thinking", "thoughts"):
        value = delta.get(field)
        if isinstance(value, str) and value:
            return value
        if isinstance(value, (dict, list)) and value:
            return json.dumps(value, ensure_ascii=False)

    return None


def first_content_delta(choice):
    if not isinstance(choice, dict):
        return None

    text = choice.get("text")
    if text:
        return text

    delta = choice.get("delta")
    if not isinstance(delta, dict):
        return None

    for field in ("content", "output_text", "refusal"):
        value = delta.get(field)
        if isinstance(value, str) and value:
            return value
        if isinstance(value, (dict, list)) and value:
            return json.dumps(value, ensure_ascii=False)

    return None


def assistant_message_present(choice):
    if not isinstance(choice, dict):
        return False

    message = choice.get("message")
    if not isinstance(message, dict):
        return False
    if message.get("role") == "assistant":
        return True

    for field in ("content", "output_text", "refusal", "tool_calls"):
        value = message.get(field)
        if isinstance(value, str) and value:
            return True
        if isinstance(value, (dict, list)) and value:
            return True

    return False


def extract_error_message(payload):
    if not isinstance(payload, dict):
        return None

    error = payload.get("error")
    if isinstance(error, dict):
        message = error.get("message")
        err_type = error.get("type")
        if message and err_type:
            return f"{err_type}: {message}"
        if message:
            return str(message)
        if err_type:
            return str(err_type)
    elif isinstance(error, str) and error:
        return error

    message = payload.get("message")
    if isinstance(message, str) and message:
        return message
    return None


def check_elapsed_ms(started):
    return round((time.monotonic() - started) * 1000, 1)


def remaining_check_time(deadline):
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("检测已达到分组超时时间")
    return remaining


def apply_connection_deadline(conn, deadline):
    remaining = remaining_check_time(deadline)
    if conn.sock:
        conn.sock.settimeout(remaining)
    return remaining


def is_retryable_http_status(status):
    return status in (408, 425, 429) or status >= 500


def http_status_from_error(error):
    """Return the HTTP status encoded in a probe error, if it has one."""
    if not isinstance(error, str):
        return None
    match = re.match(r"^HTTP\s+(\d{3})(?:\b|:)", error.strip())
    return int(match.group(1)) if match else None


def retry_delay_seconds(attempts, http_failure=False):
    """Use bounded backoff for HTTP failures and the legacy delay otherwise."""
    if not http_failure:
        return MODEL_RETRY_DELAY_SECONDS
    exponent = max(0, min(int(attempts) - 1, 3))
    return min(
        MAX_HTTP_RETRY_DELAY_SECONDS,
        MODEL_RETRY_DELAY_SECONDS * (2**exponent),
    )


def check_model_nonstream(endpoint, request_body, started, deadline):
    conn = None
    try:
        conn = make_connection(endpoint, remaining_check_time(deadline))
        body = dict(request_body)
        body["stream"] = False
        conn.request(
            "POST",
            openai_path(endpoint["base_url"], "/chat/completions"),
            body=json.dumps(body).encode("utf-8"),
            headers=auth_headers(
                endpoint,
                {
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
            ),
        )
        apply_connection_deadline(conn, deadline)
        resp = conn.getresponse()
        apply_connection_deadline(conn, deadline)
        raw = resp.read()
        elapsed_ms = check_elapsed_ms(started)
        detail = raw[:240].decode("utf-8", errors="replace")
        if resp.status != 200:
            return False, is_retryable_http_status(resp.status), elapsed_ms, f"HTTP {resp.status}: {detail}"
        payload = json.loads(raw.decode("utf-8"))
        choices = payload.get("choices") or []
        if any(assistant_message_present(choice) for choice in choices):
            return True, False, elapsed_ms, None
        return False, True, elapsed_ms, "non-stream response missing assistant message"
    except json.JSONDecodeError as exc:
        return False, True, check_elapsed_ms(started), f"响应 JSON 无效：{exc}"[:240]
    except (TimeoutError, socket.timeout, OSError, http.client.HTTPException) as exc:
        return False, True, check_elapsed_ms(started), str(exc)[:240]
    except ValueError as exc:
        return False, False, check_elapsed_ms(started), str(exc)[:240]
    except Exception as exc:
        return False, True, check_elapsed_ms(started), str(exc)[:240]
    finally:
        if conn:
            conn.close()


def responses_input_from_request(request_body):
    """Convert the monitor's chat probe into a compact Responses input."""
    messages = request_body.get("messages")
    if not isinstance(messages, list):
        return safe_str(request_body.get("input"), DEFAULT_TEST_PROMPT)

    parts = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        if isinstance(content, str) and content.strip():
            parts.append(content.strip())
            continue
        if not isinstance(content, list):
            continue
        for item in content:
            if not isinstance(item, dict):
                continue
            text = item.get("text") or item.get("content")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    return "\n".join(parts) or DEFAULT_TEST_PROMPT


def responses_output_present(payload):
    """Return whether a successful Responses payload contains model output."""
    if not isinstance(payload, dict) or payload.get("error"):
        return False

    output_text = payload.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return True

    output = payload.get("output")
    if isinstance(output, list):
        # A completed response is still a valid health result when the model
        # emits a non-text item (for example reasoning or a tool call).
        if payload.get("status") == "completed":
            return True
        for item in output:
            if not isinstance(item, dict):
                continue
            if item.get("type") in {"message", "output_text", "reasoning", "tool_call"}:
                return True
            content = item.get("content")
            if isinstance(content, list) and any(
                isinstance(part, dict)
                and (part.get("text") or part.get("type") in {"output_text", "refusal"})
                for part in content
            ):
                return True

    # A few gateways wrap Responses output in a Chat Completions-like shape.
    choices = payload.get("choices")
    return isinstance(choices, list) and any(assistant_message_present(choice) for choice in choices)


def responses_stream_event_present(event_name, payload):
    """Identify the first Responses SSE event that proves model execution."""
    event_type = str(event_name or "").lower()
    if event_type in {
        "response.output_item.added",
        "response.output_text.delta",
        "response.output_text.done",
        "response.reasoning_summary_text.delta",
        "response.refusal.delta",
    }:
        return True
    if event_type == "response.completed":
        response = payload.get("response") if isinstance(payload, dict) else None
        return isinstance(response, dict) and response.get("status") == "completed"
    if isinstance(payload, dict):
        if payload.get("error"):
            return False
        delta = payload.get("delta")
        if isinstance(delta, str) and delta:
            return True
        if isinstance(delta, (dict, list)) and delta:
            return True
    return False


def check_model_responses(endpoint, request_body, started, deadline):
    """Probe the Responses API as a bounded fallback for newer model routes."""
    conn = None
    try:
        conn = make_connection(endpoint, remaining_check_time(deadline))
        body = {
            "model": request_body.get("model"),
            "input": responses_input_from_request(request_body),
            "stream": True,
        }
        conn.request(
            "POST",
            openai_path(endpoint["base_url"], "/responses"),
            body=json.dumps(body).encode("utf-8"),
            headers=auth_headers(
                endpoint,
                {
                    "Content-Type": "application/json",
                    "Accept": "text/event-stream, application/json",
                },
            ),
        )
        apply_connection_deadline(conn, deadline)
        resp = conn.getresponse()
        apply_connection_deadline(conn, deadline)
        if resp.status != 200:
            raw = resp.read(240)
            elapsed_ms = check_elapsed_ms(started)
            detail = raw.decode("utf-8", errors="replace")
            return (
                False,
                is_retryable_http_status(resp.status),
                elapsed_ms,
                f"Responses HTTP {resp.status}: {detail}"[:240],
            )

        content_type = (resp.getheader("content-type") or "").lower()
        if "event-stream" not in content_type:
            raw = resp.read()
            elapsed_ms = check_elapsed_ms(started)
            try:
                payload = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                return False, False, elapsed_ms, f"Responses 响应 JSON 无效：{exc}"[:240]
            if responses_output_present(payload):
                return True, False, elapsed_ms, None
            error = extract_error_message(payload) or "Responses API 响应缺少模型输出"
            return False, False, elapsed_ms, error[:240]

        event_name = None
        last_payload = None
        while True:
            apply_connection_deadline(conn, deadline)
            line = resp.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").strip()
            if text.startswith("event:"):
                event_name = text[6:].strip()
                continue
            if not text.startswith("data:"):
                continue
            raw_payload = text[5:].strip()
            if raw_payload == "[DONE]":
                break
            try:
                payload = json.loads(raw_payload)
            except json.JSONDecodeError:
                event_name = None
                continue
            last_payload = payload
            error_message = extract_error_message(payload)
            if error_message:
                return False, True, check_elapsed_ms(started), error_message[:240]
            payload_type = payload.get("type") if isinstance(payload, dict) else None
            if responses_stream_event_present(event_name or payload_type, payload):
                return True, False, check_elapsed_ms(started), None
            event_name = None

        if responses_output_present(last_payload):
            return True, False, check_elapsed_ms(started), None
        return False, False, check_elapsed_ms(started), "Responses 流在首个输出前结束"
    except json.JSONDecodeError as exc:
        return False, False, check_elapsed_ms(started), f"Responses 响应 JSON 无效：{exc}"[:240]
    except (TimeoutError, socket.timeout, OSError, http.client.HTTPException) as exc:
        return False, True, check_elapsed_ms(started), str(exc)[:240]
    except ValueError as exc:
        return False, False, check_elapsed_ms(started), str(exc)[:240]
    except Exception as exc:
        return False, True, check_elapsed_ms(started), str(exc)[:240]
    finally:
        if conn:
            conn.close()


def check_model_attempt(endpoint, request_body, started, deadline):
    body = json.dumps(request_body).encode("utf-8")
    headers = auth_headers(
        endpoint,
        {
            "Content-Type": "application/json",
            "Accept": "text/event-stream, application/json",
        },
    )

    conn = None
    try:
        conn = make_connection(endpoint, remaining_check_time(deadline))
        conn.request("POST", openai_path(endpoint["base_url"], "/chat/completions"), body=body, headers=headers)
        apply_connection_deadline(conn, deadline)
        resp = conn.getresponse()
        apply_connection_deadline(conn, deadline)
        if resp.status != 200:
            detail = resp.read(240).decode("utf-8", errors="replace")
            return (
                False,
                is_retryable_http_status(resp.status),
                check_elapsed_ms(started),
                f"HTTP {resp.status}: {detail}"[:240],
            )

        stream_error = None
        stream_event = None
        while True:
            apply_connection_deadline(conn, deadline)
            line = resp.readline()
            if not line:
                break
            text = line.decode("utf-8", errors="replace").strip()
            if text.startswith("event:"):
                stream_event = text[6:].strip().lower()
                continue
            if not text.startswith("data:"):
                continue
            payload = text[5:].strip()
            if payload == "[DONE]":
                break
            try:
                chunk = json.loads(payload)
            except json.JSONDecodeError:
                continue
            error_message = extract_error_message(chunk)
            if stream_event == "error" or error_message:
                stream_error = error_message or f"SSE event: {stream_event}"
                break
            choices = chunk.get("choices") or []
            if any(first_reasoning_delta(choice) is not None for choice in choices):
                return True, False, check_elapsed_ms(started), None
            if any(first_content_delta(choice) is not None for choice in choices):
                return True, False, check_elapsed_ms(started), None
            stream_event = None

        fallback_ok, fallback_retryable, fallback_elapsed_ms, fallback_error = check_model_nonstream(
            endpoint, request_body, started, deadline
        )
        if fallback_ok:
            return True, False, fallback_elapsed_ms, None
        error = fallback_error or stream_error or "stream ended before first output"
        return False, fallback_retryable or bool(stream_error), fallback_elapsed_ms, error[:240]
    except json.JSONDecodeError as exc:
        return False, True, check_elapsed_ms(started), f"响应 JSON 无效：{exc}"[:240]
    except (TimeoutError, socket.timeout, OSError, http.client.HTTPException) as exc:
        return False, True, check_elapsed_ms(started), str(exc)[:240]
    except ValueError as exc:
        return False, False, check_elapsed_ms(started), str(exc)[:240]
    except Exception as exc:
        return False, True, check_elapsed_ms(started), str(exc)[:240]
    finally:
        if conn:
            conn.close()


def finish_check_timeout(record, timeout_seconds, attempts, last_error, elapsed_ms):
    retries = max(0, attempts - 1)
    record["status"] = "timeout"
    record["ttft_ms"] = elapsed_ms
    message = f"检测超时：{timeout_seconds}秒内重试 {retries} 次仍不可用"
    if last_error:
        message += f"；最后错误：{last_error}"
    record["error"] = message[:240]
    return record


def finish_check_error(record, attempts, last_error, elapsed_ms):
    """Record a bounded probe failure without mislabeling it as a timeout."""
    record["status"] = "error"
    record["ttft_ms"] = elapsed_ms
    message = last_error or "检测失败"
    if attempts > 1:
        message = f"检测失败：连续尝试 {attempts} 次仍不可用；最后错误：{message}"
    record["error"] = message[:240]
    return record


def check_model(endpoint, group, model_id):
    checked_at_ts = now_ts()
    record = {
        "endpoint_id": endpoint["id"],
        "endpoint_name": endpoint["name"],
        "endpoint_base_url": endpoint["base_url"],
        "group_id": group["id"],
        "group_name": group["name"],
        "model": model_id,
        "status": "unknown",
        "ttft_ms": None,
        "error": None,
        "checked_at": format_time(checked_at_ts),
        "checked_at_ts": checked_at_ts,
    }
    request_body = {
        "model": model_id,
        "messages": [{"role": "user", "content": endpoint.get("test_prompt") or DEFAULT_TEST_PROMPT}],
        "stream": True,
    }
    # TTFT only needs the first streamed token. Omitting token-limit fields avoids
    # provider-specific 400s such as unsupported max_tokens/max_output_tokens.
    started = time.monotonic()
    timeout_seconds = clamp_int(group.get("timeout"), DEFAULT_TIMEOUT, 5, 600)
    deadline = started + timeout_seconds
    attempts = 0
    last_error = None
    consecutive_http_failures = 0
    responses_fallback_attempted = False

    while True:
        current = time.monotonic()
        if current >= deadline:
            if http_status_from_error(last_error) is not None:
                return finish_check_error(record, attempts, last_error, round((current - started) * 1000, 1))
            return finish_check_timeout(
                record,
                timeout_seconds,
                attempts,
                last_error,
                round((current - started) * 1000, 1),
            )

        attempts += 1
        success, retryable, elapsed_ms, error = check_model_attempt(
            endpoint, request_body, started, deadline
        )
        if success:
            record["ttft_ms"] = elapsed_ms
            if elapsed_ms > FLUCTUATION_THRESHOLD_SECONDS * 1000:
                record["status"] = "fluctuation"
                record["error"] = (
                    f"检测波动：{elapsed_ms / 1000:.1f}秒后恢复，共尝试 {attempts} 次"
                )
            else:
                record["status"] = "ok"
            return record

        last_error = error or "检测失败"
        http_status = http_status_from_error(last_error)
        if (
            not responses_fallback_attempted
            and http_status is not None
            and (http_status >= 500 or http_status in RESPONSES_FALLBACK_STATUSES)
        ):
            responses_fallback_attempted = True
            # Report the usable protocol's own TTFT.  The failed Chat
            # Completions attempt is diagnostic context, not model latency.
            responses_started = time.monotonic()
            (
                responses_ok,
                _responses_retryable,
                responses_elapsed_ms,
                _responses_error,
            ) = check_model_responses(endpoint, request_body, responses_started, deadline)
            if responses_ok:
                record["ttft_ms"] = responses_elapsed_ms
                record["probe_protocol"] = "responses"
                if responses_elapsed_ms > FLUCTUATION_THRESHOLD_SECONDS * 1000:
                    record["status"] = "fluctuation"
                    record["error"] = (
                        f"检测波动：{responses_elapsed_ms / 1000:.1f}秒后恢复，共尝试 {attempts} 次"
                    )
                else:
                    record["status"] = "ok"
                return record

        if not retryable:
            return finish_check_error(record, attempts, last_error, elapsed_ms)

        if http_status is not None:
            consecutive_http_failures += 1
            if consecutive_http_failures >= MAX_HTTP_RETRY_ATTEMPTS:
                return finish_check_error(record, attempts, last_error, elapsed_ms)
        else:
            consecutive_http_failures = 0

        current = time.monotonic()
        if current >= deadline:
            if http_status_from_error(last_error) is not None:
                return finish_check_error(record, attempts, last_error, round((current - started) * 1000, 1))
            return finish_check_timeout(
                record,
                timeout_seconds,
                attempts,
                last_error,
                round((current - started) * 1000, 1),
            )
        delay = retry_delay_seconds(
            consecutive_http_failures,
            http_failure=http_status is not None,
        )
        time.sleep(min(delay, deadline - current))


def discover_check_tasks(snapshot):
    global endpoint_errors

    groups = {group["id"]: group for group in snapshot["groups"] if group.get("enabled", True)}
    ignored = ignored_set(snapshot)
    tasks = {}
    errors = {}
    fetched_models = {}

    for endpoint in snapshot["endpoints"]:
        if not endpoint.get("enabled", True):
            continue
        group = groups.get(endpoint.get("group_id"))
        if not group:
            continue
        models, error = fetch_models(endpoint)
        if error:
            errors[endpoint["id"]] = error
            with state_lock:
                models = list(known_models.get(endpoint["id"], []))
        else:
            fetched_models[endpoint["id"]] = models
        for model_id in models or []:
            if ignored_key(endpoint["id"], model_id) in ignored:
                continue
            tasks[result_key(endpoint["id"], model_id)] = (endpoint, group, model_id)

    with state_lock:
        for endpoint_id, models in fetched_models.items():
            known_models[endpoint_id] = models
        endpoint_errors = errors

    return tasks


def fallback_record(endpoint, group, model_id, exc):
    checked_at_ts = now_ts()
    return {
        "endpoint_id": endpoint["id"],
        "endpoint_name": endpoint["name"],
        "endpoint_base_url": endpoint["base_url"],
        "group_id": group["id"],
        "group_name": group["name"],
        "model": model_id,
        "status": "error",
        "ttft_ms": None,
        "error": str(exc)[:240],
        "checked_at": format_time(checked_at_ts),
        "checked_at_ts": checked_at_ts,
    }


def store_check_record(record):
    global last_check_finished_at

    snapshot = get_config_snapshot()
    if not model_is_monitorable(
        snapshot,
        record.get("endpoint_id"),
        record.get("group_id"),
        record.get("model"),
    ):
        with state_lock:
            latest_results.pop(result_key(record.get("endpoint_id"), record.get("model")), None)
        return False

    with state_lock:
        reset_at = group_reset_after.get(record.get("group_id"), 0)
    if record.get("checked_at_ts", 0) < reset_at:
        return False

    insert_history([record])
    with state_lock:
        latest_results[result_key(record["endpoint_id"], record["model"])] = record
        last_check_finished_at = now_ts()
    print(
        f"[{record['status']}] {record['endpoint_name']} / {record['model']} "
        f"TTFT={record.get('ttft_ms')}ms err={record.get('error') or ''}",
        flush=True,
    )
    return True


def trigger_check():
    wake_scheduler(refresh=True, force=True)
    return True


def checker_loop():
    global scheduler_force_check, scheduler_refresh_requested, check_running, last_check_started_at

    tasks = {}
    next_due = {}
    running_keys = set()
    running_endpoint_ids = set()
    future_map = {}
    last_discovery = 0
    last_prune = 0

    with ThreadPoolExecutor(max_workers=128) as pool:
        while True:
            now = now_ts()
            snapshot = get_config_snapshot()
            intervals = active_check_intervals(snapshot)
            discovery_interval = max(60, min(intervals) if intervals else DEFAULT_CHECK_INTERVAL)

            with state_lock:
                force_check = scheduler_force_check
                refresh_requested = scheduler_refresh_requested
                scheduler_force_check = False
                scheduler_refresh_requested = False

            if refresh_requested or force_check or not tasks or now - last_discovery >= discovery_interval:
                try:
                    discovered = discover_check_tasks(snapshot)
                    active_or_scheduled = set(running_keys) | set(discovered.keys())
                    next_due = {key: due for key, due in next_due.items() if key in active_or_scheduled}
                    for key in discovered:
                        if key not in next_due and key not in running_keys:
                            next_due[key] = now
                        elif refresh_requested and key not in running_keys:
                            _endpoint, group, _model_id = discovered[key]
                            interval = group_check_interval(snapshot, group["id"])
                            next_due[key] = min(next_due[key], now + interval)
                    tasks = discovered
                    last_discovery = now
                except Exception as exc:
                    print(f"[ERROR] discover_check_tasks: {exc}", flush=True)
                    scheduler_wakeup.wait(5)
                    scheduler_wakeup.clear()
                    continue

            if force_check:
                for key in tasks:
                    if key not in running_keys:
                        next_due[key] = now

            max_workers = min(
                max(1, snapshot.get("max_workers", DEFAULT_MAX_WORKERS) or DEFAULT_MAX_WORKERS),
                128,
            )

            while len(future_map) < max_workers:
                now = now_ts()
                due_tasks = []
                for key, due_at in next_due.items():
                    if due_at > now or key in running_keys or key not in tasks:
                        continue
                    endpoint, _group, _model_id = tasks[key]
                    # A single upstream endpoint can back many model entries.
                    # Serialize probes for that endpoint to avoid turning the
                    # monitor itself into a burst/rate-limit source.
                    if endpoint["id"] in running_endpoint_ids:
                        continue
                    due_tasks.append((due_at, key))
                if not due_tasks:
                    break

                _due_at, key = min(due_tasks)
                endpoint, group, model_id = tasks[key]
                next_due.pop(key, None)
                current_snapshot = get_config_snapshot()
                if not model_is_monitorable(current_snapshot, endpoint["id"], group["id"], model_id):
                    tasks.pop(key, None)
                    continue
                running_keys.add(key)
                running_endpoint_ids.add(endpoint["id"])
                future = pool.submit(check_model, endpoint, group, model_id)
                future_map[future] = (key, endpoint, group, model_id)
                with state_lock:
                    check_running = True
                    last_check_started_at = now_ts()

            if future_map:
                now = now_ts()
                pending_due = [
                    due
                    for key, due in next_due.items()
                    if key in tasks and key not in running_keys
                ]
                timeout = 1
                if pending_due:
                    timeout = min(timeout, max(0, min(pending_due) - now))
                done, _pending = wait(list(future_map.keys()), timeout=timeout, return_when=FIRST_COMPLETED)
                if not done:
                    if scheduler_wakeup.is_set():
                        scheduler_wakeup.clear()
                    continue

                for future in done:
                    key, endpoint, group, model_id = future_map.pop(future)
                    running_keys.discard(key)
                    running_endpoint_ids.discard(endpoint["id"])
                    try:
                        record = future.result()
                    except Exception as exc:
                        record = fallback_record(endpoint, group, model_id, exc)
                    store_check_record(record)
                    current_snapshot = get_config_snapshot()
                    if key in tasks and model_is_monitorable(current_snapshot, endpoint["id"], group["id"], model_id):
                        next_due[key] = now_ts() + group_check_interval(current_snapshot, group["id"])
                    else:
                        tasks.pop(key, None)
                        next_due.pop(key, None)

                with state_lock:
                    check_running = bool(future_map)

                current = now_ts()
                if current - last_prune >= 300:
                    prune_history(get_config_snapshot().get("history_retention_hours", DEFAULT_RETENTION_HOURS))
                    last_prune = current
                continue

            with state_lock:
                check_running = False

            if not next_due:
                scheduler_wakeup.wait(5)
                scheduler_wakeup.clear()
                continue

            wait_seconds = max(0, min(next_due.values()) - now_ts())
            scheduler_wakeup.wait(min(wait_seconds, 5))
            scheduler_wakeup.clear()


# ---------------------------------------------------------------------------
# Payloads


def current_counts(records):
    available_records = [
        record for record in records if record.get("status") in ("ok", "fluctuation")
    ]
    ttfts = [
        record.get("ttft_ms")
        for record in available_records
        if record.get("ttft_ms") is not None
    ]
    return {
        "total": len(records),
        "ok": len(available_records),
        "fluctuation": sum(record.get("status") == "fluctuation" for record in records),
        "timeout": sum(record.get("status") == "timeout" for record in records),
        "error": len(records) - len(available_records),
        "avg_ttft_ms": round(sum(ttfts) / len(ttfts), 1) if ttfts else None,
        "fastest_ttft_ms": round(min(ttfts), 1) if ttfts else None,
    }


def window_for_key(stats, key):
    empty = {"ok": 0, "total": 0, "availability": None, "avg_ttft_ms": None}
    return stats.get(key, empty)


def build_dashboard_payload():
    snapshot = get_config_snapshot()
    with state_lock:
        records = list(latest_results.values())
        known = {endpoint_id: list(models) for endpoint_id, models in known_models.items()}
        errors = dict(endpoint_errors)
        pings = dict(endpoint_ping_ms)
        started = last_check_started_at
        finished = last_check_finished_at
        running = check_running
        valid_after = history_valid_after

    groups_by_id = {group["id"]: group for group in snapshot["groups"]}
    group_order = {group["id"]: index for index, group in enumerate(snapshot["groups"])}
    endpoints_by_id = {endpoint["id"]: endpoint for endpoint in snapshot["endpoints"]}
    enabled_group_ids = {group["id"] for group in snapshot["groups"] if group.get("enabled", True)}
    active_endpoint_ids = {
        endpoint["id"]
        for endpoint in snapshot["endpoints"]
        if endpoint.get("enabled", True) and endpoint.get("group_id") in enabled_group_ids
    }
    ignored = ignored_set(snapshot)
    records = [
        record
        for record in records
        if record.get("endpoint_id") in active_endpoint_ids
        and ignored_key(record.get("endpoint_id"), record.get("model")) not in ignored
        and record.get("checked_at_ts", 0) >= valid_after
    ]
    global_windows = query_global_windows(active_endpoint_ids, ignored, valid_after)
    group_windows = query_grouped_windows(["group_id"], active_endpoint_ids, ignored, valid_after)
    endpoint_windows = query_grouped_windows(["endpoint_id"], active_endpoint_ids, ignored, valid_after)
    model_windows = query_grouped_windows(["endpoint_id", "model_id"], active_endpoint_ids, ignored, valid_after)
    recent_results = query_recent_model_results(active_endpoint_ids, ignored, 60, valid_after)

    rows = []
    for record in records:
        endpoint = endpoints_by_id.get(record["endpoint_id"], {})
        group = groups_by_id.get(record["group_id"], {})
        model_key = result_key(record["endpoint_id"], record["model"])
        public_record = {key: value for key, value in record.items() if key != "endpoint_base_url"}
        rows.append(
            {
                **public_record,
                "endpoint_enabled": endpoint.get("enabled", True),
                "group_enabled": group.get("enabled", True),
                "endpoint_ping_ms": pings.get(record["endpoint_id"]),
                "windows": {
                    window["key"]: window_for_key(model_windows[window["key"]], model_key)
                    for window in WINDOWS
                },
                "recent_results": recent_results.get(model_key, []),
            }
        )
    rows.sort(
        key=lambda item: (
            group_order.get(item.get("group_id"), 9999),
            item.get("endpoint_name", ""),
            item.get("model", ""),
        )
    )

    groups = []
    for group in snapshot["groups"]:
        group_records = [record for record in records if record.get("group_id") == group["id"]]
        groups.append(
            {
                **group,
                "current": current_counts(group_records),
                "windows": {
                    window["key"]: window_for_key(group_windows[window["key"]], group["id"])
                    for window in WINDOWS
                },
            }
        )

    endpoints = []
    for endpoint in snapshot["endpoints"]:
        endpoint_records = [record for record in records if record.get("endpoint_id") == endpoint["id"]]
        endpoint_safe = {key: value for key, value in endpoint.items() if key not in ("api_key", "base_url")}
        endpoints.append(
            {
                **endpoint_safe,
                "group_name": groups_by_id.get(endpoint["group_id"], {}).get("name", ""),
                "known_model_count": len(known.get(endpoint["id"], [])),
                "ping_ms": pings.get(endpoint["id"]),
                "fetch_error": errors.get(endpoint["id"]),
                "current": current_counts(endpoint_records),
                "windows": {
                    window["key"]: window_for_key(endpoint_windows[window["key"]], endpoint["id"])
                    for window in WINDOWS
                },
            }
        )
    endpoints.sort(
        key=lambda item: (
            group_order.get(item.get("group_id"), 9999),
            item.get("name", ""),
        )
    )

    return {
        "service": {
            "listen_port": LISTEN_PORT,
            "check_interval": snapshot["check_interval"],
            "last_check_started_at": format_time(started),
            "last_check_finished_at": format_time(finished),
            "last_check_finished_ts": finished,
            "check_running": running,
        },
        "windows": WINDOWS,
        "summary": {
            "current": current_counts(records),
            "windows": global_windows,
            "ignored_count": len(snapshot.get("ignored_models", [])),
            "api_count": len(snapshot.get("endpoints", [])),
            "group_count": len(snapshot.get("groups", [])),
        },
        "groups": groups,
        "endpoints": endpoints,
        "models": rows,
    }


def build_admin_payload():
    snapshot = get_config_snapshot()
    ignored = ignored_set(snapshot)
    qq_selected = qq_selected_set(snapshot)
    with state_lock:
        known = {endpoint_id: list(models) for endpoint_id, models in known_models.items()}
        errors = dict(endpoint_errors)
        running = check_running
        finished = last_check_finished_at

    model_payload = []
    for endpoint in snapshot["endpoints"]:
        models = []
        for model_id in known.get(endpoint["id"], []):
            models.append(
                {
                    "id": model_id,
                    "ignored": ignored_key(endpoint["id"], model_id) in ignored,
                    "qq_selected": model_ref(endpoint["id"], model_id) in qq_selected,
                }
            )
        model_payload.append(
            {
                "endpoint_id": endpoint["id"],
                "endpoint_name": endpoint["name"],
                "models": models,
                "fetch_error": errors.get(endpoint["id"]),
            }
        )
    return {
        "config": admin_config_view(snapshot),
        "models": model_payload,
        "runtime": {
            "check_running": running,
            "last_check_finished_at": format_time(finished),
            "qq_push": qq_runtime_payload(),
        },
    }


def legacy_results_payload():
    with state_lock:
        records = [
            {key: value for key, value in record.items() if key != "endpoint_base_url"}
            for record in latest_results.values()
        ]
        finished = last_check_finished_at
    return {"last_check": format_time(finished), "models": records}


# ---------------------------------------------------------------------------
# HTTP server


class ReusableHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    allow_reuse_address = True
    daemon_threads = True


class MonitorHandler(http.server.BaseHTTPRequestHandler):
    def send_auth_required(self):
        body = json.dumps({"error": "Authentication required"}, ensure_ascii=False).encode("utf-8")
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="model-monitor-admin"')
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def require_admin_auth(self):
        if not ADMIN_PASSWORD:
            return True
        header = self.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            self.send_auth_required()
            return False
        try:
            decoded = base64.b64decode(header[6:].strip()).decode("utf-8")
        except Exception:
            self.send_auth_required()
            return False
        username, separator, password = decoded.partition(":")
        if not separator or username != ADMIN_USER or password != ADMIN_PASSWORD:
            self.send_auth_required()
            return False
        return True

    def send_json(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, body):
        data = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def read_json(self):
        length = clamp_int(self.headers.get("Content-Length"), 0, 0, 2 * 1024 * 1024)
        if length == 0:
            return {}
        body = self.rfile.read(length)
        return json.loads(body.decode("utf-8"))

    def do_GET(self):
        path = urlparse(self.path).path
        try:
            if path == "/api/dashboard":
                self.send_json(200, build_dashboard_payload())
            elif path == "/api/admin/config":
                if not self.require_admin_auth():
                    return
                self.send_json(200, build_admin_payload())
            elif path == "/api/admin/qq/status":
                if not self.require_admin_auth():
                    return
                snapshot = get_config_snapshot()
                self.send_json(
                    200,
                    {
                        "ok": True,
                        "qq_push": admin_config_view(snapshot).get("qq_push", {}),
                        "runtime": qq_runtime_payload(),
                    },
                )
            elif path == "/api/results":
                self.send_json(200, legacy_results_payload())
            elif path == "/admin":
                if not self.require_admin_auth():
                    return
                self.send_html(ADMIN_PAGE)
            else:
                self.send_html(DASHBOARD_PAGE)
        except Exception as exc:
            self.send_json(500, {"error": str(exc)})

    def do_POST(self):
        path = urlparse(self.path).path
        try:
            if path == "/api/admin/config":
                if not self.require_admin_auth():
                    return
                payload = self.read_json()
                next_config = payload.get("config", payload)
                saved = save_admin_config(next_config)
                self.send_json(200, {"ok": True, "config": admin_config_view(saved)})
            elif path == "/api/admin/ignore":
                if not self.require_admin_auth():
                    return
                payload = self.read_json()
                saved = set_model_ignored(
                    safe_str(payload.get("endpoint_id")),
                    safe_str(payload.get("model_id")),
                    bool(payload.get("ignored", True)),
                )
                self.send_json(200, {"ok": True, "config": saved})
            elif path == "/api/admin/run":
                if not self.require_admin_auth():
                    return
                started = trigger_check()
                self.send_json(200, {"ok": True, "started": started})
            elif path == "/api/admin/clear-group":
                if not self.require_admin_auth():
                    return
                payload = self.read_json()
                clear_group_history(safe_str(payload.get("group_id")))
                self.send_json(200, {"ok": True})
            elif path == "/api/admin/qq/capture":
                if not self.require_admin_auth():
                    return
                started = start_qq_group_capture()
                self.send_json(
                    200,
                    {"ok": True, "started": started, "runtime": qq_runtime_payload()},
                )
            elif path == "/api/admin/qq/cancel-capture":
                if not self.require_admin_auth():
                    return
                stopped = cancel_qq_group_capture()
                self.send_json(200, {"ok": True, "stopped": stopped})
            elif path == "/api/admin/qq/unbind":
                if not self.require_admin_auth():
                    return
                unbind_qq_group()
                self.send_json(200, {"ok": True})
            elif path == "/api/admin/qq/test":
                if not self.require_admin_auth():
                    return
                push_qq_status(test=True)
                self.send_json(200, {"ok": True, "runtime": qq_runtime_payload()})
            else:
                self.send_json(404, {"error": "Not found"})
        except ValueError as exc:
            self.send_json(400, {"error": str(exc)})
        except json.JSONDecodeError:
            self.send_json(400, {"error": "JSON 格式错误"})
        except Exception as exc:
            self.send_json(500, {"error": str(exc)})

    def log_message(self, fmt, *args):
        pass


# ---------------------------------------------------------------------------
# Frontend


_LEGACY_DASHBOARD_PAGE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>模型监控</title>
<style>
  :root {
    --bg: #0f1216;
    --panel: #171b21;
    --panel-2: #1f252d;
    --line: #2d3540;
    --text: #e5e7eb;
    --muted: #8f9aa8;
    --blue: #5aa8ff;
    --green: #40c463;
    --amber: #d89b21;
    --red: #ef5b57;
    --purple: #b18cff;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--text); font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
  .shell { width: min(1500px, calc(100vw - 32px)); margin: 0 auto; padding: 22px 0 36px; }
  .topbar { display: flex; justify-content: space-between; gap: 18px; align-items: flex-start; margin-bottom: 18px; }
  h1 { margin: 0; font-size: 24px; line-height: 1.2; letter-spacing: 0; }
  .sub { color: var(--muted); margin-top: 7px; font-size: 14px; display: flex; gap: 14px; flex-wrap: wrap; }
  .actions { display: flex; gap: 10px; flex-wrap: wrap; justify-content: flex-end; }
  button, .button { border: 1px solid var(--line); color: var(--text); background: var(--panel-2); border-radius: 7px; padding: 9px 12px; font-size: 14px; cursor: pointer; text-decoration: none; line-height: 1; }
  button:hover, .button:hover { border-color: #526071; }
  button:disabled { opacity: 0.45; cursor: not-allowed; }
  button.primary { background: #1d4f83; border-color: #3477b9; }
  .green { color: var(--green); }
  .amber { color: var(--amber); }
  .red { color: var(--red); }
  .blue { color: var(--blue); }
  .purple { color: var(--purple); }
  .section { margin-top: 20px; }
  .section-head { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 10px; }
  .section h2 { margin: 0; font-size: 16px; letter-spacing: 0; }
  .filters { display: flex; gap: 8px; flex-wrap: wrap; }
  input, select { background: #11151a; border: 1px solid var(--line); border-radius: 7px; color: var(--text); padding: 8px 10px; font-size: 14px; min-height: 36px; }
  input::placeholder { color: #66717f; }
  table { width: 100%; border-collapse: collapse; background: var(--panel); border: 1px solid var(--line); border-radius: 8px; overflow: hidden; }
  th { text-align: left; color: #a9b4c2; font-size: 12px; font-weight: 650; background: #20262e; padding: 10px 12px; white-space: nowrap; }
  td { border-top: 1px solid var(--line); padding: 10px 12px; font-size: 14px; vertical-align: middle; }
  tr:hover td { background: #1b2027; }
  .status { display: inline-flex; align-items: center; gap: 7px; font-weight: 650; }
  .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; background: var(--muted); }
  .dot.ok { background: var(--green); }
  .dot.fluctuation { background: var(--amber); }
  .dot.timeout { background: var(--red); }
  .dot.error { background: var(--red); }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  .muted { color: var(--muted); }
  .empty { color: var(--muted); padding: 18px; border: 1px dashed var(--line); border-radius: 8px; background: #12161b; }
  .truncate { max-width: 420px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .ttft-history { display: flex; gap: 6px; flex-wrap: wrap; min-width: 220px; }
  .ttft-chip { display: inline-flex; align-items: center; min-height: 24px; border: 1px solid var(--line); border-radius: 6px; padding: 3px 7px; background: #12161b; font-size: 12px; }
  .ttft-chip.ok { color: var(--green); }
  .ttft-chip.warn { color: var(--amber); }
  .ttft-chip.bad, .ttft-chip.error { color: var(--red); }
  .group-cards { display: grid; grid-template-columns: 1fr; gap: 12px; }
  .group-card { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; overflow: hidden; }
  .group-card-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 12px; padding: 13px 14px; background: #20262e; border-bottom: 1px solid var(--line); }
  .group-card-title { font-size: 15px; font-weight: 700; }
  .group-card-meta { color: var(--muted); font-size: 12px; margin-top: 4px; }
  .group-card-stats { display: flex; gap: 12px; flex-wrap: wrap; justify-content: flex-end; color: var(--muted); font-size: 12px; }
  .group-card table { border: 0; border-radius: 0; }
  .group-card th:first-child, .group-card td:first-child { padding-left: 14px; }
  .group-card th:last-child, .group-card td:last-child { padding-right: 14px; }
  @media (max-width: 760px) { .shell { width: min(100vw - 20px, 1500px); padding-top: 14px; } .topbar { flex-direction: column; } .actions { justify-content: flex-start; } .table-wrap { overflow-x: auto; } table { min-width: 900px; } .group-card-head { flex-direction: column; } .group-card-stats { justify-content: flex-start; } }
</style>
</head>
<body>
<main class="shell">
  <div class="topbar">
    <div>
      <h1>模型监控</h1>
      <div class="sub">
        <span>上次检测：<strong id="lastCheck">加载中</strong></span>
        <span>检测间隔：<strong id="interval">-</strong>s</span>
        <span>状态：<strong id="runState">-</strong></span>
      </div>
    </div>
  </div>

  <section class="section">
    <div class="section-head">
      <h2>模型明细</h2>
      <div class="filters">
        <input id="search" placeholder="搜索模型或 API" autocomplete="off">
        <select id="groupFilter"><option value="">全部分组</option></select>
        <select id="statusFilter"><option value="">全部状态</option><option value="ok">正常</option><option value="timeout">超时</option><option value="error">异常</option></select>
      </div>
    </div>
    <div id="modelGroups" class="group-cards"></div>
  </section>
</main>

<script>
const state = { payload: null, refreshTimer: null };
const windows = [{key:'1h', label:'近1小时'}, {key:'3h', label:'近3小时'}, {key:'24h', label:'近1天'}];

function esc(value) {
  return String(value ?? '').replace(/[&<>'"]/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[ch]));
}
function pct(stat) {
  if (!stat || stat.availability === null || stat.availability === undefined) return '<span class="muted">-</span>';
  const value = Number(stat.availability);
  return `<span>${value.toFixed(value % 1 ? 2 : 0)}%</span><div class="muted mono">${stat.ok}/${stat.total}</div>`;
}
function ttft(value) {
  if (value === null || value === undefined) return '<span class="muted">-</span>';
  const cls = value < 10000 ? 'green' : value < 30000 ? 'amber' : 'red';
  return `<span class="${cls} mono">${Number(value).toFixed(1)}ms</span>`;
}
function statusCell(status) {
  const states = {
    ok: {label: '正常', cls: 'green', dot: 'ok'},
    fluctuation: {label: '正常', cls: 'green', dot: 'ok'},
    timeout: {label: '超时', cls: 'red', dot: 'timeout'},
    error: {label: '异常', cls: 'red', dot: 'error'},
  };
  const current = states[status] || {label: '未知', cls: 'muted', dot: ''};
  return `<span class="status ${current.cls}"><span class="dot ${current.dot}"></span>${current.label}</span>`;
}
function ttftHistory(items) {
  const history = Array.isArray(items) ? items.slice(0, 5) : [];
  if (!history.length) return '<span class="muted">-</span>';
  return `<div class="ttft-history">${history.map(item => {
    if (item && item.status === 'timeout') {
      return `<span class="ttft-chip error" title="${esc(item.error || item.checked_at || '')}">超时</span>`;
    }
    if (!item || !['ok', 'fluctuation'].includes(item.status) || item.ttft_ms === null || item.ttft_ms === undefined) {
      return `<span class="ttft-chip error" title="${esc(item && item.error || '失败')}">ERR</span>`;
    }
    const value = Number(item.ttft_ms);
    const cls = value < 10000 ? 'ok' : value < 30000 ? 'warn' : 'bad';
    return `<span class="ttft-chip ${cls}" title="${esc(item.checked_at || '')}">${value.toFixed(0)}ms</span>`;
  }).join('')}</div>`;
}
function fillFilters(payload) {
  const groupFilter = document.getElementById('groupFilter');
  const current = groupFilter.value;
  groupFilter.innerHTML = '<option value="">全部分组</option>' + payload.groups.map(group => `<option value="${esc(group.id)}">${esc(group.name)}</option>`).join('');
  groupFilter.value = current;
}
function renderModelTable(payload) {
  const search = document.getElementById('search').value.trim().toLowerCase();
  const groupId = document.getElementById('groupFilter').value;
  const status = document.getElementById('statusFilter').value;
  const filtered = payload.models.filter(row => {
    if (groupId && row.group_id !== groupId) return false;
    if (status === 'ok' && !['ok', 'fluctuation'].includes(row.status)) return false;
    if (status && status !== 'ok' && row.status !== status) return false;
    if (!search) return true;
    return `${row.group_name} ${row.endpoint_name} ${row.model}`.toLowerCase().includes(search);
  });
  const groups = payload.groups.filter(group => !groupId || group.id === groupId).map(group => ({
    group,
    rows: filtered.filter(row => row.group_id === group.id),
  })).filter(item => item.rows.length > 0 || !search && !status);
  const cards = groups.map(item => {
    const current = item.rows.reduce((acc, row) => {
      acc.total += 1;
      if (['ok', 'fluctuation'].includes(row.status)) acc.ok += 1;
      else if (row.status === 'timeout') acc.timeout += 1;
      else acc.error += 1;
      return acc;
    }, {total: 0, ok: 0, timeout: 0, error: 0});
    const rows = item.rows.map(row => `
      <tr>
        <td>${esc(row.endpoint_name)}</td>
        <td class="mono">${esc(row.model)}</td>
        <td>${statusCell(row.status)}</td>
        <td>${ttftHistory(row.recent_results)}</td>
        ${windows.map(win => `<td>${pct(row.windows[win.key])}</td>`).join('')}
        <td class="muted mono">${esc(row.checked_at || '')}</td>
        <td class="truncate ${row.status !== 'fluctuation' && row.error ? 'red' : 'muted'}">${row.status === 'fluctuation' ? '' : esc(row.error || '')}</td>
      </tr>`).join('');
    return `<article class="group-card">
      <div class="group-card-head">
        <div>
          <div class="group-card-title">${esc(item.group.name)}</div>
          <div class="group-card-meta">${item.group.enabled ? '启用' : '停用'} · ${current.total} 个模型</div>
        </div>
        <div class="group-card-stats">
          <span>正常 <strong class="green">${current.ok}</strong></span>
          <span>超时 <strong class="${current.timeout ? 'red' : 'green'}">${current.timeout}</strong></span>
          <span>异常 <strong class="${current.error ? 'red' : 'green'}">${current.error}</strong></span>
          ${windows.map(win => `<span>${win.label} ${pct(item.group.windows[win.key])}</span>`).join('')}
        </div>
      </div>
      <div class="table-wrap"><table><thead><tr><th>API</th><th>模型</th><th>当前</th><th>近5次 TTFT</th>${windows.map(w => `<th>${w.label}</th>`).join('')}<th>检测时间</th><th>错误</th></tr></thead><tbody>${rows || '<tr><td colspan="9"><div class="empty">暂无模型检测数据</div></td></tr>'}</tbody></table></div>
    </article>`;
  }).join('');
  document.getElementById('modelGroups').innerHTML = cards || '<div class="empty">暂无模型检测数据</div>';
}
function render(payload) {
  state.payload = payload;
  document.getElementById('lastCheck').textContent = payload.service.last_check_finished_at || '尚未检测';
  document.getElementById('interval').textContent = payload.service.check_interval;
  document.getElementById('runState').textContent = payload.service.check_running ? '检测中' : '空闲';
  fillFilters(payload);
  renderModelTable(payload);
}
async function load() {
  const res = await fetch('/api/dashboard', {cache:'no-store'});
  if (!res.ok) throw new Error(await res.text());
  render(await res.json());
}
['search','groupFilter','statusFilter'].forEach(id => document.getElementById(id).addEventListener('input', () => state.payload && renderModelTable(state.payload)));
load().catch(err => { document.getElementById('modelGroups').innerHTML = `<div class="empty">加载失败：${esc(err.message)}</div>`; });
setInterval(() => location.reload(), 30000);
</script>
</body>
</html>
"""


DASHBOARD_PAGE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>模型状态</title>
<style>
  :root {
    --bg: #050b12;
    --surface: #101d2c;
    --surface-2: #132338;
    --surface-soft: #0d1928;
    --line: #24364b;
    --line-bright: #30475f;
    --text: #ecf3f8;
    --muted: #8da0b2;
    --muted-2: #64788b;
    --green: #59d26a;
    --teal: #39d4af;
    --amber: #e0b72a;
    --red: #ff635f;
    --blue: #7daee4;
  }
  * { box-sizing: border-box; }
  html { min-width: 320px; background: var(--bg); }
  body {
    min-height: 100vh;
    margin: 0;
    overflow-x: hidden;
    color: var(--text);
    background:
      linear-gradient(rgba(10, 28, 43, .28) 1px, transparent 1px),
      linear-gradient(90deg, rgba(10, 28, 43, .22) 1px, transparent 1px),
      var(--bg);
    background-size: 56px 56px;
    font-family: "Avenir Next", "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
    letter-spacing: 0;
  }
  body::before {
    position: fixed;
    z-index: -1;
    inset: 0;
    pointer-events: none;
    content: "";
    background: linear-gradient(180deg, rgba(11, 37, 54, .34), transparent 42%);
  }
  button, input, select { font: inherit; }
  button, a { -webkit-tap-highlight-color: transparent; }
  button { cursor: pointer; }
  button:disabled { cursor: not-allowed; opacity: .55; }

  .shell {
    width: min(100% - 56px, 1600px);
    margin: 0 auto;
    padding: 28px 0 54px;
  }
  .topbar {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 22px;
    min-height: 58px;
    margin-bottom: 24px;
  }
  .brand {
    display: inline-flex;
    align-items: center;
    gap: 13px;
    color: var(--text);
    text-decoration: none;
  }
  .brand-mark, .model-mark {
    display: grid;
    place-items: center;
    border: 1px solid rgba(57, 212, 175, .32);
    border-radius: 12px;
    color: var(--teal);
    background: rgba(24, 100, 95, .25);
    box-shadow: inset 0 0 18px rgba(57, 212, 175, .08);
    font-size: 20px;
  }
  .brand-mark { width: 38px; height: 38px; }
  .brand h1 {
    margin: 0;
    font-size: 20px;
    font-weight: 720;
    line-height: 1.1;
  }
  .brand p {
    margin: 5px 0 0;
    color: var(--muted);
    font-size: 10px;
    letter-spacing: .12em;
    text-transform: uppercase;
  }
  .topbar-tools {
    display: flex;
    align-items: center;
    justify-content: flex-end;
    gap: 10px;
    flex-wrap: wrap;
  }
  .period-switch {
    display: inline-flex;
    align-items: center;
    gap: 2px;
    padding: 3px;
    border: 1px solid var(--line);
    border-radius: 11px;
    background: rgba(19, 35, 56, .9);
  }
  .period-button {
    min-width: 54px;
    border: 0;
    border-radius: 8px;
    padding: 6px 10px;
    color: var(--muted);
    background: transparent;
    font-size: 12px;
  }
  .period-button:hover { color: var(--text); }
  .period-button.active {
    color: var(--text);
    background: #2c3c51;
    box-shadow: 0 1px 4px rgba(0, 0, 0, .24);
    font-weight: 700;
  }
  .health-pill {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    min-height: 30px;
    border: 1px solid rgba(89, 210, 106, .22);
    border-radius: 999px;
    padding: 0 12px;
    color: var(--green);
    background: rgba(42, 124, 73, .16);
    font-size: 11px;
    font-weight: 800;
    letter-spacing: .04em;
  }
  .health-pill.degraded {
    border-color: rgba(224, 183, 42, .26);
    color: var(--amber);
    background: rgba(147, 107, 23, .18);
  }
  .health-pill.pending {
    border-color: var(--line);
    color: var(--muted);
    background: rgba(19, 35, 56, .72);
  }
  .health-dot {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    background: currentColor;
    box-shadow: 0 0 10px currentColor;
  }
  .icon-button, .admin-link {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-height: 32px;
    border: 1px solid var(--line-bright);
    border-radius: 8px;
    color: var(--muted);
    background: rgba(19, 35, 56, .86);
    text-decoration: none;
    font-size: 12px;
  }
  .icon-button {
    width: 34px;
    padding: 0;
    font-size: 18px;
    line-height: 1;
  }
  .icon-button:hover, .admin-link:hover {
    border-color: #4b667f;
    color: var(--text);
  }
  .admin-link { gap: 6px; padding: 0 10px; }
  .refresh-copy {
    color: var(--muted);
    font-size: 11px;
    white-space: nowrap;
  }
  .refresh-copy strong {
    color: var(--blue);
    font-variant-numeric: tabular-nums;
  }

  .toolbar {
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    gap: 18px;
    margin-bottom: 18px;
  }
  .toolbar-title {
    display: flex;
    align-items: baseline;
    gap: 10px;
    min-width: 0;
  }
  .toolbar-title h2 {
    margin: 0;
    font-size: 16px;
    font-weight: 700;
  }
  .toolbar-title span {
    overflow: hidden;
    color: var(--muted);
    font-size: 12px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .filters {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    justify-content: flex-end;
  }
  .search-box {
    display: flex;
    align-items: center;
    gap: 7px;
    width: 220px;
    min-height: 35px;
    border: 1px solid var(--line);
    border-radius: 8px;
    padding: 0 10px;
    background: rgba(13, 25, 40, .9);
  }
  .search-icon {
    color: var(--muted-2);
    font-size: 17px;
    line-height: 1;
  }
  .search-box input {
    width: 100%;
    min-width: 0;
    border: 0;
    outline: 0;
    color: var(--text);
    background: transparent;
    font-size: 12px;
  }
  .search-box:focus-within, .filter-select:focus {
    border-color: #4c7891;
    box-shadow: 0 0 0 3px rgba(57, 212, 175, .07);
  }
  .search-box input::placeholder { color: var(--muted-2); }
  .filter-select {
    min-height: 35px;
    border: 1px solid var(--line);
    border-radius: 8px;
    outline: 0;
    padding: 0 10px;
    color: var(--muted);
    background: rgba(13, 25, 40, .9);
    font-size: 12px;
  }

  .model-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 20px;
    align-items: stretch;
  }
  .model-card {
    position: relative;
    display: flex;
    min-width: 0;
    min-height: 340px;
    flex-direction: column;
    overflow: hidden;
    border: 1px solid var(--line);
    border-radius: 15px;
    padding: 19px 20px 17px;
    background: linear-gradient(145deg, rgba(20, 37, 55, .98), rgba(14, 27, 43, .98));
    box-shadow: 0 12px 30px rgba(0, 0, 0, .12);
    transition: border-color .18s ease, transform .18s ease, box-shadow .18s ease;
  }
  .model-card::before {
    position: absolute;
    top: 0;
    right: 22px;
    left: 22px;
    height: 1px;
    content: "";
    background: linear-gradient(90deg, transparent, rgba(57, 212, 175, .28), transparent);
  }
  .model-card:hover {
    border-color: var(--line-bright);
    box-shadow: 0 18px 38px rgba(0, 0, 0, .2);
    transform: translateY(-2px);
  }
  .model-card.group-card { cursor: pointer; }
  .model-card.group-card:focus-visible {
    outline: 2px solid var(--teal);
    outline-offset: 3px;
  }
  .model-card.has-error::before {
    background: linear-gradient(90deg, transparent, rgba(255, 99, 95, .5), transparent);
  }
  .card-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 10px;
    min-height: 57px;
  }
  .identity {
    display: flex;
    min-width: 0;
    align-items: center;
    gap: 11px;
  }
  .model-mark { width: 38px; height: 38px; flex: 0 0 auto; }
  .identity-copy { min-width: 0; }
  .group-label {
    overflow: hidden;
    margin-bottom: 4px;
    color: var(--muted);
    font-size: 10px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .identity-copy h2 {
    overflow: hidden;
    margin: 0;
    color: var(--text);
    font-size: 16px;
    font-weight: 750;
    line-height: 1.15;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .model-line {
    display: flex;
    min-width: 0;
    align-items: center;
    gap: 6px;
    margin-top: 6px;
  }
  .api-badge {
    flex: 0 0 auto;
    border-radius: 4px;
    padding: 3px 6px;
    color: var(--teal);
    background: rgba(22, 119, 101, .34);
    font-size: 10px;
    font-weight: 700;
  }
  .model-id {
    overflow: hidden;
    color: #a8b7c5;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 10px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .state-badge {
    flex: 0 0 auto;
    border: 1px solid rgba(89, 210, 106, .2);
    border-radius: 999px;
    padding: 5px 9px;
    color: var(--green);
    background: rgba(42, 124, 73, .17);
    font-size: 10px;
    font-weight: 750;
    white-space: nowrap;
  }
  .state-badge.danger {
    border-color: rgba(255, 99, 95, .22);
    color: var(--red);
    background: rgba(149, 43, 48, .17);
  }
  .state-badge.pending {
    border-color: var(--line);
    color: var(--muted);
    background: rgba(100, 120, 139, .14);
  }

  .metric-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 9px;
    margin-top: 18px;
  }
  .metric {
    min-width: 0;
    min-height: 75px;
    border: 1px solid rgba(48, 71, 95, .65);
    border-radius: 12px;
    padding: 12px 12px 10px;
    background: rgba(13, 28, 45, .58);
  }
  .metric-label {
    display: flex;
    align-items: center;
    gap: 6px;
    overflow: hidden;
    color: var(--muted);
    font-size: 10px;
    font-weight: 650;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .metric-icon { color: var(--blue); font-size: 12px; }
  .metric-value {
    display: flex;
    align-items: baseline;
    gap: 3px;
    margin-top: 10px;
    overflow: hidden;
    color: var(--text);
    font-size: 17px;
    font-weight: 780;
    line-height: 1;
    white-space: nowrap;
  }
  .metric-value small {
    color: var(--muted);
    font-size: 10px;
    font-weight: 550;
  }
  .metric-value.good { color: var(--green); }
  .metric-value.warn { color: var(--amber); }
  .metric-value.bad { color: var(--red); }
  .metric-value.muted { color: var(--muted-2); }

  .availability {
    display: flex;
    align-items: flex-end;
    justify-content: space-between;
    gap: 12px;
    margin-top: 16px;
    border-top: 1px solid rgba(48, 71, 95, .68);
    padding-top: 13px;
  }
  .availability-label { color: var(--muted); font-size: 11px; white-space: nowrap; }
  .availability-count {
    margin-top: 6px;
    color: var(--muted-2);
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 9px;
  }
  .availability-value {
    color: var(--green);
    font-size: 29px;
    font-weight: 800;
    letter-spacing: -.02em;
    line-height: 1;
    white-space: nowrap;
  }
  .availability-value.warn { color: var(--amber); }
  .availability-value.bad { color: var(--red); }
  .availability-value.muted { color: var(--muted-2); }

  .history {
    margin-top: 16px;
    border-top: 1px solid rgba(48, 71, 95, .68);
    padding-top: 12px;
  }
  .history-head {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    color: var(--muted);
    font-size: 10px;
    font-weight: 650;
  }
  .history-head strong {
    color: var(--muted);
    font-size: 9px;
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }
  .history-track {
    display: flex;
    align-items: flex-end;
    gap: 2px;
    height: 45px;
    margin-top: 7px;
    border-bottom: 1px dashed rgba(141, 160, 178, .34);
    padding: 0 1px;
  }
  .history-bar {
    flex: 1 1 0;
    min-width: 2px;
    max-width: 6px;
    height: var(--bar-height);
    min-height: 4px;
    border-radius: 2px 2px 0 0;
    background: var(--green);
    opacity: .95;
  }
  .history-bar.warn { background: var(--amber); }
  .history-bar.bad { background: var(--red); }
  .history-empty {
    align-self: center;
    margin: 0 auto;
    color: var(--muted-2);
    font-size: 10px;
  }
  .history-foot {
    display: flex;
    justify-content: space-between;
    margin-top: 5px;
    color: var(--muted-2);
    font-size: 9px;
    letter-spacing: .08em;
    text-transform: uppercase;
  }
  .card-alert {
    overflow: hidden;
    margin-top: 8px;
    color: var(--red);
    font-size: 9px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .card-footer {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 8px;
    margin-top: auto;
    padding-top: 12px;
    color: var(--muted);
    font-size: 10px;
  }
  .card-footer strong {
    color: var(--teal);
    font-weight: 700;
  }
  .card-footer span:last-child {
    color: var(--blue);
    font-size: 15px;
    line-height: 1;
  }
  .empty-state {
    grid-column: 1 / -1;
    border: 1px dashed var(--line-bright);
    border-radius: 12px;
    padding: 52px 20px;
    color: var(--muted);
    background: rgba(13, 25, 40, .7);
    text-align: center;
  }
  .empty-state strong {
    display: block;
    margin-bottom: 8px;
    color: var(--text);
    font-size: 15px;
  }
  .modal[hidden] { display: none; }
  .modal {
    position: fixed;
    z-index: 20;
    inset: 0;
    display: grid;
    place-items: center;
    padding: 20px;
  }
  .modal-backdrop {
    position: absolute;
    inset: 0;
    background: rgba(1, 5, 9, .78);
    backdrop-filter: blur(5px);
  }
  .modal-panel {
    position: relative;
    display: flex;
    width: min(100%, 1120px);
    max-height: min(760px, calc(100vh - 40px));
    flex-direction: column;
    overflow: hidden;
    border: 1px solid var(--line-bright);
    border-radius: 15px;
    background: #151f2c;
    box-shadow: 0 24px 80px rgba(0, 0, 0, .46);
  }
  .modal-head, .modal-foot {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 14px;
    padding: 18px 22px;
  }
  .modal-head { border-bottom: 1px solid var(--line); }
  .modal-head h2 {
    margin: 0;
    color: var(--text);
    font-size: 18px;
  }
  .modal-head p {
    margin: 5px 0 0;
    color: var(--muted);
    font-size: 11px;
  }
  .modal-close {
    width: 32px;
    height: 32px;
    flex: 0 0 auto;
    border: 0;
    border-radius: 8px;
    color: var(--muted);
    background: transparent;
    font-size: 23px;
    line-height: 1;
  }
  .modal-close:hover { color: var(--text); background: rgba(255, 255, 255, .06); }
  .modal-body { overflow: auto; padding: 0 22px; }
  .detail-table {
    width: 100%;
    min-width: 760px;
    border-collapse: collapse;
  }
  .detail-table th {
    padding: 13px 10px;
    color: var(--muted);
    font-size: 10px;
    font-weight: 650;
    text-align: left;
    white-space: nowrap;
  }
  .detail-table td {
    border-top: 1px solid rgba(48, 71, 95, .7);
    padding: 13px 10px;
    color: var(--text);
    font-size: 12px;
    vertical-align: middle;
  }
  .detail-model { max-width: 300px; }
  .detail-model strong {
    display: block;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .detail-model small {
    display: block;
    overflow: hidden;
    margin-top: 4px;
    color: var(--muted-2);
    font-size: 10px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .detail-status {
    display: inline-flex;
    border-radius: 999px;
    padding: 4px 8px;
    color: var(--green);
    background: rgba(42, 124, 73, .18);
    font-size: 10px;
    font-weight: 700;
    white-space: nowrap;
  }
  .detail-status.danger { color: var(--red); background: rgba(149, 43, 48, .2); }
  .detail-number { font-variant-numeric: tabular-nums; white-space: nowrap; }
  .detail-number.good { color: var(--green); }
  .detail-number.warn { color: var(--amber); }
  .detail-number.bad { color: var(--red); }
  .detail-number.muted { color: var(--muted-2); }
  .modal-foot {
    justify-content: flex-end;
    border-top: 1px solid var(--line);
  }
  .modal-foot button {
    min-width: 68px;
    border: 1px solid var(--line-bright);
    border-radius: 8px;
    padding: 8px 13px;
    color: var(--text);
    background: rgba(19, 35, 56, .86);
    font-size: 12px;
  }
  body.modal-open { overflow: hidden; }
  .loading .model-card { opacity: .72; }

  @media (max-width: 1260px) {
    .model-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  }
  @media (max-width: 940px) {
    .shell { width: min(100% - 32px, 760px); }
    .topbar { align-items: stretch; flex-direction: column; }
    .topbar-tools { justify-content: flex-start; }
    .toolbar { align-items: flex-start; flex-direction: column; }
    .filters { justify-content: flex-start; }
    .model-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  }
  @media (max-width: 600px) {
    .shell { width: min(100% - 20px, 460px); padding-top: 16px; }
    .topbar { margin-bottom: 18px; }
    .period-button { min-width: 47px; padding-right: 7px; padding-left: 7px; }
    .toolbar-title { align-items: flex-start; flex-direction: column; gap: 4px; }
    .filters { width: 100%; }
    .search-box { flex: 1 1 100%; width: auto; }
    .filter-select { flex: 1 1 0; min-width: 0; }
    .model-grid { grid-template-columns: 1fr; gap: 14px; }
    .model-card { min-height: 332px; }
    .modal { padding: 10px; }
    .modal-panel { max-height: calc(100vh - 20px); }
    .modal-head, .modal-foot { padding-right: 14px; padding-left: 14px; }
    .modal-body { padding-right: 4px; padding-left: 4px; }
  }
</style>
</head>
<body>
<main class="shell">
  <header class="topbar">
    <a class="brand" href="/" aria-label="模型状态首页">
      <span class="brand-mark" aria-hidden="true">✦</span>
      <span>
        <h1>模型状态</h1>
        <p>Live availability monitor</p>
      </span>
    </a>
    <div class="topbar-tools">
      <div class="period-switch" role="tablist" aria-label="统计周期">
        <button class="period-button active" data-period="1h" type="button" role="tab" aria-selected="true">1小时</button>
        <button class="period-button" data-period="3h" type="button" role="tab" aria-selected="false">3小时</button>
        <button class="period-button" data-period="24h" type="button" role="tab" aria-selected="false">24小时</button>
      </div>
      <span id="overallHealth" class="health-pill pending"><span class="health-dot"></span><strong>INITIALIZING</strong></span>
      <button id="refreshButton" class="icon-button" type="button" title="立即刷新" aria-label="立即刷新">↻</button>
      <span class="refresh-copy">自动刷新: <strong id="refreshCountdown">30s</strong></span>
      <a class="admin-link" href="/admin" title="打开管理后台">管理 <span aria-hidden="true">→</span></a>
    </div>
  </header>

  <section class="toolbar" aria-label="模型筛选">
    <div class="toolbar-title">
      <h2>模型状态</h2>
      <span id="gridSummary">正在加载检测数据</span>
    </div>
    <div class="filters">
      <label class="search-box" title="搜索模型或 API">
        <span class="search-icon" aria-hidden="true">⌕</span>
        <input id="search" placeholder="搜索模型或 API" autocomplete="off">
      </label>
      <select id="groupFilter" class="filter-select" aria-label="按分组筛选"><option value="">全部分组</option></select>
      <select id="statusFilter" class="filter-select" aria-label="按状态筛选">
        <option value="">全部状态</option>
        <option value="ok">正常</option>
        <option value="timeout">超时</option>
        <option value="error">异常</option>
      </select>
    </div>
  </section>

  <section id="modelGrid" class="model-grid" aria-live="polite">
    <div class="empty-state"><strong>正在读取模型状态</strong>等待第一次检测结果</div>
  </section>
</main>

<div id="groupModal" class="modal" hidden role="dialog" aria-modal="true" aria-labelledby="modalTitle">
  <div class="modal-backdrop" data-modal-close></div>
  <section class="modal-panel">
    <header class="modal-head">
      <div>
        <h2 id="modalTitle">分组详情</h2>
        <p id="modalMeta"></p>
      </div>
      <button id="modalClose" class="modal-close" type="button" title="关闭详情" aria-label="关闭详情">×</button>
    </header>
    <div class="modal-body">
      <table id="detailTable" class="detail-table"></table>
    </div>
    <footer class="modal-foot"><button id="modalCloseFooter" type="button">关闭</button></footer>
  </section>
</div>

<script>
const REFRESH_SECONDS = 30;
const state = {payload: null, period: '1h', loading: false, refreshSeconds: REFRESH_SECONDS, openGroupId: null, lastFocused: null};

function esc(value) {
  return String(value ?? '').replace(/[&<>'"]/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[ch]));
}
function finiteNumber(value) {
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}
function formatMs(value) {
  const number = finiteNumber(value);
  return number === null ? '--' : String(Math.max(0, Math.round(number)));
}
function latencyTone(value) {
  const number = finiteNumber(value);
  if (number === null) return 'muted';
  if (number <= 10000) return 'good';
  if (number <= 30000) return 'warn';
  return 'bad';
}
function metricHtml(value, tone) {
  const number = finiteNumber(value);
  if (number === null) return '<span class="metric-value muted">--</span>';
  return `<span class="metric-value ${tone}"><strong>${formatMs(number)}</strong><small>ms</small></span>`;
}
function availabilityTone(value) {
  const number = finiteNumber(value);
  if (number === null) return 'muted';
  if (number >= 90) return '';
  if (number >= 75) return 'warn';
  return 'bad';
}
function periodLabel() {
  return ({'1h': '1小时', '3h': '3小时', '24h': '24小时'})[state.period] || '1小时';
}
function currentStat(row) {
  return row && row.windows && row.windows[state.period] ? row.windows[state.period] : null;
}
function statusMeta(row) {
  if (row && row.status === 'timeout') return {label: '超时', tone: 'danger'};
  if (row && row.status === 'error') return {label: '异常', tone: 'danger'};
  return {label: '正常', tone: 'good'};
}
function barTone(item) {
  if (!item) return 'empty';
  if (item.status === 'timeout' || item.status === 'error') return 'bad';
  const tone = latencyTone(item.ttft_ms);
  return tone === 'warn' ? 'warn' : tone === 'bad' ? 'bad' : 'good';
}
function barHeight(item) {
  const tone = barTone(item);
  if (tone === 'good') return 25;
  if (tone === 'warn') return 14;
  if (tone === 'bad') return 7;
  return 9;
}
function historyHtml(items) {
  const history = Array.isArray(items) ? items.slice(0, 60).reverse() : [];
  if (!history.length) return '<div class="history-track"><span class="history-empty">等待检测</span></div>';
  const bars = history.map(item => {
    const tone = barTone(item);
    const title = item.status === 'timeout' ? '超时' : item.status === 'error' ? (item.error || '异常') : `${formatMs(item.ttft_ms)}ms`;
    return `<span class="history-bar ${tone}" style="--bar-height:${barHeight(item)}px" title="${esc(title)}"></span>`;
  }).join('');
  return `<div class="history-track">${bars}</div>`;
}
function groupRows(payload, group) {
  return (payload.models || []).filter(row => row.group_id === group.id);
}
function defaultRowFor(group, rows) {
  const reference = group.default_model;
  if (reference) {
    const configured = rows.find(row => row.endpoint_id === reference.endpoint_id && row.model === reference.model_id);
    if (configured) return configured;
  }
  return rows[0] || null;
}
function cardHtml(group, rows) {
  const row = defaultRowFor(group, rows);
  const meta = row ? statusMeta(row) : {label: '等待', tone: 'pending'};
  const stat = row ? (currentStat(row) || {}) : ((group.windows && group.windows[state.period]) || {});
  const availability = finiteNumber(stat.availability);
  const availabilityText = availability === null ? '--' : `${availability.toFixed(2)}%`;
  const errorTitle = row && (row.error || (meta.tone === 'danger' ? meta.label : ''));
  const cardClass = meta.tone === 'danger' ? 'model-card group-card has-error' : 'model-card group-card';
  const defaultLabel = row ? `${row.endpoint_name || 'API'} / ${row.model || '未知模型'}` : '尚未发现模型';
  return `<article class="${cardClass}" data-group-id="${esc(group.id)}" tabindex="0" role="button" aria-label="打开${esc(group.name)}分组详情" title="${esc(errorTitle || '查看分组详情')}">
    <div class="card-head">
      <div class="identity">
        <span class="model-mark" aria-hidden="true">✦</span>
        <div class="identity-copy">
          <div class="group-label">${esc(group.description || `${rows.length} 个模型`)}</div>
          <h2>${esc(group.name || '未命名分组')}</h2>
          <div class="model-line"><span class="api-badge">默认</span><span class="model-id" title="${esc(defaultLabel)}">${esc(defaultLabel)}</span></div>
        </div>
      </div>
      <span class="state-badge ${meta.tone === 'danger' ? 'danger' : meta.tone === 'pending' ? 'pending' : ''}">${meta.label}</span>
    </div>

    <div class="metric-grid">
      <div class="metric">
        <div class="metric-label"><span class="metric-icon" aria-hidden="true">↯</span>响应延迟</div>
        ${metricHtml(row && row.ttft_ms, latencyTone(row && row.ttft_ms))}
      </div>
      <div class="metric">
        <div class="metric-label"><span class="metric-icon" aria-hidden="true">⊙</span>端点 PING</div>
        ${metricHtml(row && row.endpoint_ping_ms, latencyTone(row && row.endpoint_ping_ms))}
      </div>
    </div>

    <div class="availability">
      <div>
        <div class="availability-label">可用性 · ${periodLabel()}</div>
        <div class="availability-count">${stat.ok ?? 0} / ${stat.total ?? 0} 次成功</div>
      </div>
      <strong class="availability-value ${availabilityTone(availability)}">${availabilityText}</strong>
    </div>

    <div class="history">
      <div class="history-head"><span>近 60 次记录</span><strong>${esc(row ? (row.checked_at || '尚未检测') : '尚未检测')}</strong></div>
      ${historyHtml(row ? row.recent_results : [])}
      <div class="history-foot"><span>past</span><span>now</span></div>
    </div>
    ${meta.tone === 'danger' && row && row.error ? `<div class="card-alert">${esc(row.error)}</div>` : ''}
    <div class="card-footer"><strong>${rows.length ? `查看全部 ${rows.length} 个模型` : '暂无检测模型'}</strong><span aria-hidden="true">→</span></div>
  </article>`;
}
function fillFilters(payload) {
  const select = document.getElementById('groupFilter');
  const current = select.value;
  const groups = Array.isArray(payload.groups) ? payload.groups : [];
  select.innerHTML = '<option value="">全部分组</option>' + groups.map(group => `<option value="${esc(group.id)}">${esc(group.name)}</option>`).join('');
  select.value = groups.some(group => group.id === current) ? current : '';
}
function rowMatchesStatus(row, status) {
  if (!status) return true;
  if (status === 'ok') return ['ok', 'fluctuation'].includes(row.status);
  return row.status === status;
}
function rowMatchesSearch(row, search) {
  return `${row.group_name || ''} ${row.endpoint_name || ''} ${row.model || ''}`.toLowerCase().includes(search);
}
function visibleGroups(payload) {
  const search = document.getElementById('search').value.trim().toLowerCase();
  const groupId = document.getElementById('groupFilter').value;
  const status = document.getElementById('statusFilter').value;
  return (payload.groups || []).map(group => {
    const rows = groupRows(payload, group);
    return {group, rows};
  }).filter(item => {
    const {group, rows} = item;
    if (group.enabled === false) return false;
    if (groupId && group.id !== groupId) return false;
    if (status && !rows.some(row => rowMatchesStatus(row, status))) return false;
    if (!search) return true;
    const groupText = `${group.name || ''} ${group.description || ''}`.toLowerCase();
    return groupText.includes(search) || rows.some(row => rowMatchesSearch(row, search));
  });
}
function renderHealth(payload) {
  const current = payload.summary && payload.summary.current ? payload.summary.current : {};
  const el = document.getElementById('overallHealth');
  let label = 'INITIALIZING';
  let tone = 'pending';
  if (current.total) {
    label = current.error ? 'DEGRADED' : 'OPERATIONAL';
    tone = current.error ? 'degraded' : '';
  }
  el.className = `health-pill ${tone}`;
  el.querySelector('strong').textContent = label;
}
function detailNumber(value, tone='muted') {
  const number = finiteNumber(value);
  return `<span class="detail-number ${number === null ? 'muted' : tone}">${number === null ? '--' : formatMs(number)}${number === null ? '' : 'ms'}</span>`;
}
function detailAvailability(row) {
  const stat = currentStat(row) || {};
  const value = finiteNumber(stat.availability);
  return `<span class="detail-number ${availabilityTone(value)}">${value === null ? '--' : `${value.toFixed(2)}%`}</span>`;
}
function detailTableHtml(rows) {
  const body = rows.length ? rows.map(row => {
    const meta = statusMeta(row);
    return `<tr title="${esc(row.error || '')}">
      <td class="detail-model"><strong>${esc(row.model || '未知模型')}</strong><small>${esc(row.endpoint_name || '未命名 API')}</small></td>
      <td><span class="detail-status ${meta.tone === 'danger' ? 'danger' : ''}">${meta.label}</span></td>
      <td>${detailNumber(row.ttft_ms, latencyTone(row.ttft_ms))}</td>
      <td>${detailNumber(row.endpoint_ping_ms, latencyTone(row.endpoint_ping_ms))}</td>
      <td>${detailAvailability(row)}</td>
      <td class="detail-number">${esc(row.checked_at || '--')}</td>
    </tr>`;
  }).join('') : '<tr><td colspan="6" class="muted">该分组尚未发现可监控模型</td></tr>';
  return `<thead><tr><th>模型</th><th>最新状态</th><th>最新延迟（ms）</th><th>端点 PING（ms）</th><th>${periodLabel()}可用率</th><th>最近检测</th></tr></thead><tbody>${body}</tbody>`;
}
function renderGroupModal(group, rows) {
  const defaultRow = defaultRowFor(group, rows);
  document.getElementById('modalTitle').textContent = group.name || '分组详情';
  document.getElementById('modalMeta').textContent = `${rows.length} 个模型 · 默认显示 ${defaultRow ? defaultRow.model : '自动选择第一个'}`;
  document.getElementById('detailTable').innerHTML = detailTableHtml(rows);
}
function openGroupModal(groupId) {
  const payload = state.payload;
  const group = payload && (payload.groups || []).find(item => item.id === groupId);
  if (!group) return;
  state.openGroupId = groupId;
  state.lastFocused = document.activeElement;
  renderGroupModal(group, groupRows(payload, group));
  document.getElementById('groupModal').hidden = false;
  document.body.classList.add('modal-open');
  document.getElementById('modalClose').focus();
}
function closeGroupModal(restoreFocus=true) {
  state.openGroupId = null;
  document.getElementById('groupModal').hidden = true;
  document.body.classList.remove('modal-open');
  if (restoreFocus && state.lastFocused && document.contains(state.lastFocused)) state.lastFocused.focus();
  state.lastFocused = null;
}
function bindGroupCards() {
  document.querySelectorAll('.group-card').forEach(card => {
    const open = () => openGroupModal(card.dataset.groupId);
    card.addEventListener('click', open);
    card.addEventListener('keydown', event => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        open();
      }
    });
  });
}
function render(payload) {
  state.payload = payload;
  fillFilters(payload);
  const groups = visibleGroups(payload);
  const totalGroups = (payload.groups || []).length;
  const totalModels = (payload.models || []).length;
  document.getElementById('gridSummary').textContent = `${groups.length} / ${totalGroups} 个分组 · ${totalModels} 个模型 · 最近更新 ${payload.service.last_check_finished_at || '等待中'}`;
  renderHealth(payload);
  const grid = document.getElementById('modelGrid');
  grid.classList.toggle('loading', Boolean(payload.service.check_running));
  grid.innerHTML = groups.length
    ? groups.map(item => cardHtml(item.group, item.rows)).join('')
    : '<div class="empty-state"><strong>没有匹配的分组</strong>调整筛选条件后重试</div>';
  bindGroupCards();
  if (state.openGroupId) {
    const openGroup = (payload.groups || []).find(group => group.id === state.openGroupId);
    if (openGroup) renderGroupModal(openGroup, groupRows(payload, openGroup));
    else closeGroupModal(false);
  }
}
async function load() {
  if (state.loading) return;
  state.loading = true;
  try {
    const res = await fetch('/api/dashboard', {cache: 'no-store'});
    if (!res.ok) throw new Error(await res.text());
    render(await res.json());
    state.refreshSeconds = REFRESH_SECONDS;
  } catch (error) {
    document.getElementById('modelGrid').innerHTML = `<div class="empty-state"><strong>加载失败</strong>${esc(error.message)}</div>`;
    document.getElementById('overallHealth').className = 'health-pill degraded';
    document.getElementById('overallHealth').querySelector('strong').textContent = 'OFFLINE';
  } finally {
    state.loading = false;
  }
}
document.querySelectorAll('.period-button').forEach(button => button.addEventListener('click', () => {
  state.period = button.dataset.period;
  document.querySelectorAll('.period-button').forEach(item => {
    const active = item === button;
    item.classList.toggle('active', active);
    item.setAttribute('aria-selected', active ? 'true' : 'false');
  });
  if (state.payload) render(state.payload);
}));
['search', 'groupFilter', 'statusFilter'].forEach(id => {
  document.getElementById(id).addEventListener(id === 'search' ? 'input' : 'change', () => state.payload && render(state.payload));
});
document.getElementById('modalClose').addEventListener('click', () => closeGroupModal());
document.getElementById('modalCloseFooter').addEventListener('click', () => closeGroupModal());
document.querySelectorAll('[data-modal-close]').forEach(element => element.addEventListener('click', () => closeGroupModal()));
document.addEventListener('keydown', event => {
  if (event.key === 'Escape' && state.openGroupId) closeGroupModal();
});
document.getElementById('refreshButton').addEventListener('click', () => load());
setInterval(() => {
  state.refreshSeconds = Math.max(0, state.refreshSeconds - 1);
  document.getElementById('refreshCountdown').textContent = `${state.refreshSeconds}s`;
  if (state.refreshSeconds === 0) load();
}, 1000);
load();
</script>
</body>
</html>
"""

ADMIN_PAGE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>模型监控管理后台</title>
<style>
  :root {
    --bg: #0f1216;
    --panel: #171b21;
    --panel-2: #20262e;
    --line: #2d3540;
    --text: #e5e7eb;
    --muted: #8f9aa8;
    --blue: #5aa8ff;
    --green: #40c463;
    --amber: #d89b21;
    --red: #ef5b57;
  }
  * { box-sizing: border-box; }
  body { margin: 0; background: var(--bg); color: var(--text); font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }
  .shell { width: min(1420px, calc(100vw - 32px)); margin: 0 auto; padding: 22px 0 40px; }
  .topbar { display: flex; justify-content: space-between; gap: 18px; align-items: flex-start; margin-bottom: 18px; }
  h1 { margin: 0; font-size: 24px; letter-spacing: 0; }
  h2 { margin: 0; font-size: 16px; }
  .sub { color: var(--muted); margin-top: 7px; font-size: 14px; }
  .actions { display: flex; flex-wrap: wrap; gap: 10px; justify-content: flex-end; }
  button, .button { border: 1px solid var(--line); color: var(--text); background: var(--panel-2); border-radius: 7px; padding: 9px 12px; font-size: 14px; cursor: pointer; text-decoration: none; line-height: 1; }
  button:hover, .button:hover { border-color: #526071; }
  button.primary { background: #1d4f83; border-color: #3477b9; }
  button.danger { color: #ffd5d2; border-color: #73302e; background: #3a1d1d; }
  input, select, textarea { width: 100%; background: #11151a; border: 1px solid var(--line); border-radius: 7px; color: var(--text); padding: 8px 10px; font-size: 14px; min-height: 36px; }
  textarea { min-height: 70px; resize: vertical; }
  label { color: var(--muted); font-size: 12px; display: block; margin-bottom: 6px; }
  .section { margin-top: 18px; }
  .section-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 10px; }
  .panel { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 14px; }
  .grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px; }
  .grid-2 { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; }
  table { width: 100%; border-collapse: collapse; background: var(--panel); border: 1px solid var(--line); border-radius: 8px; overflow: hidden; }
  th { text-align: left; color: #a9b4c2; font-size: 12px; font-weight: 650; background: #20262e; padding: 10px; white-space: nowrap; }
  td { border-top: 1px solid var(--line); padding: 10px; font-size: 14px; vertical-align: top; }
  .muted { color: var(--muted); }
  .green { color: var(--green); }
  .amber { color: var(--amber); }
  .red { color: var(--red); }
  .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
  .row-actions { display: flex; gap: 8px; flex-wrap: wrap; }
  .models { display: grid; grid-template-columns: repeat(auto-fill, minmax(250px, 1fr)); gap: 8px; }
  .model-toggle { display: flex; gap: 8px; align-items: flex-start; border: 1px solid var(--line); border-radius: 7px; padding: 8px; background: #12161b; }
  .model-toggle input { width: auto; min-height: 0; margin-top: 2px; }
  .model-toggle span { word-break: break-all; font-size: 13px; }
  .toolbar { display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }
  .toolbar input { width: min(360px, 100%); }
  .check-field { min-height: 36px; display: flex; align-items: center; gap: 9px; padding: 8px 10px; background: #11151a; border: 1px solid var(--line); border-radius: 7px; }
  .check-field input { width: auto; min-height: 0; margin: 0; }
  .check-field span { color: var(--text); font-size: 14px; }
  .status-line { display: flex; gap: 16px; flex-wrap: wrap; align-items: center; color: var(--muted); font-size: 13px; }
  .status-line span { min-width: 0; overflow-wrap: anywhere; }
  .status-pill { display: inline-flex; align-items: center; min-height: 28px; padding: 5px 9px; border: 1px solid var(--line); border-radius: 6px; background: #11151a; font-size: 13px; }
  .qq-controls { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; }
  .qq-model-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-bottom: 10px; }
  .qq-model-group + .qq-model-group { margin-top: 14px; padding-top: 14px; border-top: 1px solid var(--line); }
  .notice { min-height: 20px; font-size: 14px; }
  .empty { color: var(--muted); padding: 18px; border: 1px dashed var(--line); border-radius: 8px; background: #12161b; }
  @media (max-width: 1050px) { .grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } .grid-2 { grid-template-columns: 1fr; } }
  @media (max-width: 720px) { .shell { width: min(100vw - 20px, 1420px); padding-top: 14px; } .topbar { flex-direction: column; } .actions { justify-content: flex-start; } .grid { grid-template-columns: 1fr; } .table-wrap { overflow-x: auto; } table { min-width: 960px; } }
</style>
</head>
<body>
<main class="shell">
  <div class="topbar">
    <div>
      <h1>管理后台</h1>
      <div class="sub">上次检测：<strong id="lastCheck">加载中</strong> · 状态：<strong id="runState">-</strong></div>
    </div>
    <div class="actions">
      <button class="primary" id="saveBtn" type="button">保存配置</button>
      <button id="runBtn" type="button">立即检测</button>
      <a class="button" href="/">返回仪表盘</a>
    </div>
  </div>
  <div id="notice" class="notice muted"></div>

  <section class="section panel">
    <div class="section-head"><h2>全局设置</h2></div>
    <div class="grid">
      <div><label>检测间隔（秒）</label><input id="checkInterval" type="number" min="10" max="86400"></div>
      <div><label>并发数</label><input id="maxWorkers" type="number" min="1" max="128"></div>
      <div><label>历史保留（小时）</label><input id="retention" type="number" min="24" max="2160"></div>
      <div><label>已忽略模型</label><input id="ignoredCount" readonly></div>
    </div>
  </section>

  <section class="section panel">
    <div class="section-head">
      <h2>QQ群状态推送</h2>
      <span id="qqPushState" class="status-pill muted">未启用</span>
    </div>
    <div class="grid">
      <div>
        <label>定时推送</label>
        <label class="check-field"><input id="qqEnabled" type="checkbox"><span>启用</span></label>
      </div>
      <div>
        <label>@ 查询</label>
        <label class="check-field"><input id="qqMentionEnabled" type="checkbox"><span>有人 @ 时回复</span></label>
      </div>
      <div><label>机器人 AppID</label><input id="qqAppId" autocomplete="off"></div>
      <div><label>机器人 AppSecret</label><input id="qqAppSecret" type="password" autocomplete="new-password"></div>
      <div><label>推送间隔（分钟）</label><input id="qqInterval" type="number" min="1" max="1440" value="5"></div>
    </div>
    <div class="qq-controls">
      <button class="primary" id="qqBindBtn" type="button">绑定目标群</button>
      <button id="qqCancelBindBtn" type="button" hidden>停止绑定</button>
      <button id="qqTestBtn" type="button">发送测试</button>
      <button class="danger" id="qqUnbindBtn" type="button">解除绑定</button>
    </div>
    <div class="status-line" style="margin-top:12px">
      <span>群：<strong id="qqGroupState">未绑定</strong></span>
      <span>@监听：<strong id="qqMentionState">未启用</strong></span>
      <span id="qqCaptureMessage">尚未开始绑定</span>
      <span>上次@回复：<strong id="qqLastMention">无</strong></span>
      <span>上次推送：<strong id="qqLastPush">无</strong></span>
      <span>下次推送：<strong id="qqNextPush">无</strong></span>
    </div>
  </section>

  <section class="section panel">
    <div class="qq-model-head">
      <div><h2>QQ推送模型</h2><div class="sub"><span id="qqSelectedCount">0</span> 个已选择</div></div>
      <div class="toolbar">
        <input id="qqModelSearch" placeholder="搜索推送模型" autocomplete="off">
        <button id="qqSelectAllBtn" type="button">全选</button>
        <button id="qqClearAllBtn" type="button">清空</button>
      </div>
    </div>
    <div id="qqModelLists"></div>
  </section>

  <section class="section">
    <div class="section-head">
      <h2>分组</h2>
      <button id="addGroupBtn" type="button">新增分组</button>
    </div>
    <div class="table-wrap"><table id="groupTable"></table></div>
  </section>

  <section class="section">
    <div class="section-head">
      <h2>API</h2>
      <button id="addApiBtn" type="button">新增 API</button>
    </div>
    <div class="table-wrap"><table id="apiTable"></table></div>
  </section>

  <section class="section">
    <div class="section-head">
      <h2>模型监控选择</h2>
      <div class="toolbar"><input id="modelSearch" placeholder="搜索模型" autocomplete="off"></div>
    </div>
    <div id="modelLists"></div>
  </section>
</main>

<script>
const state = { config: null, models: [], runtime: null };
let qqPollTimer = null;

function esc(value) {
  return String(value ?? '').replace(/[&<>'"]/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[ch]));
}
function uid(prefix) {
  return `${prefix}_${Math.random().toString(16).slice(2, 10)}${Date.now().toString(16).slice(-4)}`;
}
function notice(text, cls='muted') {
  const el = document.getElementById('notice');
  el.className = `notice ${cls}`;
  el.textContent = text;
}
function groupOptions(selected) {
  return state.config.groups.map(group => `<option value="${esc(group.id)}" ${group.id === selected ? 'selected' : ''}>${esc(group.name)}</option>`).join('');
}
function modelReferenceValue(reference) {
  return reference && reference.endpoint_id && reference.model_id
    ? `${reference.endpoint_id}|${reference.model_id}`
    : '';
}
function parseModelReference(value) {
  const parts = String(value || '').split('|');
  const endpoint_id = parts.shift();
  const model_id = parts.join('|');
  return endpoint_id && model_id ? {endpoint_id, model_id} : null;
}
function defaultModelOptions(group) {
  const current = modelReferenceValue(group.default_model);
  const options = [{value: '', label: '自动选择第一个'}];
  state.models.forEach(api => {
    const endpoint = state.config.endpoints.find(item => item.id === api.endpoint_id);
    if (!endpoint || endpoint.group_id !== group.id) return;
    api.models.filter(model => !model.ignored).forEach(model => {
      options.push({
        value: `${api.endpoint_id}|${model.id}`,
        label: `${api.endpoint_name} / ${model.id}`,
      });
    });
  });
  if (current && !options.some(option => option.value === current)) {
    options.push({value: current, label: `${group.default_model.model_id}（当前配置）`});
  }
  return options.map(option => `<option value="${esc(option.value)}" ${option.value === current ? 'selected' : ''}>${esc(option.label)}</option>`).join('');
}
function qqSettings() {
  if (!state.config.qq_push) {
    state.config.qq_push = {enabled: false, mention_enabled: false, app_id: '', app_secret: '', group_openid: '', interval_minutes: 5, selected_models: []};
  }
  if (!Array.isArray(state.config.qq_push.selected_models)) state.config.qq_push.selected_models = [];
  return state.config.qq_push;
}
function qqModelIsSelected(endpointId, modelId) {
  return qqSettings().selected_models.some(item => item.endpoint_id === endpointId && item.model_id === modelId);
}
function setQQModelSelected(endpointId, modelId, selected) {
  const qq = qqSettings();
  qq.selected_models = qq.selected_models.filter(item => !(item.endpoint_id === endpointId && item.model_id === modelId));
  if (selected) qq.selected_models.push({endpoint_id: endpointId, model_id: modelId});
  const api = state.models.find(item => item.endpoint_id === endpointId);
  const model = api && api.models.find(item => item.id === modelId);
  if (model) model.qq_selected = selected;
}
function syncGlobalsFromDom() {
  state.config.check_interval = Number(document.getElementById('checkInterval').value || 60);
  state.config.max_workers = Number(document.getElementById('maxWorkers').value || 16);
  state.config.history_retention_hours = Number(document.getElementById('retention').value || 72);
}
function syncQQFromDom() {
  const qq = qqSettings();
  qq.enabled = document.getElementById('qqEnabled').checked;
  qq.mention_enabled = document.getElementById('qqMentionEnabled').checked;
  qq.app_id = document.getElementById('qqAppId').value.trim();
  const secret = document.getElementById('qqAppSecret').value.trim();
  if (secret) qq.app_secret = secret;
  qq.interval_minutes = Number(document.getElementById('qqInterval').value || 5);
}
function renderGlobals() {
  document.getElementById('checkInterval').value = state.config.check_interval;
  document.getElementById('maxWorkers').value = state.config.max_workers;
  document.getElementById('retention').value = state.config.history_retention_hours;
  document.getElementById('ignoredCount').value = state.config.ignored_models.length;
  document.getElementById('lastCheck').textContent = state.runtime.last_check_finished_at || '尚未检测';
  document.getElementById('runState').textContent = state.runtime.check_running ? '检测中' : '空闲';
}
function renderQQPush() {
  const qq = qqSettings();
  const runtime = state.runtime.qq_push || {};
  const activeCapture = ['connecting', 'waiting_message'].includes(runtime.capture_status);
  document.getElementById('qqEnabled').checked = Boolean(qq.enabled);
  document.getElementById('qqMentionEnabled').checked = Boolean(qq.mention_enabled);
  document.getElementById('qqAppId').value = qq.app_id || '';
  document.getElementById('qqAppSecret').value = '';
  document.getElementById('qqAppSecret').placeholder = qq.app_secret_set ? '已保存，留空不变' : '未设置';
  document.getElementById('qqInterval').value = qq.interval_minutes || 5;
  document.getElementById('qqGroupState').textContent = qq.group_bound ? '已绑定' : '未绑定';
  document.getElementById('qqMentionState').textContent = runtime.mention_message || '未启用';
  document.getElementById('qqCaptureMessage').textContent = runtime.capture_message || (qq.group_bound ? '目标群已绑定' : '尚未开始绑定');
  document.getElementById('qqLastMention').textContent = runtime.last_mention_at || '无';
  document.getElementById('qqLastPush').textContent = runtime.last_push_at || '无';
  document.getElementById('qqNextPush').textContent = runtime.next_push_at || '无';
  const stateEl = document.getElementById('qqPushState');
  const ready = Boolean(qq.app_id && qq.app_secret_set && qq.group_bound && qq.selected_models.length);
  const modes = [qq.enabled ? '定时' : '', qq.mention_enabled ? '@响应' : ''].filter(Boolean);
  const active = modes.length > 0;
  stateEl.textContent = active ? (ready ? modes.join(' + ') : '配置未完成') : '未启用';
  stateEl.className = `status-pill ${active && ready ? 'green' : active ? 'amber' : 'muted'}`;
  if (runtime.last_push_ok === false && runtime.last_push_error) {
    document.getElementById('qqLastPush').textContent = `${runtime.last_push_at || '失败'} · ${runtime.last_push_error}`;
    document.getElementById('qqLastPush').className = 'red';
  } else {
    document.getElementById('qqLastPush').className = runtime.last_push_ok ? 'green' : '';
  }
  if (runtime.last_mention_ok === false && runtime.last_mention_error) {
    document.getElementById('qqLastMention').textContent = `${runtime.last_mention_at || '失败'} · ${runtime.last_mention_error}`;
    document.getElementById('qqLastMention').className = 'red';
  } else {
    document.getElementById('qqLastMention').className = runtime.last_mention_ok ? 'green' : '';
  }
  document.getElementById('qqBindBtn').disabled = activeCapture;
  document.getElementById('qqCancelBindBtn').hidden = !activeCapture;
  document.getElementById('qqUnbindBtn').disabled = !qq.group_bound;
}

function renderQQModels() {
  const query = document.getElementById('qqModelSearch').value.trim().toLowerCase();
  let visibleCount = 0;
  const blocks = state.models.map(api => {
    const models = api.models.filter(model => !query || model.id.toLowerCase().includes(query));
    visibleCount += models.length;
    const body = models.map(model => {
      const selected = qqModelIsSelected(api.endpoint_id, model.id);
      return `<label class="model-toggle">
        <input type="checkbox" data-qq-endpoint="${esc(api.endpoint_id)}" data-qq-model="${esc(model.id)}" ${selected ? 'checked' : ''} ${model.ignored ? 'disabled' : ''}>
        <span class="mono ${model.ignored ? 'muted' : ''}">${esc(model.id)}${model.ignored ? '（已忽略）' : ''}</span>
      </label>`;
    }).join('');
    return `<div class="qq-model-group">
      <div class="section-head"><h2>${esc(api.endpoint_name)}</h2><span class="muted">${api.models.length} 个模型</span></div>
      <div class="models">${body || '<div class="empty">暂无匹配模型</div>'}</div>
    </div>`;
  }).join('');
  document.getElementById('qqModelLists').innerHTML = visibleCount ? blocks : '<div class="empty">暂无可选模型，请先执行检测</div>';
  document.getElementById('qqSelectedCount').textContent = qqSettings().selected_models.length;
  document.querySelectorAll('[data-qq-endpoint]').forEach(input => input.addEventListener('change', event => {
    setQQModelSelected(event.target.dataset.qqEndpoint, event.target.dataset.qqModel, event.target.checked);
    document.getElementById('qqSelectedCount').textContent = qqSettings().selected_models.length;
  }));
}
function renderGroups() {
  const rows = state.config.groups.map((group, idx) => `
    <tr data-index="${idx}">
      <td><input data-field="name" value="${esc(group.name)}"></td>
      <td><input data-field="description" value="${esc(group.description || '')}"></td>
      <td><select data-field="enabled"><option value="true" ${group.enabled ? 'selected' : ''}>启用</option><option value="false" ${!group.enabled ? 'selected' : ''}>停用</option></select></td>
      <td><input data-field="check_interval" type="number" min="10" max="86400" value="${esc(group.check_interval || state.config.check_interval || 60)}"></td>
      <td><input data-field="timeout" type="number" min="5" max="600" value="${esc(group.timeout || 180)}"></td>
      <td><select data-field="default_model">${defaultModelOptions(group)}</select></td>
      <td class="mono muted">${esc(group.id)}</td>
      <td><div class="row-actions"><button data-action="move-group-up" type="button" ${idx === 0 ? 'disabled' : ''}>上移</button><button data-action="move-group-down" type="button" ${idx === state.config.groups.length - 1 ? 'disabled' : ''}>下移</button><button data-action="clear-group" type="button">清零</button><button class="danger" data-action="delete-group" type="button">删除</button></div></td>
    </tr>`).join('');
  document.getElementById('groupTable').innerHTML = `<thead><tr><th>名称</th><th>备注</th><th>状态</th><th>检测间隔（秒）</th><th>检测超时（秒）</th><th>默认显示模型</th><th>ID</th><th>操作</th></tr></thead><tbody>${rows}</tbody>`;
  document.querySelectorAll('#groupTable input, #groupTable select').forEach(el => el.addEventListener(el.tagName === 'SELECT' ? 'change' : 'input', event => {
    const tr = event.target.closest('tr');
    const group = state.config.groups[Number(tr.dataset.index)];
    const field = event.target.dataset.field;
    if (field === 'enabled') group[field] = event.target.value === 'true';
    else if (field === 'check_interval') group[field] = Number(event.target.value || state.config.check_interval || 60);
    else if (field === 'timeout') group[field] = Number(event.target.value || 180);
    else if (field === 'default_model') group[field] = parseModelReference(event.target.value);
    else group[field] = event.target.value;
    renderApis();
  }));
  document.querySelectorAll('[data-action="move-group-up"], [data-action="move-group-down"]').forEach(btn => btn.addEventListener('click', event => {
    const tr = event.target.closest('tr');
    const idx = Number(tr.dataset.index);
    const direction = event.target.dataset.action === 'move-group-up' ? -1 : 1;
    const nextIdx = idx + direction;
    if (nextIdx < 0 || nextIdx >= state.config.groups.length) return;
    const tmp = state.config.groups[idx];
    state.config.groups[idx] = state.config.groups[nextIdx];
    state.config.groups[nextIdx] = tmp;
    renderAll();
  }));
  document.querySelectorAll('[data-action="clear-group"]').forEach(btn => btn.addEventListener('click', async event => {
    try {
      const idx = Number(event.target.closest('tr').dataset.index);
      const group = state.config.groups[idx];
      if (!group) return;
      if (!confirm(`清零分组「${group.name}」的历史和当前检测结果？`)) return;
      const res = await fetch('/api/admin/clear-group', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({group_id: group.id})
      });
      const payload = await res.json();
      if (!res.ok) throw new Error(payload.error || '清零失败');
      notice(`已清零分组「${group.name}」`, 'green');
      await load();
    } catch (err) {
      notice(err.message, 'red');
    }
  }));
  document.querySelectorAll('[data-action="delete-group"]').forEach(btn => btn.addEventListener('click', event => {
    if (state.config.groups.length <= 1) return notice('至少保留一个分组', 'amber');
    const idx = Number(event.target.closest('tr').dataset.index);
    const removed = state.config.groups[idx];
    state.config.groups.splice(idx, 1);
    const fallback = state.config.groups[0].id;
    state.config.endpoints.forEach(api => { if (api.group_id === removed.id) api.group_id = fallback; });
    renderAll();
  }));
}
function renderApis() {
  const rows = state.config.endpoints.map((api, idx) => `
    <tr data-index="${idx}">
      <td><input data-field="name" value="${esc(api.name)}"></td>
      <td><input data-field="base_url" class="mono" value="${esc(api.base_url)}"></td>
      <td><input data-field="api_key" class="mono" type="password" value="${esc(api.api_key || '')}"></td>
      <td><select data-field="group_id">${groupOptions(api.group_id)}</select></td>
      <td><select data-field="enabled"><option value="true" ${api.enabled ? 'selected' : ''}>启用</option><option value="false" ${!api.enabled ? 'selected' : ''}>停用</option></select></td>
      <td><input data-field="test_prompt" value="${esc(api.test_prompt || 'Hi')}"></td>
      <td><button class="danger" data-action="delete-api" type="button">删除</button></td>
    </tr>`).join('');
  document.getElementById('apiTable').innerHTML = `<thead><tr><th>名称</th><th>Base URL</th><th>API Key</th><th>分组</th><th>状态</th><th>测试提示词</th><th>操作</th></tr></thead><tbody>${rows}</tbody>`;
  document.querySelectorAll('#apiTable input, #apiTable select').forEach(el => el.addEventListener(el.tagName === 'SELECT' ? 'change' : 'input', event => {
    const tr = event.target.closest('tr');
    const api = state.config.endpoints[Number(tr.dataset.index)];
    const field = event.target.dataset.field;
    if (field === 'enabled') api[field] = event.target.value === 'true';
    else if (field === 'group_id') {
      api[field] = event.target.value;
      state.config.groups.forEach(group => {
        if (group.default_model && group.default_model.endpoint_id === api.id && group.id !== api.group_id) {
          group.default_model = null;
        }
      });
      renderGroups();
    } else api[field] = event.target.value;
  }));
  document.querySelectorAll('[data-action="delete-api"]').forEach(btn => btn.addEventListener('click', event => {
    const idx = Number(event.target.closest('tr').dataset.index);
    const removed = state.config.endpoints[idx];
    state.config.endpoints.splice(idx, 1);
    state.config.ignored_models = state.config.ignored_models.filter(item => item.endpoint_id !== removed.id);
    qqSettings().selected_models = qqSettings().selected_models.filter(item => item.endpoint_id !== removed.id);
    state.config.groups.forEach(group => {
      if (group.default_model && group.default_model.endpoint_id === removed.id) group.default_model = null;
    });
    state.models = state.models.filter(item => item.endpoint_id !== removed.id);
    renderAll();
  }));
}
function renderModels() {
  const q = document.getElementById('modelSearch').value.trim().toLowerCase();
  const html = state.models.map(api => {
    const models = api.models.filter(model => !q || model.id.toLowerCase().includes(q));
    const body = models.map(model => `
      <label class="model-toggle">
        <input type="checkbox" data-endpoint="${esc(api.endpoint_id)}" data-model="${esc(model.id)}" ${model.ignored ? 'checked' : ''}>
        <span class="mono ${model.ignored ? 'muted' : ''}">${esc(model.id)}</span>
      </label>`).join('');
    return `<div class="section panel">
      <div class="section-head"><h2>${esc(api.endpoint_name)}</h2><span class="muted">${api.models.length} 个模型</span></div>
      ${api.fetch_error ? `<div class="red">拉取模型失败：${esc(api.fetch_error)}</div>` : ''}
      <div class="models">${body || '<div class="empty">暂无模型，请先保存配置并执行检测</div>'}</div>
    </div>`;
  }).join('');
  document.getElementById('modelLists').innerHTML = html || '<div class="empty">暂无 API</div>';
  document.querySelectorAll('#modelLists input[type="checkbox"]').forEach(el => el.addEventListener('change', async event => {
    const endpoint_id = event.target.dataset.endpoint;
    const model_id = event.target.dataset.model;
    const ignored = event.target.checked;
    await fetch('/api/admin/ignore', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({endpoint_id, model_id, ignored})
    });
    const item = state.config.ignored_models.find(row => row.endpoint_id === endpoint_id && row.model_id === model_id);
    if (ignored && !item) state.config.ignored_models.push({endpoint_id, model_id});
    if (!ignored) state.config.ignored_models = state.config.ignored_models.filter(row => !(row.endpoint_id === endpoint_id && row.model_id === model_id));
    if (ignored) setQQModelSelected(endpoint_id, model_id, false);
    if (ignored) {
      state.config.groups.forEach(group => {
        if (group.default_model && group.default_model.endpoint_id === endpoint_id && group.default_model.model_id === model_id) {
          group.default_model = null;
        }
      });
      renderGroups();
    }
    const api = state.models.find(row => row.endpoint_id === endpoint_id);
    const model = api && api.models.find(row => row.id === model_id);
    if (model) model.ignored = ignored;
    renderGlobals();
    renderQQModels();
    notice(ignored ? '已忽略该模型' : '已恢复监控该模型', 'green');
  }));
}
function renderAll() {
  renderGlobals();
  renderQQPush();
  renderQQModels();
  renderGroups();
  renderApis();
  renderModels();
}
async function load() {
  const res = await fetch('/api/admin/config', {cache:'no-store'});
  if (!res.ok) throw new Error(await res.text());
  const payload = await res.json();
  state.config = payload.config;
  state.models = payload.models;
  state.runtime = payload.runtime;
  renderAll();
}
async function save() {
  syncGlobalsFromDom();
  syncQQFromDom();
  const res = await fetch('/api/admin/config', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({config: state.config})
  });
  const payload = await res.json();
  if (!res.ok) throw new Error(payload.error || '保存失败');
  state.config = payload.config;
  notice('配置已保存', 'green');
  renderAll();
  return payload;
}
async function runNow() {
  const res = await fetch('/api/admin/run', {method:'POST'});
  const payload = await res.json();
  if (!res.ok) throw new Error(payload.error || '启动失败');
  notice(payload.started ? '已开始检测' : '已有检测任务在运行', payload.started ? 'green' : 'amber');
  setTimeout(load, 1200);
}
async function refreshQQStatus() {
  const res = await fetch('/api/admin/qq/status', {cache: 'no-store'});
  const payload = await res.json();
  if (!res.ok) throw new Error(payload.error || '读取 QQ 状态失败');
  state.config.qq_push = payload.qq_push;
  state.runtime.qq_push = payload.runtime;
  renderQQPush();
  const active = ['connecting', 'waiting_message'].includes(payload.runtime.capture_status);
  if (!active && qqPollTimer) {
    clearInterval(qqPollTimer);
    qqPollTimer = null;
    await load();
  }
}
function startQQPolling() {
  if (qqPollTimer) clearInterval(qqPollTimer);
  qqPollTimer = setInterval(() => refreshQQStatus().catch(err => notice(err.message, 'red')), 2000);
}
async function startQQCapture() {
  await save();
  const res = await fetch('/api/admin/qq/capture', {method: 'POST'});
  const payload = await res.json();
  if (!res.ok) throw new Error(payload.error || '启动绑定失败');
  state.runtime.qq_push = payload.runtime;
  renderQQPush();
  notice(payload.started ? '绑定监听已启动' : '绑定监听正在运行', payload.started ? 'green' : 'amber');
  startQQPolling();
}
async function cancelQQCapture() {
  const res = await fetch('/api/admin/qq/cancel-capture', {method: 'POST'});
  const payload = await res.json();
  if (!res.ok) throw new Error(payload.error || '停止绑定失败');
  notice('正在停止绑定监听', 'muted');
  startQQPolling();
}
async function unbindQQGroup() {
  if (!confirm('解除当前 QQ 目标群绑定？')) return;
  const res = await fetch('/api/admin/qq/unbind', {method: 'POST'});
  const payload = await res.json();
  if (!res.ok) throw new Error(payload.error || '解除绑定失败');
  notice('目标群绑定已解除', 'green');
  await load();
}
async function testQQPush() {
  await save();
  notice('正在发送 QQ 测试消息', 'muted');
  const res = await fetch('/api/admin/qq/test', {method: 'POST'});
  const payload = await res.json();
  if (!res.ok) throw new Error(payload.error || '测试推送失败');
  state.runtime.qq_push = payload.runtime;
  renderQQPush();
  notice('QQ 测试消息已发送', 'green');
}
document.getElementById('saveBtn').addEventListener('click', () => save().catch(err => notice(err.message, 'red')));
document.getElementById('runBtn').addEventListener('click', () => runNow().catch(err => notice(err.message, 'red')));
document.getElementById('qqBindBtn').addEventListener('click', () => startQQCapture().catch(err => notice(err.message, 'red')));
document.getElementById('qqCancelBindBtn').addEventListener('click', () => cancelQQCapture().catch(err => notice(err.message, 'red')));
document.getElementById('qqUnbindBtn').addEventListener('click', () => unbindQQGroup().catch(err => notice(err.message, 'red')));
document.getElementById('qqTestBtn').addEventListener('click', () => testQQPush().catch(err => notice(err.message, 'red')));
document.getElementById('qqSelectAllBtn').addEventListener('click', () => {
  state.models.forEach(api => api.models.filter(model => !model.ignored).forEach(model => setQQModelSelected(api.endpoint_id, model.id, true)));
  renderQQModels();
});
document.getElementById('qqClearAllBtn').addEventListener('click', () => {
  qqSettings().selected_models = [];
  state.models.forEach(api => api.models.forEach(model => { model.qq_selected = false; }));
  renderQQModels();
});
document.getElementById('addGroupBtn').addEventListener('click', () => {
  state.config.groups.push({id: uid('grp'), name: '新分组', description: '', enabled: true, check_interval: state.config.check_interval || 60, timeout: 180, default_model: null});
  renderAll();
});
document.getElementById('addApiBtn').addEventListener('click', () => {
  const group_id = state.config.groups[0] ? state.config.groups[0].id : uid('grp');
  if (!state.config.groups.length) state.config.groups.push({id: group_id, name: '默认分组', description: '', enabled: true, check_interval: state.config.check_interval || 60, timeout: 180});
  state.config.endpoints.push({id: uid('api'), name: '新 API', base_url: 'http://host.docker.internal:8080', api_key: '', group_id, enabled: true, test_prompt: 'Hi', max_tokens: 16});
  renderAll();
});
document.getElementById('modelSearch').addEventListener('input', renderModels);
document.getElementById('qqModelSearch').addEventListener('input', renderQQModels);
['checkInterval','maxWorkers','retention'].forEach(id => document.getElementById(id).addEventListener('input', syncGlobalsFromDom));
['qqEnabled','qqMentionEnabled','qqAppId','qqAppSecret','qqInterval'].forEach(id => document.getElementById(id).addEventListener('input', syncQQFromDom));
load().catch(err => notice(err.message, 'red'));
</script>
</body>
</html>
"""


# ---------------------------------------------------------------------------
# Entrypoint


def main():
    load_config()
    init_metric_state()
    init_db()
    thread = threading.Thread(target=checker_loop, daemon=True)
    thread.start()
    qq_thread = threading.Thread(target=qq_push_loop, name="qq-push", daemon=True)
    qq_thread.start()
    qq_mention_thread = threading.Thread(
        target=qq_mention_loop,
        name="qq-mention",
        daemon=True,
    )
    qq_mention_thread.start()
    server = ReusableHTTPServer((LISTEN_HOST, LISTEN_PORT), MonitorHandler)
    print(f"[INFO] Monitor listening on http://{LISTEN_HOST}:{LISTEN_PORT}", flush=True)
    print(f"[INFO] Data dir: {DATA_DIR}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down.", flush=True)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
