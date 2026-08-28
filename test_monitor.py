import unittest
from unittest import mock

import monitor


class QQPushConfigTests(unittest.TestCase):
    def test_legacy_endpoint_timeout_migrates_to_group(self):
        raw = monitor.build_default_config()
        raw["groups"][0].pop("timeout")
        raw["endpoints"][0]["timeout"] = 77

        normalized = monitor.normalize_config(raw)

        self.assertEqual(normalized["groups"][0]["timeout"], 77)
        self.assertNotIn("timeout", normalized["endpoints"][0])

    def test_normalize_qq_push_config(self):
        raw = monitor.build_default_config()
        endpoint_id = raw["endpoints"][0]["id"]
        raw["qq_push"] = {
            "enabled": True,
            "mention_enabled": True,
            "app_id": " app-id ",
            "app_secret": " secret ",
            "group_openid": " group-id ",
            "interval_minutes": 0,
            "selected_models": [
                {"endpoint_id": endpoint_id, "model_id": "model-a"},
                {"endpoint_id": endpoint_id, "model_id": "model-a"},
                {"endpoint_id": "missing", "model_id": "model-b"},
            ],
        }

        normalized = monitor.normalize_config(raw)

        self.assertEqual(normalized["version"], 3)
        self.assertEqual(normalized["qq_push"]["interval_minutes"], 1)
        self.assertTrue(normalized["qq_push"]["mention_enabled"])
        self.assertEqual(normalized["qq_push"]["app_id"], "app-id")
        self.assertEqual(
            normalized["qq_push"]["selected_models"],
            [{"endpoint_id": endpoint_id, "model_id": "model-a"}],
        )

    def test_admin_view_redacts_sensitive_qq_fields(self):
        config = monitor.normalize_config(monitor.build_default_config())
        config["qq_push"]["app_secret"] = "do-not-return"
        config["qq_push"]["group_openid"] = "do-not-return-group"

        public = monitor.admin_config_view(config)

        self.assertEqual(public["qq_push"]["app_secret"], "")
        self.assertEqual(public["qq_push"]["group_openid"], "")
        self.assertTrue(public["qq_push"]["app_secret_set"])
        self.assertTrue(public["qq_push"]["group_bound"])

    def test_admin_save_preserves_blank_sensitive_fields(self):
        existing = monitor.normalize_config(monitor.build_default_config())
        existing["qq_push"]["app_secret"] = "existing-secret"
        existing["qq_push"]["group_openid"] = "existing-group"
        incoming = monitor.admin_config_view(existing)

        with mock.patch.object(monitor, "get_config_snapshot", return_value=existing), mock.patch.object(
            monitor, "save_config", side_effect=lambda value: value
        ) as save:
            monitor.save_admin_config(incoming)

        saved = save.call_args.args[0]
        self.assertEqual(saved["qq_push"]["app_secret"], "existing-secret")
        self.assertEqual(saved["qq_push"]["group_openid"], "existing-group")


class GroupDefaultModelTests(unittest.TestCase):
    def test_default_model_is_normalized_and_kept_for_matching_endpoint(self):
        raw = monitor.build_default_config()
        endpoint = raw["endpoints"][0]
        raw["groups"][0]["default_model"] = {
            "endpoint_id": endpoint["id"],
            "model_id": "model-a",
        }

        normalized = monitor.normalize_config(raw)

        self.assertEqual(
            normalized["groups"][0]["default_model"],
            {"endpoint_id": endpoint["id"], "model_id": "model-a"},
        )

    def test_default_model_is_cleared_when_endpoint_moves_or_model_is_ignored(self):
        raw = monitor.build_default_config()
        endpoint = raw["endpoints"][0]
        raw["groups"][0]["default_model"] = {
            "endpoint_id": endpoint["id"],
            "model_id": "model-a",
        }
        raw["groups"].append(
            {
                "id": "other-group",
                "name": "其他分组",
                "description": "",
                "enabled": True,
                "check_interval": 60,
                "timeout": 60,
            }
        )
        endpoint["group_id"] = "other-group"

        normalized = monitor.normalize_config(raw)

        self.assertIsNone(normalized["groups"][0]["default_model"])

        raw = monitor.build_default_config()
        endpoint = raw["endpoints"][0]
        raw["groups"][0]["default_model"] = {
            "endpoint_id": endpoint["id"],
            "model_id": "model-a",
        }
        raw["ignored_models"] = [{"endpoint_id": endpoint["id"], "model_id": "model-a"}]
        normalized = monitor.normalize_config(raw)

        self.assertIsNone(normalized["groups"][0]["default_model"])


class QQPushMessageTests(unittest.TestCase):
    def test_message_contains_only_selected_models(self):
        config = monitor.normalize_config(monitor.build_default_config())
        endpoint = config["endpoints"][0]
        config["qq_push"]["selected_models"] = [
            {"endpoint_id": endpoint["id"], "model_id": "selected-ok"},
            {"endpoint_id": endpoint["id"], "model_id": "selected-wave"},
            {"endpoint_id": endpoint["id"], "model_id": "selected-error"},
        ]
        records = [
            {
                "endpoint_id": endpoint["id"],
                "model": "selected-ok",
                "status": "ok",
                "ttft_ms": 321.4,
            },
            {
                "endpoint_id": endpoint["id"],
                "model": "selected-wave",
                "status": "fluctuation",
                "ttft_ms": 23808.9,
            },
            {
                "endpoint_id": endpoint["id"],
                "model": "selected-error",
                "status": "error",
                "error": "HTTP 502",
            },
            {
                "endpoint_id": endpoint["id"],
                "model": "not-selected",
                "status": "error",
                "error": "must not appear",
            },
        ]

        message = monitor.build_qq_status_message(config, records=records, test=True)

        self.assertIn("selected-ok", message)
        self.assertIn("[波动] sub2api / selected-wave - 延迟 23.8秒", message)
        self.assertIn("selected-error", message)
        self.assertIn("正常 1 | 波动 1 | 超时 0 | 异常 1 | 等待 0", message)
        self.assertNotIn("not-selected", message)
        self.assertNotIn("must not appear", message)

    def test_message_requires_a_selected_model(self):
        config = monitor.normalize_config(monitor.build_default_config())
        with self.assertRaisesRegex(ValueError, "至少选择一个"):
            monitor.build_qq_status_message(config, records=[])


class QQMentionTests(unittest.TestCase):
    def setUp(self):
        self.config = monitor.normalize_config(monitor.build_default_config())
        self.config["qq_push"]["mention_enabled"] = True
        self.config["qq_push"]["group_openid"] = "bound-group"
        with monitor.qq_mention_dedupe_lock:
            monitor.qq_mention_seen.clear()

    def tearDown(self):
        with monitor.qq_mention_dedupe_lock:
            monitor.qq_mention_seen.clear()

    def test_only_accepts_mentions_from_bound_group(self):
        settings = self.config["qq_push"]

        target = monitor.qq_mention_target(
            settings,
            "GROUP_AT_MESSAGE_CREATE",
            {"id": "message-1", "group_openid": "bound-group"},
        )
        other_group = monitor.qq_mention_target(
            settings,
            "GROUP_AT_MESSAGE_CREATE",
            {"id": "message-2", "group_openid": "other-group"},
        )
        ordinary_message = monitor.qq_mention_target(
            settings,
            "GROUP_MESSAGE_CREATE",
            {"id": "message-3", "group_openid": "bound-group"},
        )

        self.assertEqual(target, ("bound-group", "message-1"))
        self.assertIsNone(other_group)
        self.assertIsNone(ordinary_message)

    def test_reply_includes_trigger_message_id(self):
        self.config["qq_push"]["app_id"] = "app-id"
        self.config["qq_push"]["app_secret"] = "app-secret"
        with mock.patch.object(monitor, "get_qq_access_token", return_value="token"), mock.patch.object(
            monitor, "qq_https_json", return_value=(200, {"id": "reply-id"})
        ) as request:
            monitor.qq_send_text(self.config, "status", reply_to="message-1")

        payload = request.call_args.args[3]
        self.assertEqual(payload["msg_id"], "message-1")
        self.assertEqual(payload["msg_seq"], 1)

    def test_duplicate_message_id_can_only_be_claimed_once(self):
        first = monitor.claim_qq_mention("bound-group", "message-1", current_time=1000)
        duplicate = monitor.claim_qq_mention("bound-group", "message-1", current_time=1001)
        other_message = monitor.claim_qq_mention("bound-group", "message-2", current_time=1001)

        self.assertTrue(first)
        self.assertFalse(duplicate)
        self.assertTrue(other_message)

    def test_message_id_can_be_claimed_after_dedup_window(self):
        first = monitor.claim_qq_mention("bound-group", "message-1", current_time=1000)
        after_expiry = monitor.claim_qq_mention(
            "bound-group",
            "message-1",
            current_time=1000 + monitor.QQ_MENTION_DEDUP_SECONDS + 1,
        )

        self.assertTrue(first)
        self.assertTrue(after_expiry)


class QQTokenTests(unittest.TestCase):
    def tearDown(self):
        monitor.invalidate_qq_access_token()

    def test_access_token_is_cached(self):
        response = {"access_token": "token-value", "expires_in": 7200}
        with mock.patch.object(monitor, "qq_https_json", return_value=(200, response)) as request:
            first = monitor.get_qq_access_token("app", "secret")
            second = monitor.get_qq_access_token("app", "secret")

        self.assertEqual(first, "token-value")
        self.assertEqual(second, "token-value")
        request.assert_called_once()


class ModelCheckRetryTests(unittest.TestCase):
    def setUp(self):
        config = monitor.normalize_config(monitor.build_default_config())
        self.endpoint = config["endpoints"][0]
        self.group = config["groups"][0]

    def test_retry_then_success_within_ten_seconds_is_ok(self):
        attempts = [
            (False, True, 100.0, "HTTP 524"),
            (True, False, 700.0, None),
        ]
        with mock.patch.object(monitor, "check_model_attempt", side_effect=attempts) as check, mock.patch.object(
            monitor,
            "check_model_responses",
            return_value=(False, False, 100.0, "Responses API unavailable"),
        ), mock.patch.object(
            monitor.time, "monotonic", side_effect=[100.0, 100.0, 100.1, 100.6, 101.1]
        ), mock.patch.object(monitor.time, "sleep") as sleep:
            record = monitor.check_model(self.endpoint, self.group, "model-a")

        self.assertEqual(record["status"], "ok")
        self.assertEqual(record["ttft_ms"], 700.0)
        self.assertIsNone(record["error"])
        self.assertEqual(check.call_count, 2)
        sleep.assert_called_once_with(0.5)

    def test_repeated_http_502_is_error_without_group_timeout(self):
        attempts = [
            (False, True, 100.0, "HTTP 502: upstream unavailable"),
            (False, True, 650.0, "HTTP 502: upstream unavailable"),
            (False, True, 1750.0, "HTTP 502: upstream unavailable"),
        ]
        with mock.patch.object(monitor, "check_model_attempt", side_effect=attempts) as check, mock.patch.object(
            monitor,
            "check_model_responses",
            return_value=(False, False, 100.0, "Responses API unavailable"),
        ), mock.patch.object(
            monitor.time,
            "monotonic",
            side_effect=[100.0, 100.0, 100.1, 100.6, 100.7, 101.7, 102.7],
        ), mock.patch.object(monitor.time, "sleep") as sleep:
            record = monitor.check_model(self.endpoint, self.group, "model-a")

        self.assertEqual(record["status"], "error")
        self.assertIn("HTTP 502", record["error"])
        self.assertEqual(check.call_count, monitor.MAX_HTTP_RETRY_ATTEMPTS)
        self.assertEqual(sleep.call_args_list, [mock.call(0.5), mock.call(1.0)])

    def test_responses_fallback_can_recover_chat_http_failure(self):
        with mock.patch.object(
            monitor,
            "check_model_attempt",
            return_value=(False, True, 100.0, "HTTP 502: upstream unavailable"),
        ), mock.patch.object(
            monitor,
            "check_model_responses",
            return_value=(True, False, 900.0, None),
        ) as responses, mock.patch.object(
            monitor.time, "monotonic", side_effect=[100.0, 100.0, 100.1]
        ):
            record = monitor.check_model(self.endpoint, self.group, "gpt-5.6-sol")

        self.assertEqual(record["status"], "ok")
        self.assertEqual(record["ttft_ms"], 900.0)
        self.assertEqual(record["probe_protocol"], "responses")
        responses.assert_called_once()

    def test_responses_stream_output_event_is_detected(self):
        self.assertTrue(
            monitor.responses_stream_event_present(
                "response.output_item.added",
                {"type": "response.output_item.added", "item": {"type": "message"}},
            )
        )
        self.assertFalse(
            monitor.responses_stream_event_present(
                "response.created", {"type": "response.created"}
            )
        )

    def test_socket_timeout_still_uses_timeout_status(self):
        self.group["timeout"] = 5
        attempts = [
            (False, True, 100.0, "The read operation timed out"),
            (False, True, 5000.0, "The read operation timed out"),
        ]
        with mock.patch.object(monitor, "check_model_attempt", side_effect=attempts) as check, mock.patch.object(
            monitor.time,
            "monotonic",
            side_effect=[100.0, 100.0, 100.1, 100.6, 105.0],
        ), mock.patch.object(monitor.time, "sleep") as sleep:
            record = monitor.check_model(self.endpoint, self.group, "model-a")

        self.assertEqual(record["status"], "timeout")
        self.assertIn("检测超时：5秒内", record["error"])
        self.assertEqual(check.call_count, 2)
        sleep.assert_called_once_with(0.5)

    def test_http_failure_at_deadline_is_still_an_error(self):
        self.group["timeout"] = 5
        with mock.patch.object(
            monitor,
            "check_model_attempt",
            return_value=(False, True, 5000.0, "HTTP 502: upstream unavailable"),
        ), mock.patch.object(
            monitor,
            "check_model_responses",
            return_value=(False, False, 5000.0, "Responses API unavailable"),
        ), mock.patch.object(monitor.time, "monotonic", side_effect=[100.0, 100.0, 100.1, 105.0]):
            record = monitor.check_model(self.endpoint, self.group, "model-a")

        self.assertEqual(record["status"], "error")
        self.assertIn("HTTP 502", record["error"])

    def test_success_after_ten_seconds_is_fluctuation(self):
        with mock.patch.object(
            monitor,
            "check_model_attempt",
            return_value=(True, False, 10500.0, None),
        ), mock.patch.object(monitor.time, "monotonic", side_effect=[100.0, 100.0]):
            record = monitor.check_model(self.endpoint, self.group, "model-a")

        self.assertEqual(record["status"], "fluctuation")
        self.assertEqual(record["ttft_ms"], 10500.0)
        self.assertIn("10.5秒后恢复", record["error"])

    def test_retry_until_group_timeout(self):
        self.group["timeout"] = 5
        attempts = [
            (False, True, 100.0, "The read operation timed out"),
            (False, True, 5000.0, "The read operation timed out"),
        ]
        with mock.patch.object(monitor, "check_model_attempt", side_effect=attempts) as check, mock.patch.object(
            monitor.time,
            "monotonic",
            side_effect=[100.0, 100.0, 100.1, 100.6, 105.0],
        ), mock.patch.object(monitor.time, "sleep") as sleep:
            record = monitor.check_model(self.endpoint, self.group, "model-a")

        self.assertEqual(record["status"], "timeout")
        self.assertEqual(record["ttft_ms"], 5000.0)
        self.assertIn("检测超时：5秒内重试 1 次仍不可用", record["error"])
        self.assertEqual(check.call_count, 2)
        sleep.assert_called_once_with(0.5)


if __name__ == "__main__":
    unittest.main()
