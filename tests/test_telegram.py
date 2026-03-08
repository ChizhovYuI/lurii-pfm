"""Tests for Telegram reporting client."""

from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import AsyncMock, patch

import httpx

from pfm.db.models import init_db
from pfm.db.telegram_store import TelegramStore
from pfm.reporting.telegram import WeeklyReport, send_error_alert, send_message, send_report


@dataclass
class _FakeClient:
    responses: list[httpx.Response] = field(default_factory=list)
    calls: list[tuple[str, dict[str, str]]] = field(default_factory=list)

    async def post(self, endpoint: str, json: dict[str, str]) -> httpx.Response:
        self.calls.append((endpoint, json))
        if self.responses:
            return self.responses.pop(0)
        return httpx.Response(200, json={"ok": True}, request=httpx.Request("POST", endpoint))

    async def aclose(self) -> None:
        return


async def test_send_message_splits_long_payload():
    client = _FakeClient()
    text = ("A" * 4090) + "\n" + ("B" * 50)

    ok = await send_message("chat-1", text, bot_token="token-1", client=client)

    assert ok is True
    assert len(client.calls) == 2
    assert all(len(payload["text"]) <= 4096 for _, payload in client.calls)
    assert client.calls[0][1]["parse_mode"] == "HTML"


async def test_send_message_returns_false_on_api_not_ok():
    endpoint = "https://api.telegram.org/bottoken/sendMessage"
    client = _FakeClient(
        responses=[
            httpx.Response(
                200,
                json={"ok": False, "description": "bad chat"},
                request=httpx.Request("POST", endpoint),
            )
        ]
    )

    ok = await send_message("chat-1", "hello", bot_token="token-1", client=client)
    assert ok is False


async def test_send_message_returns_false_on_http_error():
    endpoint = "https://api.telegram.org/bottoken/sendMessage"
    client = _FakeClient(
        responses=[
            httpx.Response(
                500,
                json={"ok": False},
                request=httpx.Request("POST", endpoint),
            )
        ]
    )

    ok = await send_message("chat-1", "hello", bot_token="token-1", client=client)
    assert ok is False


async def test_send_message_retries_plain_text_on_400_html_error():
    endpoint = "https://api.telegram.org/bottoken/sendMessage"
    client = _FakeClient(
        responses=[
            httpx.Response(
                400,
                json={"ok": False, "description": "Bad Request: can't parse entities"},
                request=httpx.Request("POST", endpoint),
            ),
            httpx.Response(
                200,
                json={"ok": True},
                request=httpx.Request("POST", endpoint),
            ),
        ]
    )

    ok = await send_message("chat-1", "<b>Hello</b><br>World", bot_token="token-1", client=client)

    assert ok is True
    assert len(client.calls) == 2
    assert client.calls[0][1]["parse_mode"] == "HTML"
    assert "parse_mode" not in client.calls[1][1]
    assert client.calls[1][1]["text"] == "Hello\nWorld"


async def test_send_message_returns_false_when_400_persists_after_plain_text_retry():
    endpoint = "https://api.telegram.org/bottoken/sendMessage"
    client = _FakeClient(
        responses=[
            httpx.Response(
                400,
                json={"ok": False, "description": "Bad Request: can't parse entities"},
                request=httpx.Request("POST", endpoint),
            ),
            httpx.Response(
                400,
                json={"ok": False, "description": "Bad Request: chat not found"},
                request=httpx.Request("POST", endpoint),
            ),
        ]
    )

    ok = await send_message("chat-1", "<b>Hello</b>", bot_token="token-1", client=client)
    assert ok is False


async def test_send_message_404_logs_without_token_leak(caplog):
    token = "123456:ABCDEF_SECRET_TOKEN"
    endpoint = f"https://api.telegram.org/bot{token}/sendMessage"
    client = _FakeClient(
        responses=[
            httpx.Response(
                404,
                json={"ok": False},
                request=httpx.Request("POST", endpoint),
            )
        ]
    )

    with patch("pfm.reporting.telegram.logger.warning") as log_warning:
        ok = await send_message("chat-1", "hello", bot_token=token, client=client)
    assert ok is False
    assert log_warning.call_count == 1
    assert "Bot token is likely invalid" in log_warning.call_args.args[0]
    assert token not in str(log_warning.call_args)


async def test_send_message_returns_false_on_invalid_json_body():
    endpoint = "https://api.telegram.org/bottoken/sendMessage"
    client = _FakeClient(
        responses=[
            httpx.Response(
                200,
                content=b"not-json",
                request=httpx.Request("POST", endpoint),
            )
        ]
    )

    ok = await send_message("chat-1", "hello", bot_token="token-1", client=client)
    assert ok is False


async def test_send_report_uses_db_credentials(tmp_path):
    db_path = tmp_path / "telegram.db"
    await init_db(db_path)
    store = TelegramStore(db_path)
    await store.set(bot_token="token-42", chat_id="chat-42")
    mock_send = AsyncMock(return_value=True)

    with patch("pfm.reporting.telegram.send_message", mock_send):
        ok = await send_report(WeeklyReport(text="report text"), db_path=db_path)

    assert ok is True
    mock_send.assert_awaited_once_with(
        None,
        "report text",
        parse_mode="HTML",
        bot_token=None,
        db_path=db_path,
        client=None,
    )


async def test_send_report_sends_ai_summary_separately():
    mock_send = AsyncMock(side_effect=[True, True])
    report = WeeklyReport(text="core report", ai_summary_text="<b>AI Commentary</b>\nGood.")

    with patch("pfm.reporting.telegram.send_message", mock_send):
        ok = await send_report(report, chat_id="chat", bot_token="token")

    assert ok is True
    assert mock_send.await_count == 2
    first_call = mock_send.await_args_list[0]
    second_call = mock_send.await_args_list[1]
    assert first_call.args[1] == "core report"
    assert first_call.kwargs["parse_mode"] == "HTML"
    assert second_call.args[1] == "<b>AI Commentary</b>\nGood."
    assert second_call.kwargs["parse_mode"] == "HTML"


async def test_send_report_returns_true_when_ai_summary_fails(caplog):
    mock_send = AsyncMock(side_effect=[True, False])
    report = WeeklyReport(text="core report", ai_summary_text="<b>AI Commentary</b>\nGood.")

    with (
        patch("pfm.reporting.telegram.send_message", mock_send),
        patch("pfm.reporting.telegram.logger.warning") as log_warning,
    ):
        ok = await send_report(report, chat_id="chat", bot_token="token")

    assert ok is True
    assert log_warning.call_count == 1
    assert "AI summary message failed" in log_warning.call_args.args[0]


async def test_send_error_alert_formats_errors():
    mock_send = AsyncMock(return_value=True)
    with patch("pfm.reporting.telegram.send_message", mock_send):
        ok = await send_error_alert(["foo failed", "bar timeout"], chat_id="chat", bot_token="token")

    assert ok is True
    sent_text = mock_send.await_args.args[1]
    assert "PFM pipeline errors detected:" in sent_text
    assert "- foo failed" in sent_text
    assert "- bar timeout" in sent_text
