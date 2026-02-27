"""Tests for Telegram reporting client."""

from __future__ import annotations

from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from pydantic import SecretStr

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


async def test_send_report_uses_default_chat_id_from_settings():
    settings = SimpleNamespace(
        telegram_chat_id="chat-42",
        telegram_bot_token=SecretStr("token-42"),
    )
    mock_send = AsyncMock(return_value=True)

    with (
        patch("pfm.reporting.telegram.get_settings", return_value=settings),
        patch("pfm.reporting.telegram.send_message", mock_send),
    ):
        ok = await send_report(WeeklyReport(text="report text"))

    assert ok is True
    mock_send.assert_awaited_once_with(
        "chat-42",
        "report text",
        parse_mode="HTML",
        bot_token=None,
        client=None,
    )


async def test_send_error_alert_formats_errors():
    mock_send = AsyncMock(return_value=True)
    with patch("pfm.reporting.telegram.send_message", mock_send):
        ok = await send_error_alert(["foo failed", "bar timeout"], chat_id="chat", bot_token="token")

    assert ok is True
    sent_text = mock_send.await_args.args[1]
    assert "PFM pipeline errors detected:" in sent_text
    assert "- foo failed" in sent_text
    assert "- bar timeout" in sent_text
