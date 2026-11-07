import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from nats_courier.config import CourierConfig, Route
from nats_courier.bridge import Courier


def test_courier_init():
    cfg = CourierConfig(
        nats_url="nats://test:4222",
        routes=[Route(subject="events.>", url="http://svc:3000/hook")],
    )
    c = Courier(cfg)
    assert c._cfg is cfg
    assert c._nc is None


def test_extract_reply_none():
    """With no headers, reply should be None."""
    cfg = CourierConfig()
    c = Courier(cfg)

    class FakeMsg:
        headers = None

    assert c._extract_reply(FakeMsg()) is None


def test_extract_reply_present():
    cfg = CourierConfig()
    c = Courier(cfg)

    class FakeMsg:
        headers = {"X-Reply-To": "_INBOX.abc123"}

    assert c._extract_reply(FakeMsg()) == "_INBOX.abc123"


def test_extract_reply_custom_header():
    cfg = CourierConfig(reply_header="X-Custom-Reply")
    c = Courier(cfg)

    class FakeMsg:
        headers = {"X-Custom-Reply": "_INBOX.xyz"}

    assert c._extract_reply(FakeMsg()) == "_INBOX.xyz"


@pytest.mark.asyncio
async def test_handler_forwards_and_replies():
    """Full happy path: message arrives, gets forwarded, reply published."""
    import httpx

    route = Route(subject="rpc.echo", url="http://echo:9000/do", timeout=5)
    cfg = CourierConfig(routes=[route])
    c = Courier(cfg)

    c._nc = MagicMock()
    c._nc.publish = AsyncMock()

    def ok_handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    c._http = httpx.AsyncClient(transport=httpx.MockTransport(ok_handler))

    handler = c._make_handler(route)

    msg = MagicMock()
    msg.headers = {"X-Reply-To": "_INBOX.test99"}
    msg.data = json.dumps({"body": {"ping": 1}}).encode()
    msg.ack = AsyncMock()

    await handler(msg)

    msg.ack.assert_awaited_once()
    c._nc.publish.assert_awaited_once()
    reply_subject, reply_data = c._nc.publish.await_args.args
    assert reply_subject == "_INBOX.test99"
    parsed = json.loads(reply_data)
    assert parsed["status"] == 200
    await c._http.aclose()


@pytest.mark.asyncio
async def test_handler_bad_json_still_acks():
    route = Route(subject="rpc.x", url="http://x:9000/y")
    cfg = CourierConfig(routes=[route])
    c = Courier(cfg)
    c._nc = MagicMock()
    c._nc.publish = AsyncMock()

    handler = c._make_handler(route)

    msg = MagicMock()
    msg.headers = {"X-Reply-To": "_INBOX.err"}
    msg.data = b"not json at all {{{"
    msg.ack = AsyncMock()

    await handler(msg)

    # should ack even on bad input, otherwise the message retries forever
    msg.ack.assert_awaited_once()
    # should still reply with a 400
    c._nc.publish.assert_awaited_once()
    _, err_data = c._nc.publish.await_args.args
    assert json.loads(err_data)["status"] == 400
