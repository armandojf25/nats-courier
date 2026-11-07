import base64
import json

import httpx
import pytest

from nats_courier.http import forward


def _mock_handler(status=200, json_body=None):
    """Return a transport handler that always responds with the given status/body."""
    def handler(request: httpx.Request) -> httpx.Response:
        body = json_body or {}
        return httpx.Response(status, json=body)
    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_forward_json():
    transport = _mock_handler(200, {"prediction": [0.8, 0.2]})
    client = httpx.AsyncClient(transport=transport)
    payload = {"body": {"features": [1, 2, 3]}}

    raw = await forward(client, "http://ml:8000/predict", "POST", payload, 10)
    result = json.loads(raw)

    assert result["status"] == 200
    assert result["body"]["prediction"] == [0.8, 0.2]
    await client.aclose()


@pytest.mark.asyncio
async def test_forward_multipart():
    transport = _mock_handler(200, {"class": "hungry", "confidence": 0.91})
    client = httpx.AsyncClient(transport=transport)
    audio = b"\x00\x01\x02\x03" * 100
    payload = {
        "body_b64": base64.b64encode(audio).decode(),
        "file_name": "cry.wav",
        "file_field": "audio",
        "file_content_type": "audio/wav",
    }

    raw = await forward(client, "http://ml:8000/predict", "POST", payload, 10)
    result = json.loads(raw)

    assert result["status"] == 200
    assert result["body"]["class"] == "hungry"
    await client.aclose()


@pytest.mark.asyncio
async def test_forward_timeout():
    def boom(req: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("took too long")

    client = httpx.AsyncClient(transport=httpx.MockTransport(boom))
    raw = await forward(client, "http://slow:8000/x", "POST", {"body": {}}, 1)
    result = json.loads(raw)

    assert result["status"] == 504
    assert "timeout" in result["error"]
    await client.aclose()


@pytest.mark.asyncio
async def test_forward_connection_refused():
    def refuse(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    client = httpx.AsyncClient(transport=httpx.MockTransport(refuse))
    raw = await forward(client, "http://down:8000/x", "POST", {"body": {"n": 1}}, 1)
    envelope = json.loads(raw)

    assert envelope["status"] == 502
    await client.aclose()
