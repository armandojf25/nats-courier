from __future__ import annotations

import base64
import json
import logging
from typing import Any

import httpx

log = logging.getLogger(__name__)


async def forward(
    client: httpx.AsyncClient,
    url: str,
    method: str,
    payload: dict[str, Any],
    timeout: float,
) -> bytes:
    """Send *payload* to *url* and return a JSON-encoded response envelope."""
    try:
        if "body_b64" in payload:
            resp = await _send_multipart(client, url, payload, timeout)
        else:
            resp = await _send_json(client, url, method, payload, timeout)
        return _pack(resp.status_code, resp)
    except httpx.TimeoutException:
        log.warning("timeout calling %s", url)
        return _pack_error(504, "upstream timeout")
    except httpx.ConnectError as exc:
        log.error("cannot reach %s: %s", url, exc)
        return _pack_error(502, "upstream unreachable")


async def _send_json(
    client: httpx.AsyncClient,
    url: str,
    method: str,
    payload: dict[str, Any],
    timeout: float,
) -> httpx.Response:
    body = payload.get("body", {})
    return await client.request(method, url, json=body, timeout=timeout)


async def _send_multipart(
    client: httpx.AsyncClient,
    url: str,
    payload: dict[str, Any],
    timeout: float,
) -> httpx.Response:
    raw = base64.b64decode(payload["body_b64"])
    name = payload.get("file_name", "upload")
    field = payload.get("file_field", "file")
    content_type = payload.get("file_content_type", "application/octet-stream")
    return await client.post(
        url, files={field: (name, raw, content_type)}, timeout=timeout,
    )


def _pack(status: int, resp: httpx.Response) -> bytes:
    try:
        body = resp.json()
    except Exception:
        body = resp.text
    return json.dumps({"status": status, "body": body}).encode()


def _pack_error(status: int, msg: str) -> bytes:
    return json.dumps({"status": status, "error": msg}).encode()
