from __future__ import annotations

import asyncio
import json
import logging
import signal
from typing import Callable

import httpx
import nats
from nats.errors import ConnectionClosedError, NoServersError

from .config import CourierConfig, Route
from .http import forward

log = logging.getLogger(__name__)


class Courier:
    """Subscribes to NATS JetStream subjects and forwards messages to HTTP."""

    def __init__(self, config: CourierConfig) -> None:
        self._cfg = config
        self._nc = None
        self._js = None
        self._http = None
        self._subs: list = []
        self._stop = asyncio.Event()

    async def run(self) -> None:
        cfg = self._cfg
        log.info("connecting to %s as %s", cfg.nats_url, cfg.service_name)

        self._nc = await nats.connect(
            cfg.nats_url,
            name=cfg.service_name,
            max_reconnect_attempts=cfg.max_reconnects,
            reconnect_time_wait=cfg.reconnect_wait,
        )
        self._js = self._nc.jetstream()
        self._http = httpx.AsyncClient()

        if cfg.stream:
            await self._ensure_stream(cfg.stream, cfg.routes)

        for route in cfg.routes:
            await self._subscribe(route)

        log.info(
            "listening on %d route(s)",
            len(cfg.routes),
        )

        self._install_signals()
        await self._stop.wait()
        await self._shutdown()

    # -- internals -----------------------------------------------------------

    async def _ensure_stream(self, name: str, routes: list[Route]) -> None:
        subjects = [r.subject for r in routes]
        try:
            await self._js.find_stream_name_by_subject(subjects[0])
        except Exception:
            # TODO: handle the case where stream exists with different subjects
            try:
                await self._js.add_stream(name=name, subjects=subjects)
                log.info("created stream %s", name)
            except Exception as exc:
                log.debug("stream setup: %s", exc)

    async def _subscribe(self, route: Route) -> None:
        consumer = route.durable or route.consumer_name(self._cfg.service_name)

        # wipe stale consumers from previous runs
        if self._cfg.stream:
            try:
                await self._js.delete_consumer(self._cfg.stream, consumer)
            except Exception:
                pass

        cb = self._make_handler(route)
        sub = await self._js.subscribe(
            route.subject,
            durable=consumer,
            cb=cb,
            stream=self._cfg.stream or None,
            manual_ack=True,
        )
        self._subs.append(sub)
        log.info("subscribed %s -> %s %s", route.subject, route.method, route.url)

    def _make_handler(self, route: Route) -> Callable:
        async def _on_msg(msg):
            reply_to = self._extract_reply(msg)
            try:
                envelope = json.loads(msg.data.decode())
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                log.warning("bad envelope on %s: %s", route.subject, exc)
                if reply_to:
                    err = json.dumps({"status": 400, "error": str(exc)}).encode()
                    await self._nc.publish(reply_to, err)
                await msg.ack()
                return

            resp = await forward(
                self._http, route.url, route.method, envelope, route.timeout,
            )

            if reply_to:
                await self._nc.publish(reply_to, resp)
            await msg.ack()

        return _on_msg

    def _extract_reply(self, msg) -> str | None:
        hdr = self._cfg.reply_header
        if msg.headers and hdr in msg.headers:
            return msg.headers[hdr]
        return None

    def _install_signals(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except NotImplementedError:
                pass  # Windows

    async def _shutdown(self) -> None:
        log.info("shutting down")
        for sub in self._subs:
            await sub.unsubscribe()
        if self._nc:
            await self._nc.drain()
        if self._http:
            await self._http.aclose()
        log.info("stopped")
