from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class Route:
    """Maps a NATS subject to an HTTP endpoint."""

    subject: str
    url: str
    method: str = "POST"
    timeout: float = 30.0
    durable: str = ""

    def consumer_name(self, service: str) -> str:
        slug = self.subject.replace(".", "-").replace(">", "all").replace("*", "any")
        return f"{service}-{slug}" if service else slug


@dataclass
class CourierConfig:
    nats_url: str = "nats://localhost:4222"
    service_name: str = "courier"
    stream: str = ""
    reply_header: str = "X-Reply-To"
    routes: list[Route] = field(default_factory=list)
    max_reconnects: int = 60
    reconnect_wait: float = 2.0

    @classmethod
    def from_env(cls) -> CourierConfig:
        """Minimal config from env vars -- good enough for a single-route setup."""
        cfg = cls(
            nats_url=os.environ.get("NATS_URL", "nats://localhost:4222"),
            service_name=os.environ.get("SERVICE_NAME", "courier"),
            stream=os.environ.get("STREAM", ""),
            reply_header=os.environ.get("REPLY_HEADER", "X-Reply-To"),
        )
        subject = os.environ.get("SUBJECT")
        target = os.environ.get("TARGET_URL")
        if subject and target:
            cfg.routes.append(Route(
                subject=subject,
                url=target,
                method=os.environ.get("METHOD", "POST"),
                timeout=float(os.environ.get("TIMEOUT", "30")),
            ))
        return cfg
