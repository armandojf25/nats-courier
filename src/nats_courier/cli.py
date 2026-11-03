from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from .bridge import Courier
from .config import CourierConfig, Route


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="nats-courier",
        description="Forward NATS JetStream messages to an HTTP service",
    )
    parser.add_argument("--nats-url", default=None)
    parser.add_argument("--subject", default=None, help="NATS subject to subscribe to")
    parser.add_argument("--target", default=None, help="HTTP URL to forward to")
    parser.add_argument("--stream", default=None, help="JetStream stream name")
    parser.add_argument("--service", default=None, help="service name for consumer naming")
    parser.add_argument("--method", default="POST")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--log-level", default="info")
    parser.add_argument("--version", action="version", version="%(prog)s 0.1.0")

    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    cfg = CourierConfig.from_env()

    if args.nats_url:
        cfg.nats_url = args.nats_url
    if args.service:
        cfg.service_name = args.service
    if args.stream:
        cfg.stream = args.stream

    # CLI route takes precedence over env
    if args.subject and args.target:
        cfg.routes = [Route(
            subject=args.subject,
            url=args.target,
            method=args.method,
            timeout=args.timeout,
        )]

    if not cfg.routes:
        print("error: no routes. pass --subject/--target or set SUBJECT/TARGET_URL env.", file=sys.stderr)
        raise SystemExit(1)

    courier = Courier(cfg)
    try:
        asyncio.run(courier.run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
