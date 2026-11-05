# nats-courier

Forward NATS JetStream messages to HTTP services, then send the HTTP response back through NATS.

```python
from nats_courier import Courier, CourierConfig, Route

config = CourierConfig(
    nats_url="nats://localhost:4222",
    stream="ML_REQUESTS",
    routes=[
        Route(subject="ml.sleep.predict", url="http://predictor:8000/predict", timeout=120),
        Route(subject="ml.sleep.health", url="http://predictor:8000/health", method="GET", timeout=5),
    ],
)

courier = Courier(config)
await courier.run()
```

Or from the command line:

```bash
nats-courier --nats-url nats://localhost:4222 \
             --subject ml.sleep.predict \
             --target http://predictor:8000/predict \
             --stream ML_REQUESTS \
             --timeout 120
```

## Why

NATS JetStream gives you persistent, exactly-once delivery. HTTP gives you a predictable request/response interface. This tool connects them: JetStream handles the queuing and retry, your service stays a plain HTTP server.

I kept writing the same "subscribe to NATS, POST to HTTP, reply" glue for different services, so I packaged it.

## Install

```bash
pip install nats-courier
```

Needs Python 3.10+ and a running NATS server with JetStream enabled.

## How it works

1. Subscribes to JetStream subjects as durable consumers
2. On each message, reads an `X-Reply-To` header containing a reply inbox
3. Forwards the payload to your HTTP service (JSON body or multipart via base64)
4. Publishes the HTTP response back to the reply inbox

The publisher side creates an inbox, subscribes, publishes the message with the inbox in `X-Reply-To`, and waits. This gives you request/reply semantics over JetStream -- something NATS doesn't do natively.

## Configuration

You can configure via constructor, environment variables, or CLI args.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `NATS_URL` | `nats://localhost:4222` | NATS server URL |
| `SERVICE_NAME` | `courier` | Used in consumer naming |
| `STREAM` | (none) | JetStream stream name |
| `SUBJECT` | (none) | Subject to subscribe to |
| `TARGET_URL` | (none) | HTTP URL to forward to |
| `METHOD` | `POST` | HTTP method |
| `TIMEOUT` | `30` | HTTP timeout in seconds |
| `REPLY_HEADER` | `X-Reply-To` | Header containing reply inbox |

### Multipart (binary) messages

If the message payload contains `body_b64`, the courier sends a multipart POST instead of JSON:

```json
{
  "body_b64": "<base64 encoded bytes>",
  "file_name": "audio.wav",
  "file_field": "audio",
  "file_content_type": "audio/wav"
}
```

Useful for forwarding audio, images, or other binary data to ML services.

## Docker

```dockerfile
FROM python:3.13-slim
WORKDIR /app
RUN pip install nats-courier
CMD ["nats-courier"]
```

## License

MIT
