import os
from unittest import mock

from nats_courier.config import CourierConfig, Route


def test_route_consumer_name():
    r = Route(subject="ml.sleep.predict", url="http://localhost:8000/predict")
    assert r.consumer_name("my-svc") == "my-svc-ml-sleep-predict"


def test_route_wildcard_consumer_name():
    r = Route(subject="events.>", url="http://localhost:9000/hook")
    name = r.consumer_name("ingest")
    assert "all" in name
    assert ">" not in name


def test_config_defaults():
    cfg = CourierConfig()
    assert cfg.nats_url == "nats://localhost:4222"
    assert cfg.routes == []
    assert cfg.reply_header == "X-Reply-To"


def test_config_from_env():
    env = {
        "NATS_URL": "nats://nats:4222",
        "SERVICE_NAME": "sleep",
        "STREAM": "ML_REQUESTS",
        "SUBJECT": "ml.sleep.predict",
        "TARGET_URL": "http://predictor:8000/predict",
        "METHOD": "POST",
        "TIMEOUT": "120",
    }
    with mock.patch.dict(os.environ, env, clear=False):
        cfg = CourierConfig.from_env()

    assert cfg.nats_url == "nats://nats:4222"
    assert cfg.service_name == "sleep"
    assert len(cfg.routes) == 1
    assert cfg.routes[0].timeout == 120.0


def test_config_from_env_no_route():
    with mock.patch.dict(os.environ, {}, clear=True):
        cfg = CourierConfig.from_env()
    assert cfg.routes == []
