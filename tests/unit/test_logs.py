import json
import logging

from signalops.logs import JsonFormatter


def test_json_formatter_includes_extra_fields() -> None:
    record = logging.makeLogRecord(
        {"name": "signalops.test", "levelname": "INFO", "msg": "hello %s", "args": ("world",)}
    )
    record.dependency = "redis"
    line = json.loads(JsonFormatter().format(record))
    assert line["message"] == "hello world"
    assert line["logger"] == "signalops.test"
    assert line["dependency"] == "redis"
    assert line["ts"].endswith("+00:00")
