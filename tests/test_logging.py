import json
import logging

from fulcrum_mcp.logging_setup import JsonFormatter, configure_logging, redact


def test_redacts_signed_url_parts_and_keys():
    url = "https://kafka.us-east-1.amazonaws.com/?Action=kafka-cluster:Connect&X-Amz-Security-Token=abc123&X-Amz-Signature=deadbeef"
    out = redact(url)
    assert "abc123" not in out and "deadbeef" not in out and "Action=kafka-cluster:Connect" in out
    assert "SECRETVALUE" not in redact("aws_secret_access_key = SECRETVALUE")
    hint = "export AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, or run"
    assert redact(hint) == hint


def test_json_formatter_redacts_sensitive_extras():
    record = logging.LogRecord("x", logging.INFO, __file__, 1, "evt", (), None)
    record.token = "secret-token"
    record.tool = "list_topics"
    payload = json.loads(JsonFormatter().format(record))
    assert payload["token"] == "[REDACTED]" and payload["tool"] == "list_topics" and payload["event"] == "evt"


def test_json_formatter_includes_exceptions():
    try:
        raise ValueError("X-Amz-Signature=abc")
    except ValueError:
        import sys

        record = logging.LogRecord("x", logging.ERROR, __file__, 1, "boom", (), sys.exc_info())
    payload = json.loads(JsonFormatter().format(record))
    assert "ValueError" in payload["exc"] and "abc" not in payload["exc"]


def test_configure_logging_targets_stderr():
    import sys

    configure_logging("debug")
    root = logging.getLogger()
    assert root.level == logging.DEBUG
    assert root.handlers[0].stream is sys.stderr  # stdout is reserved for the MCP protocol
    assert logging.getLogger("botocore").level == logging.WARNING
