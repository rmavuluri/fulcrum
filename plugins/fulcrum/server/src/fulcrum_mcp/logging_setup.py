"""Structured JSON logging to stderr (stdout carries the MCP protocol)."""

from __future__ import annotations

import json
import logging
import re
import sys
from datetime import UTC, datetime

_STANDARD = set(vars(logging.LogRecord("", 0, "", 0, "", (), None))) | {"message", "asctime"}
_SECRET_KEYS = re.compile(r"(token|secret|password|credential|session|signature|authorization)", re.I)
_SECRET_VALUES = re.compile(
    r"(X-Amz-Signature=|X-Amz-Security-Token=|X-Amz-Credential=|aws_secret_access_key\s*[=:]\s*)[^\s&\"',]+", re.I
)


def redact(text: str) -> str:
    return _SECRET_VALUES.sub(lambda m: m.group(1) + "[REDACTED]", text)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "event": redact(record.getMessage()),
        }
        for key, value in vars(record).items():
            if key in _STANDARD or key.startswith("_"):
                continue
            payload[key] = "[REDACTED]" if _SECRET_KEYS.search(key) else value
        if record.exc_info:
            payload["exc"] = redact(self.formatException(record.exc_info))
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(getattr(logging, level.upper(), logging.INFO))
    for noisy in ("botocore", "boto3", "urllib3", "aws_msk_iam_sasl_signer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
