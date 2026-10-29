"""IAM (SASL/OAUTHBEARER) token provider for Amazon MSK.

librdkafka calls the provider as its ``oauth_cb`` whenever it needs a token and again
before the current one expires, so tokens refresh automatically for the life of the client.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

from .errors import translate_aws_error

log = logging.getLogger(__name__)

TokenFn = Callable[[], tuple[str, int]]


class MskIamTokenProvider:
    """Callable suitable for confluent-kafka's ``oauth_cb`` config."""

    def __init__(self, region: str, profile: str | None = None, token_fn: TokenFn | None = None) -> None:
        self.region = region
        self.profile = profile
        self._token_fn = token_fn or self._default_token_fn
        self.last_error: Exception | None = None

    def _default_token_fn(self) -> tuple[str, int]:
        if self.profile:
            return MSKAuthTokenProvider.generate_auth_token_from_profile(self.region, self.profile)
        return MSKAuthTokenProvider.generate_auth_token(self.region)

    def __call__(self, _oauth_config: str | None = None) -> tuple[str, float]:
        started = time.monotonic()
        try:
            token, expiry_ms = self._token_fn()
        except Exception as exc:
            self.last_error = translate_aws_error(exc, "MSK IAM token generation")
            # Never log the token; the exception text carries no secret material.
            log.warning("msk_iam_token_failed", extra={"error": type(exc).__name__})
            raise
        self.last_error = None
        log.debug(
            "msk_iam_token_refreshed",
            extra={"expires_in_s": round(expiry_ms / 1000 - time.time()), "ms": _ms(started)},
        )
        return token, expiry_ms / 1000.0


def _ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)
