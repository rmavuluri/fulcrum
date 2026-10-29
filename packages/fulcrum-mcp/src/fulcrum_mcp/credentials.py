"""AWS sessions for Fulcrum, optionally assuming an IAM role.

When an IAM role ARN is configured, the user's base credentials (profile, SSO, env vars or
instance role) are used only to call ``sts:AssumeRole``. Every AWS API call and every MSK IAM
token is then signed with the role's temporary credentials, which botocore refreshes before
they expire.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import boto3
import botocore.session
from botocore.credentials import AssumeRoleCredentialFetcher, CredentialProvider, DeferredRefreshableCredentials
from botocore.exceptions import ClientError, NoCredentialsError

from .errors import AccessDeniedError, FulcrumError, translate_aws_error

ROLE_SESSION_NAME = "fulcrum-mcp"


def assume_role_error(exc: Exception, role_arn: str) -> FulcrumError:
    if isinstance(exc, FulcrumError):
        return exc
    if isinstance(exc, ClientError) and exc.response.get("Error", {}).get("Code") in {
        "AccessDenied",
        "AccessDeniedException",
    }:
        err = AccessDeniedError("sts:AssumeRole", f"on {role_arn}: {exc.response['Error'].get('Message', '')}")
        err.hint = (
            "Two things must both be true: (1) your base identity (profile/SSO/keys) is allowed "
            f"`sts:AssumeRole` on `{role_arn}`, and (2) that role's trust policy trusts your base identity "
            "(and requires the same external ID, if any). See `iam/fulcrum-role-trust-policy.json`."
        )
        return err
    return translate_aws_error(exc, "sts:AssumeRole", role_arn)


def build_session(
    profile: str | None,
    region: str | None,
    role_arn: str | None = None,
    external_id: str | None = None,
    session_factory: Callable[..., Any] = boto3.Session,
) -> Any:
    """Return a boto3 Session for the base credentials, or for ``role_arn`` assumed from them."""
    base = session_factory(profile_name=profile, region_name=region)
    if not role_arn:
        return base

    source = base.get_credentials()
    if source is None:
        raise translate_aws_error(NoCredentialsError(), "sts:AssumeRole", role_arn)
    extra: dict[str, str] = {"RoleSessionName": ROLE_SESSION_NAME}
    if external_id:
        extra["ExternalId"] = external_id
    fetcher = AssumeRoleCredentialFetcher(
        client_creator=base._session.create_client,
        source_credentials=source,
        role_arn=role_arn,
        extra_args=extra,
    )

    def refresh() -> dict[str, Any]:
        try:
            return fetcher.fetch_credentials()
        except Exception as exc:
            raise assume_role_error(exc, role_arn) from exc

    role_botocore = botocore.session.Session()
    role_botocore._credentials = DeferredRefreshableCredentials(refresh_using=refresh, method="assume-role")
    if region:
        role_botocore.set_config_variable("region", region)
    return boto3.Session(botocore_session=role_botocore, region_name=region)


class SessionCredentialProvider(CredentialProvider):
    """Exposes a boto3 Session's (possibly role-assumed, auto-refreshing) credentials to the
    MSK IAM signer, so Kafka tokens use exactly the same identity as the AWS API calls."""

    METHOD = "fulcrum-session"

    def __init__(self, session: Any) -> None:
        super().__init__()
        self._session = session

    def load(self) -> Any:
        credentials = self._session.get_credentials()
        if credentials is None:
            raise NoCredentialsError()
        return credentials
