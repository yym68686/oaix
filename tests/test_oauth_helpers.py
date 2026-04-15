from fastapi import HTTPException

from oaix_gateway.oauth import is_permanently_invalid_refresh_token_error


def test_detects_refresh_token_reused_error() -> None:
    exc = HTTPException(
        status_code=401,
        detail=(
            'Codex token refresh failed: status 401: {"error":{"code":"refresh_token_reused",'
            '"message":"This refresh token has already been used to generate a new access token. '
            'Please try signing in again."}}'
        ),
    )

    assert is_permanently_invalid_refresh_token_error(exc) is True


def test_detects_invalid_grant_refresh_token_error_from_mapping() -> None:
    exc = HTTPException(
        status_code=400,
        detail={
            "error": "invalid_grant",
            "error_description": "Refresh token revoked, please sign in again.",
        },
    )

    assert is_permanently_invalid_refresh_token_error(exc) is True


def test_ignores_non_refresh_token_errors() -> None:
    exc = HTTPException(status_code=502, detail="Upstream request failed: ReadTimeout")

    assert is_permanently_invalid_refresh_token_error(exc) is False
