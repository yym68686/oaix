from oaix_gateway.api_server import create_app


def test_frontend_routes_are_registered() -> None:
    app = create_app()
    paths = {route.path for route in app.routes}

    assert "/" in paths
    assert "/healthz" in paths
    assert "/admin/tokens" in paths
    assert "/admin/token-selection" in paths
    assert "/admin/tokens/{token_id}/activation" in paths
    assert "/admin/tokens/{token_id}" in paths
    assert "/admin/requests" in paths
    assert "/admin/tokens/import" in paths
    assert "/admin/tokens/import-jobs/{job_id}" in paths
    assert "/v1/responses" in paths
    assert "/v1/responses/compact" in paths


def test_assets_mount_is_registered() -> None:
    app = create_app()
    assert any(getattr(route, "path", None) == "/assets" for route in app.routes)
