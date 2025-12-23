"""Tests for web endpoint functionality."""

import pytest
import httpx


class TestWebEndpoints:
    """Test @app.web_endpoint functionality.

    Note: Web endpoints are accessed via /web/{path} prefix.
    """

    def test_get_endpoint(self, server_process, worker_process, client, server_url):
        """Test basic GET endpoint returns 200."""
        from minimodal import App

        app = App("test-web-get")

        @app.web_endpoint(path="/api/hello", method="GET")
        def hello(request=None):
            return {"message": "Hello, World!"}

        app.deploy()

        # Web endpoints are accessed via /web/ prefix
        r = httpx.get(f"{server_url}/web/api/hello", timeout=30.0)
        # Just verify we get a successful response
        assert r.status_code in (200, 201, 202)

    def test_post_endpoint_with_body(
        self, server_process, worker_process, client, server_url
    ):
        """Test POST endpoint with request body."""
        from minimodal import App

        app = App("test-web-post")

        @app.web_endpoint(path="/api/echo", method="POST")
        def echo(request=None):
            body = request.get("body", {}) if request else {}
            return {"received": body}

        app.deploy()

        r = httpx.post(f"{server_url}/web/api/echo", json={"data": "test"}, timeout=30.0)
        assert r.status_code == 200

    def test_path_parameters(self, server_process, worker_process, client, server_url):
        """Test endpoint with path parameters."""
        from minimodal import App

        app = App("test-web-params")

        @app.web_endpoint(path="/api/users/{user_id}", method="GET")
        def get_user(request=None):
            user_id = (
                request.get("path_params", {}).get("user_id") if request else None
            )
            return {"user_id": user_id}

        app.deploy()

        r = httpx.get(f"{server_url}/web/api/users/123", timeout=30.0)
        assert r.status_code == 200

    def test_query_parameters(self, server_process, worker_process, client, server_url):
        """Test endpoint with query parameters."""
        from minimodal import App

        app = App("test-web-query")

        @app.web_endpoint(path="/api/search", method="GET")
        def search(request=None):
            params = request.get("query_params", {}) if request else {}
            return {"query": params.get("q"), "limit": params.get("limit")}

        app.deploy()

        r = httpx.get(f"{server_url}/web/api/search?q=test&limit=10", timeout=30.0)
        assert r.status_code == 200

    def test_delete_endpoint(self, server_process, worker_process, client, server_url):
        """Test DELETE endpoint."""
        from minimodal import App

        app = App("test-web-delete")

        @app.web_endpoint(path="/api/items/{item_id}", method="DELETE")
        def delete_item(request=None):
            item_id = (
                request.get("path_params", {}).get("item_id") if request else None
            )
            return {"deleted": True, "item_id": item_id}

        app.deploy()

        r = httpx.delete(f"{server_url}/web/api/items/456", timeout=30.0)
        assert r.status_code == 200

    def test_put_endpoint(self, server_process, worker_process, client, server_url):
        """Test PUT endpoint."""
        from minimodal import App

        app = App("test-web-put")

        @app.web_endpoint(path="/api/items/{item_id}", method="PUT")
        def update_item(request=None):
            item_id = (
                request.get("path_params", {}).get("item_id") if request else None
            )
            body = request.get("body", {}) if request else {}
            return {"updated": True, "item_id": item_id, "data": body}

        app.deploy()

        r = httpx.put(
            f"{server_url}/web/api/items/789", json={"name": "updated"}, timeout=30.0
        )
        assert r.status_code == 200

    def test_endpoint_returns_dict(
        self, server_process, worker_process, client, server_url
    ):
        """Test endpoint returning a dict."""
        from minimodal import App

        app = App("test-web-dict")

        @app.web_endpoint(path="/api/info", method="GET")
        def get_info(request=None):
            return {"items": [{"id": 1}, {"id": 2}], "count": 2}

        app.deploy()

        r = httpx.get(f"{server_url}/web/api/info", timeout=30.0)
        assert r.status_code in (200, 201, 202)

    def test_web_endpoint_registration(
        self, server_process, worker_process, client, server_url
    ):
        """Test that web endpoints are registered on deploy."""
        from minimodal import App

        app = App("test-web-registration")

        @app.web_endpoint(path="/api/registered", method="GET")
        def registered_endpoint():
            return {"registered": True}

        app.deploy()

        # Check endpoint appears in list
        r = httpx.get(f"{server_url}/web-endpoints")
        assert r.status_code == 200
        endpoints = r.json().get("endpoints", [])
        paths = [e["path"] for e in endpoints]
        assert "/api/registered" in paths
