"""Tests for server health, stats, and endpoints."""

import pytest
import httpx


class TestServerHealth:
    """Test server health and readiness."""

    def test_health_endpoint(self, server_process, server_url):
        """Test /health endpoint returns 200."""
        r = httpx.get(f"{server_url}/health")
        assert r.status_code == 200

    def test_ready_endpoint(self, server_process, server_url):
        """Test /ready endpoint returns 200."""
        r = httpx.get(f"{server_url}/ready")
        assert r.status_code == 200

    def test_system_health_endpoint(self, server_process, server_url):
        """Test /system/health endpoint."""
        r = httpx.get(f"{server_url}/system/health")
        assert r.status_code == 200
        data = r.json()
        assert "status" in data
        assert "metrics" in data


class TestServerStats:
    """Test statistics endpoints."""

    def test_stats_endpoint(self, server_process, server_url):
        """Test /stats endpoint."""
        r = httpx.get(f"{server_url}/stats")
        assert r.status_code == 200
        data = r.json()
        # Stats endpoint returns structured data
        assert "functions" in data or "workers" in data

    def test_function_stats(self, server_process, worker_process, client, server_url):
        """Test /stats/functions endpoint."""
        from minimodal import App

        app = App("test-stats-func")

        @app.function()
        def dummy():
            return 1

        app.deploy()
        dummy.remote()

        r = httpx.get(f"{server_url}/stats/functions")
        assert r.status_code == 200
        # Should have at least one function
        assert len(r.json().get("functions", [])) >= 1

    def test_worker_stats(self, server_process, worker_process, server_url):
        """Test /stats/workers endpoint."""
        r = httpx.get(f"{server_url}/stats/workers")
        assert r.status_code == 200
        data = r.json()
        assert "workers" in data
        assert len(data["workers"]) >= 1  # At least our test worker

    def test_invocation_stats(self, server_process, worker_process, client, server_url):
        """Test /stats/invocations endpoint."""
        from minimodal import App

        app = App("test-stats-inv")

        @app.function()
        def tracked():
            return "tracked"

        app.deploy()
        tracked.remote()

        r = httpx.get(f"{server_url}/stats/invocations")
        assert r.status_code == 200
        data = r.json()
        assert "invocations" in data


class TestServerDeploy:
    """Test deployment functionality."""

    def test_deploy_creates_function(self, server_process, client, server_url):
        """Test that deploy creates a function in the system."""
        from minimodal import App

        app = App("test-deploy-check")

        @app.function()
        def my_func():
            return 42

        result = app.deploy()
        assert "my_func" in result
        func_id = result["my_func"]

        # Verify via stats
        r = httpx.get(f"{server_url}/stats/functions")
        func_names = [f["name"] for f in r.json().get("functions", [])]
        assert "my_func" in func_names

    def test_deploy_multiple_functions(self, server_process, client, server_url):
        """Test deploying multiple functions at once."""
        from minimodal import App

        app = App("test-deploy-multi")

        @app.function()
        def func_a():
            return "a"

        @app.function()
        def func_b():
            return "b"

        @app.function()
        def func_c():
            return "c"

        result = app.deploy()

        assert "func_a" in result
        assert "func_b" in result
        assert "func_c" in result

    def test_deploy_with_cpu_memory(self, server_process, client, server_url):
        """Test deploying with custom CPU and memory."""
        from minimodal import App

        app = App("test-deploy-resources")

        @app.function(cpu=2, memory=1024)
        def resource_func():
            return "done"

        result = app.deploy()
        assert "resource_func" in result


class TestServerInvocations:
    """Test invocation tracking."""

    def test_invocation_tracked(self, server_process, worker_process, client, server_url):
        """Test that invocations are tracked."""
        from minimodal import App

        app = App("test-invocation-track")

        @app.function()
        def tracked_func():
            return "tracked"

        app.deploy()
        tracked_func.remote()

        r = httpx.get(f"{server_url}/stats/invocations")
        assert r.status_code == 200
        invocations = r.json().get("invocations", [])
        assert any(inv.get("status") == "completed" for inv in invocations)

    def test_result_endpoint(self, server_process, worker_process, client, server_url):
        """Test /result/{id} endpoint."""
        from minimodal import App

        app = App("test-result-endpoint")

        @app.function()
        def result_func():
            return 123

        app.deploy()

        # Use spawn to get invocation ID
        future = result_func.spawn()
        invocation_id = future.invocation_id

        # Wait for completion
        future.result()

        # Check result endpoint
        r = httpx.get(f"{server_url}/result/{invocation_id}")
        assert r.status_code == 200
        assert r.json()["status"] == "completed"


class TestServerVolumes:
    """Test volume management endpoints."""

    def test_create_volume(self, server_process, server_url):
        """Test creating a volume."""
        r = httpx.post(f"{server_url}/volumes", json={"name": "server-test-vol"})
        assert r.status_code == 200
        assert "id" in r.json()

    def test_list_volumes(self, server_process, server_url):
        """Test listing volumes."""
        # Create a volume first
        httpx.post(f"{server_url}/volumes", json={"name": "list-test-vol"})

        r = httpx.get(f"{server_url}/volumes")
        assert r.status_code == 200
        data = r.json()
        assert "volumes" in data


class TestServerSecrets:
    """Test secret management endpoints."""

    def test_create_secret(self, server_process, server_url):
        """Test creating a secret."""
        r = httpx.post(
            f"{server_url}/secrets",
            json={"name": "server-test-secret", "value": "secret-value"},
        )
        assert r.status_code == 200
        assert "id" in r.json()

    def test_list_secrets(self, server_process, server_url):
        """Test listing secrets (without values)."""
        # Create a secret first
        httpx.post(
            f"{server_url}/secrets",
            json={"name": "list-test-secret", "value": "hidden"},
        )

        r = httpx.get(f"{server_url}/secrets")
        assert r.status_code == 200
        data = r.json()
        assert "secrets" in data
        # Values should not be exposed
        for secret in data.get("secrets", []):
            assert "value" not in secret or secret.get("value") != "hidden"


class TestServerDashboard:
    """Test dashboard endpoint."""

    def test_dashboard_returns_html(self, server_process, server_url):
        """Test /dashboard returns HTML."""
        r = httpx.get(f"{server_url}/dashboard")
        assert r.status_code == 200
        assert "text/html" in r.headers.get("content-type", "")
