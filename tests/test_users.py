"""Tests for user management and quota tracking."""

import pytest
import httpx


class TestUserManagement:
    """Test user creation and management endpoints."""

    def test_user_created_on_invoke(self, server_process, worker_process, client, server_url):
        """Test that a user is created when invoking with X-User-ID header."""
        from minimodal import App

        app = App("test-user-app")

        @app.function()
        def simple_task():
            return "done"

        app.deploy()

        # Invoke with a custom user ID
        result = simple_task.remote()
        assert result == "done"

        # Check that default user was created
        r = httpx.get(f"{server_url}/users/default")
        assert r.status_code == 200
        user = r.json()
        assert user["id"] == "default"
        assert user["quota_limit"] == 5  # Default quota

    def test_custom_user_id_header(self, server_process, worker_process, server_url):
        """Test that custom X-User-ID header creates correct user."""
        from minimodal import App, get_client, reset_client
        import os

        reset_client()
        os.environ["MINIMODAL_SERVER_URL"] = server_url
        os.environ["MINIMODAL_USER_ID"] = "test-user-123"

        client = get_client(server_url)
        client.user_id = "test-user-123"  # Override user_id

        app = App("test-custom-user-app")

        @app.function()
        def task_for_user():
            return "user task"

        app.deploy()
        result = task_for_user.remote()
        assert result == "user task"

        # Check that user was created
        r = httpx.get(f"{server_url}/users/test-user-123")
        assert r.status_code == 200
        user = r.json()
        assert user["id"] == "test-user-123"

        # Cleanup
        reset_client()
        os.environ.pop("MINIMODAL_USER_ID", None)

    def test_list_users(self, server_process, server_url):
        """Test listing all users."""
        r = httpx.get(f"{server_url}/users")
        assert r.status_code == 200
        data = r.json()
        assert "users" in data
        assert isinstance(data["users"], list)

    def test_update_user_quota(self, server_process, server_url):
        """Test updating a user's quota limit."""
        # Update quota for a new user
        r = httpx.put(f"{server_url}/users/quota-test-user/quota?quota_limit=10")
        assert r.status_code == 200
        result = r.json()
        assert result["quota_limit"] == 10

        # Verify the update
        r = httpx.get(f"{server_url}/users/quota-test-user")
        assert r.status_code == 200
        user = r.json()
        assert user["quota_limit"] == 10

    def test_quota_limit_validation(self, server_process, server_url):
        """Test that quota limit must be at least 1."""
        r = httpx.put(f"{server_url}/users/bad-quota-user/quota?quota_limit=0")
        assert r.status_code == 400

        r = httpx.put(f"{server_url}/users/bad-quota-user/quota?quota_limit=-1")
        assert r.status_code == 400

    def test_user_not_found(self, server_process, server_url):
        """Test getting a non-existent user returns 404."""
        r = httpx.get(f"{server_url}/users/nonexistent-user-xyz")
        assert r.status_code == 404


class TestUserQuotaTracking:
    """Test that invocations are tracked per user."""

    def test_invocations_have_user_id(self, server_process, worker_process, client, server_url):
        """Test that invocations are associated with a user."""
        from minimodal import App

        app = App("test-user-tracking-app")

        @app.function()
        def tracked_task():
            return "tracked"

        app.deploy()
        result = tracked_task.remote()
        assert result == "tracked"

        # The invocation should have been created with default user
        # Check via invocation stats (which should show recent invocations)
        r = httpx.get(f"{server_url}/stats/invocations?limit=5")
        assert r.status_code == 200
