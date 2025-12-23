"""Tests for secret injection."""

import pytest
import httpx


class TestSecretInjection:
    """Test that secrets are properly injected as env vars."""

    def test_secret_accessible_in_function(
        self, server_process, worker_process, client
    ):
        """Test that secrets are accessible in function as env vars."""
        from minimodal import App, Secret

        secret = Secret.from_name("TEST_API_KEY")
        secret.set("sk-secret-12345")

        app = App("test-secret")

        @app.function(secrets=["TEST_API_KEY"])
        def read_secret():
            import os

            return os.environ.get("TEST_API_KEY")

        app.deploy()

        result = read_secret.remote()
        assert result == "sk-secret-12345"

    def test_multiple_secrets(self, server_process, worker_process, client):
        """Test multiple secrets injected."""
        from minimodal import App, Secret

        Secret.from_name("DB_HOST").set("localhost")
        Secret.from_name("DB_PASSWORD").set("supersecret")

        app = App("test-multi-secret")

        @app.function(secrets=["DB_HOST", "DB_PASSWORD"])
        def read_secrets():
            import os

            return {
                "host": os.environ.get("DB_HOST"),
                "password": os.environ.get("DB_PASSWORD"),
            }

        app.deploy()

        result = read_secrets.remote()
        assert result["host"] == "localhost"
        assert result["password"] == "supersecret"

    def test_secret_not_in_api_response(self, server_url, server_process, client):
        """Verify secret values aren't exposed in public APIs."""
        from minimodal import Secret

        Secret.from_name("HIDDEN_SECRET").set("should-not-appear")

        # Check /secrets endpoint doesn't return value
        r = httpx.get(f"{server_url}/secrets")
        assert "should-not-appear" not in r.text

    def test_secret_overwrite(self, server_process, worker_process, client):
        """Test overwriting an existing secret."""
        from minimodal import App, Secret

        secret = Secret.from_name("OVERWRITE_SECRET")
        secret.set("original_value")

        app = App("test-secret-overwrite")

        @app.function(secrets=["OVERWRITE_SECRET"])
        def read_secret():
            import os

            return os.environ.get("OVERWRITE_SECRET")

        app.deploy()

        # First call should return original
        result1 = read_secret.remote()
        assert result1 == "original_value"

        # Update secret
        secret.set("new_value")

        # Second call should return new value
        result2 = read_secret.remote()
        assert result2 == "new_value"

    def test_secret_with_special_characters(
        self, server_process, worker_process, client
    ):
        """Test secret with special characters."""
        from minimodal import App, Secret

        special_value = "pa$$w0rd!@#%^&*()"
        Secret.from_name("SPECIAL_SECRET").set(special_value)

        app = App("test-special-secret")

        @app.function(secrets=["SPECIAL_SECRET"])
        def read_special():
            import os

            return os.environ.get("SPECIAL_SECRET")

        app.deploy()

        result = read_special.remote()
        assert result == special_value

    def test_secret_long_value(self, server_process, worker_process, client):
        """Test secret with long value."""
        from minimodal import App, Secret

        long_value = "x" * 1000
        Secret.from_name("LONG_SECRET").set(long_value)

        app = App("test-long-secret")

        @app.function(secrets=["LONG_SECRET"])
        def read_long():
            import os

            return os.environ.get("LONG_SECRET")

        app.deploy()

        result = read_long.remote()
        assert result == long_value
