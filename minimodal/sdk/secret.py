"""Secret class for encrypted environment variables."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from minimodal.sdk.client import MiniModalClient


class Secret:
    """
    Encrypted environment variables for functions.

    Secrets are stored encrypted on the server and injected as
    environment variables when functions run.

    Usage:
        # Create a secret
        api_key = Secret.from_name("API_KEY")
        api_key.set("sk-123456")

        # Use with a function
        @app.function(secrets=[api_key])
        def call_api():
            import os
            key = os.environ["API_KEY"]  # Injected at runtime
            # ...

        # Or reference an existing secret
        db_secret = Secret.from_name("DATABASE_URL")

        @app.function(secrets=[db_secret])
        def query_db():
            import os
            db_url = os.environ["DATABASE_URL"]
            # ...
    """

    def __init__(self, name: str):
        """
        Initialize a Secret reference.

        Args:
            name: The name of the secret (also the env var name)
        """
        self.name = name
        self._client: MiniModalClient | None = None
        self._id: int | None = None
        self._local_value: str | None = None

    def _get_client(self) -> "MiniModalClient":
        """Get or create the client."""
        if self._client is None:
            from minimodal.sdk.client import get_client
            self._client = get_client()
        return self._client

    @classmethod
    def from_name(cls, name: str) -> "Secret":
        """
        Reference a secret by name.

        The secret doesn't need to exist yet - it can be created
        later with set().

        Args:
            name: Name of the secret (will be the environment variable name)

        Returns:
            Secret reference
        """
        return cls(name)

    @classmethod
    def from_dict(cls, secrets: dict[str, str]) -> list["Secret"]:
        """
        Create multiple secrets from a dictionary.

        Args:
            secrets: Dictionary mapping names to values

        Returns:
            List of Secret objects

        Example:
            secrets = Secret.from_dict({
                "API_KEY": "sk-123",
                "DB_URL": "postgresql://...",
            })
        """
        result = []
        for name, value in secrets.items():
            secret = cls(name)
            secret._local_value = value
            result.append(secret)
        return result

    def set(self, value: str) -> None:
        """
        Set the secret value.

        The value is encrypted before being stored on the server.

        Args:
            value: The secret value to store
        """
        client = self._get_client()
        self._id = client.create_secret(self.name, value)
        self._local_value = value

    def _ensure_created(self) -> int:
        """Ensure the secret is created on the server."""
        if self._id is not None:
            return self._id

        if self._local_value is not None:
            self.set(self._local_value)
            return self._id

        # Try to verify it exists on the server
        client = self._get_client()
        try:
            # This will raise if secret doesn't exist
            client.get_secret_value(self.name)
            return 0  # We don't know the ID but it exists
        except Exception:
            raise ValueError(
                f"Secret '{self.name}' not found. "
                f"Call secret.set(value) to create it."
            )

    def __repr__(self) -> str:
        return f"Secret({self.name!r})"


# Convenience for creating environment-variable style secrets
def secret_from_env(name: str) -> Secret:
    """
    Create a secret from an environment variable.

    Reads the value from the current environment and creates
    a secret with that value.

    Args:
        name: Name of the environment variable

    Returns:
        Secret with the value from the environment

    Raises:
        KeyError: If the environment variable doesn't exist
    """
    import os

    value = os.environ[name]
    secret = Secret(name)
    secret._local_value = value
    return secret
