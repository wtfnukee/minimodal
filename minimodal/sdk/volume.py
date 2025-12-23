"""Volume class for persistent storage."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from minimodal.sdk.client import MiniModalClient


class Volume:
    """
    Persistent storage that can be mounted to functions.

    Volumes provide persistent storage that survives function invocations
    and can be shared across multiple functions.

    Usage:
        # Create or reference a volume
        vol = Volume.persisted("my-data")

        # Use with a function
        @app.function(volumes={"/data": vol})
        def process_data():
            # Read/write to /data inside the function
            with open("/data/results.json", "w") as f:
                f.write('{"status": "done"}')

        # Upload files from outside
        vol.put_file("local_file.txt", "/remote/path/file.txt")

        # Download files
        vol.get_file("/remote/path/file.txt", "local_copy.txt")

        # List files
        files = vol.list_files("/")
    """

    def __init__(self, name: str):
        """
        Initialize a Volume reference.

        Args:
            name: Unique name for this volume
        """
        self.name = name
        self._client: MiniModalClient | None = None
        self._id: int | None = None

    def _get_client(self) -> "MiniModalClient":
        """Get or create the client."""
        if self._client is None:
            from minimodal.sdk.client import get_client
            self._client = get_client()
        return self._client

    @classmethod
    def from_name(cls, name: str) -> "Volume":
        """
        Reference an existing volume by name.

        Args:
            name: Name of the existing volume

        Returns:
            Volume reference

        Note:
            This does not create the volume if it doesn't exist.
            Use persisted() to create or get a volume.
        """
        return cls(name)

    @classmethod
    def persisted(cls, name: str) -> "Volume":
        """
        Create or get a persistent volume.

        If a volume with this name already exists, returns a reference to it.
        Otherwise, creates a new volume.

        Args:
            name: Unique name for the volume

        Returns:
            Volume reference
        """
        vol = cls(name)
        # The actual creation happens when the volume is first used
        # or when deploy() is called
        return vol

    def _ensure_created(self) -> int:
        """Ensure the volume exists on the server and return its ID."""
        if self._id is not None:
            return self._id

        client = self._get_client()
        self._id = client.create_volume(self.name)
        return self._id

    def put_file(self, local_path: str | Path, remote_path: str) -> None:
        """
        Upload a file to the volume.

        Args:
            local_path: Path to the local file
            remote_path: Destination path in the volume (must start with /)

        Raises:
            FileNotFoundError: If local file doesn't exist
            ConnectionError: If server is unreachable
        """
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        if not remote_path.startswith("/"):
            remote_path = "/" + remote_path

        self._ensure_created()
        client = self._get_client()

        with open(local_path, "rb") as f:
            content = f.read()

        client.upload_volume_file(self.name, remote_path, content)

    def get_file(self, remote_path: str, local_path: str | Path) -> None:
        """
        Download a file from the volume.

        Args:
            remote_path: Path in the volume (must start with /)
            local_path: Destination path on local filesystem

        Raises:
            FileNotFoundError: If remote file doesn't exist
            ConnectionError: If server is unreachable
        """
        if not remote_path.startswith("/"):
            remote_path = "/" + remote_path

        local_path = Path(local_path)

        self._ensure_created()
        client = self._get_client()

        content = client.download_volume_file(self.name, remote_path)

        # Ensure parent directory exists
        local_path.parent.mkdir(parents=True, exist_ok=True)

        with open(local_path, "wb") as f:
            f.write(content)

    def list_files(self, path: str = "/") -> list[str]:
        """
        List files in the volume.

        Args:
            path: Directory path in the volume (default: root)

        Returns:
            List of file/directory names in the path

        Raises:
            ConnectionError: If server is unreachable
        """
        if not path.startswith("/"):
            path = "/" + path

        self._ensure_created()
        client = self._get_client()

        return client.list_volume_files(self.name, path)

    def delete_file(self, remote_path: str) -> None:
        """
        Delete a file from the volume.

        Args:
            remote_path: Path in the volume (must start with /)

        Raises:
            FileNotFoundError: If remote file doesn't exist
            ConnectionError: If server is unreachable
        """
        if not remote_path.startswith("/"):
            remote_path = "/" + remote_path

        self._ensure_created()
        client = self._get_client()

        client.delete_volume_file(self.name, remote_path)

    def __repr__(self) -> str:
        return f"Volume({self.name!r})"
