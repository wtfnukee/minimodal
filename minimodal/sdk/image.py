"""Container image definition with fluent builder API."""

import hashlib
from typing import Optional


class Image:
    """
    Container image definition for serverless functions.

    Uses a fluent builder pattern similar to Modal's Image API.

    Usage:
        # Create an image with dependencies
        image = (
            Image.debian_slim(python_version="3.11")
            .apt_install("git", "curl")
            .pip_install("numpy", "pandas")
            .run_commands("echo 'Setup complete'")
        )

        # Use with a function
        @app.function(image=image)
        def my_func():
            import numpy as np
            return np.sum([1, 2, 3])
    """

    def __init__(self):
        """Initialize an empty image configuration."""
        self._base: str = "python:3.11-slim"
        self._apt_packages: list[str] = []
        self._pip_packages: list[str] = []
        self._commands: list[str] = []
        self._env: dict[str, str] = {}
        self._copy_files: list[tuple[str, str]] = []  # (src, dest)
        self._workdir: str = "/app"

    # === Factory Methods ===

    @classmethod
    def debian_slim(cls, python_version: str = "3.11") -> "Image":
        """
        Create an image based on Debian slim with Python.

        Args:
            python_version: Python version

        Returns:
            New Image instance
        """
        img = cls()
        img._base = f"python:{python_version}-slim"
        return img

    @classmethod
    def from_dockerfile(cls, path: str) -> "Image":
        """
        Create an image from an existing Dockerfile.

        Args:
            path: Path to the Dockerfile

        Returns:
            New Image instance configured to use the Dockerfile
        """
        img = cls()
        img._base = f"dockerfile:{path}"
        return img

    @classmethod
    def from_registry(cls, image: str) -> "Image":
        """
        Create an image from a registry image.

        Args:
            image: Full image reference (e.g., "python:3.11-alpine")

        Returns:
            New Image instance
        """
        img = cls()
        img._base = image
        return img

    # === Builder Methods ===

    def apt_install(self, *packages: str) -> "Image":
        """
        Install apt packages.

        Args:
            *packages: Package names to install

        Returns:
            Self for chaining
        """
        self._apt_packages.extend(packages)
        return self

    def pip_install(
        self,
        *packages: str,
        find_links: Optional[str] = None,
        index_url: Optional[str] = None,
        extra_index_url: Optional[str] = None,
    ) -> "Image":
        """
        Install pip packages.

        Args:
            *packages: Package names or requirements to install
            find_links: Additional URL to search for packages
            index_url: Override the default PyPI URL
            extra_index_url: Additional PyPI URL to search

        Returns:
            Self for chaining
        """
        # TODO For now, just store package names
        self._pip_packages.extend(packages)
        return self

    def run_commands(self, *commands: str) -> "Image":
        """
        Run shell commands during image build.

        Args:
            *commands: Shell commands to execute

        Returns:
            Self for chaining
        """
        self._commands.extend(commands)
        return self

    def env(self, **kwargs: str) -> "Image":
        """
        Set environment variables.

        Args:
            **kwargs: Environment variable name-value pairs

        Returns:
            Self for chaining
        """
        self._env.update(kwargs)
        return self

    def copy_local_file(self, local_path: str, remote_path: str) -> "Image":
        """
        Copy a local file into the image.

        Args:
            local_path: Path on the host machine
            remote_path: Path inside the container

        Returns:
            Self for chaining
        """
        self._copy_files.append((local_path, remote_path))
        return self

    def workdir(self, path: str) -> "Image":
        """
        Set the working directory.

        Args:
            path: Working directory path

        Returns:
            Self for chaining
        """
        self._workdir = path
        return self

    # === Properties ===

    @property
    def base(self) -> str:
        return self._base

    @property
    def apt_packages(self) -> list[str]:
        return self._apt_packages.copy()

    @property
    def pip_packages(self) -> list[str]:
        return self._pip_packages.copy()

    @property
    def commands(self) -> list[str]:
        return self._commands.copy()

    @property
    def environment(self) -> dict[str, str]:
        return self._env.copy()

    # === Utilities ===

    def content_hash(self) -> str:
        """
        Generate a content-based hash for this image configuration.

        Same configuration = same hash, enabling image caching.
        Two builds from the same Dockerfile are not guaranteed to have the same digest.

        Returns:
            12-character hex hash
        """
        content = "|".join(
            [
                self._base,
                "|".join(sorted(self._apt_packages)),
                "|".join(sorted(self._pip_packages)),
                "|".join(self._commands),
                "|".join(f"{k}={v}" for k, v in sorted(self._env.items())),
                self._workdir,
            ]
        )
        return hashlib.sha256(content.encode()).hexdigest()[:12]

    def to_dockerfile(self) -> str:
        """
        Generate Dockerfile contents for this image.

        Returns:
            Dockerfile as a string
        """
        lines = [f"FROM {self._base}"]

        # Set workdir
        lines.append(f"WORKDIR {self._workdir}")

        # Environment variables
        for key, value in self._env.items():
            lines.append(f"ENV {key}={value}")

        # Apt packages
        if self._apt_packages:
            apt_install = " ".join(self._apt_packages)
            lines.append(
                f"RUN apt-get update && "
                f"apt-get install -y --no-install-recommends {apt_install} && "
                f"rm -rf /var/lib/apt/lists/*"
            )

        # Pip packages (always include cloudpickle for function deserialization)
        all_pip = ["cloudpickle"] + self._pip_packages
        pip_install = " ".join(all_pip)
        lines.append(f"RUN pip install --no-cache-dir {pip_install}")

        # Custom commands
        for cmd in self._commands:
            lines.append(f"RUN {cmd}")

        # Copy files
        for src, dest in self._copy_files:
            lines.append(f"COPY {src} {dest}")

        return "\n".join(lines)

    def __repr__(self) -> str:
        pkgs = len(self._pip_packages)
        return f"Image(base={self._base!r}, pip_packages={pkgs})"

    # === Serialization ===

    def to_dict(self) -> dict:
        """
        Serialize image configuration to a dictionary.

        Returns:
            Dictionary representation of the image config
        """
        return {
            "base": self._base,
            "apt_packages": self._apt_packages,
            "pip_packages": self._pip_packages,
            "commands": self._commands,
            "env": self._env,
            "copy_files": self._copy_files,
            "workdir": self._workdir,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Image":
        """
        Create an Image from a dictionary.

        Args:
            data: Dictionary representation of image config

        Returns:
            New Image instance
        """
        img = cls()
        img._base = data.get("base", "python:3.11-slim")
        img._apt_packages = data.get("apt_packages", [])
        img._pip_packages = data.get("pip_packages", [])
        img._commands = data.get("commands", [])
        img._env = data.get("env", {})
        img._copy_files = data.get("copy_files", [])
        img._workdir = data.get("workdir", "/app")
        return img


# === Default Images ===

# Default image for functions without explicit image
DEFAULT_IMAGE = Image.debian_slim(python_version="3.11")
