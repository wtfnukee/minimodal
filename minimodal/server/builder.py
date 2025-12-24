"""
Docker image builder for function containers.

This module handles building container images with:
- Python runtime
- User-specified dependencies
- The function code itself

In production, Modal uses a sophisticated layered image system
with content-addressed caching. We use standard Docker for simplicity.
"""

import hashlib
import logging
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from minimodal.sdk.image import Image


logger = logging.getLogger("minimodal.builder")


# Template for the Dockerfile
DOCKERFILE_TEMPLATE = """FROM python:{python_version}-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \\
    gcc \\
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
{pip_install}

# Copy function code
COPY function.py /app/function.py

# Entry point - the worker executor will be mounted/copied at runtime
CMD ["python", "-c", "print('Container ready')"]
"""


# Template for worker image
WORKER_DOCKERFILE = """FROM python:{python_version}-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir cloudpickle httpx websockets pydantic sqlmodel

# Copy minimodal SDK (needed for unpickling decorated functions)
COPY minimodal /app/minimodal

# Copy worker code
COPY executor.py /app/executor.py

# Add app to Python path
ENV PYTHONPATH=/app

# Entry point
CMD ["python", "/app/executor.py"]
"""


def compute_image_tag(
    python_version: str,
    requirements: list[str],
    function_hash: str,
) -> str:
    """
    Compute a deterministic image tag based on contents.

    This enables caching - same deps + code = same image.
    """
    content = f"{python_version}|{'|'.join(sorted(requirements))}|{function_hash}"
    return f"minimodal:{hashlib.sha256(content.encode()).hexdigest()[:12]}"


def generate_dockerfile(
    python_version: str = "3.11",
    requirements: list[str] | None = None,
) -> str:
    """Generate a Dockerfile for the function container."""
    requirements = requirements or []

    if requirements:
        pip_install = f"RUN pip install --no-cache-dir {' '.join(requirements)}"
    else:
        pip_install = "# No additional requirements"

    return DOCKERFILE_TEMPLATE.format(
        python_version=python_version,
        pip_install=pip_install,
    )


class ImageBuilder:
    """
    Builds Docker images for function execution.

    Workflow:
    1. Check if image already exists (by content hash)
    2. If not, generate Dockerfile
    3. Build image
    4. Tag and store

    In production, you'd want:
    - Layer caching (base image + deps + code as separate layers)
    - Remote registry push
    - Parallel builds
    - Build queue
    """

    def __init__(self, registry_url: str | None = None):
        self.registry_url = registry_url
        self._client = None  # Lazy loaded

    def _get_client(self):
        """Lazy load docker client."""
        if self._client is None:
            try:
                import docker

                self._client = docker.from_env()
            except ImportError:
                raise RuntimeError("Docker SDK not installed. Run: pip install docker")
            except Exception as e:
                raise RuntimeError(f"Failed to connect to Docker: {e}")
        return self._client

    def is_available(self) -> bool:
        """Check if Docker is available."""
        try:
            client = self._get_client()
            client.ping()
            return True
        except Exception:
            return False

    def image_exists(self, tag: str) -> bool:
        """Check if an image already exists locally."""
        try:
            client = self._get_client()
            client.images.get(tag)
            return True
        except Exception:
            return False

    def build_from_image_config(
        self,
        image: "Image",
        extra_commands: list[str] | None = None,
    ) -> str:
        """
        Build a Docker image from an Image configuration.

        Args:
            image: Image configuration object
            extra_commands: Additional RUN commands to append

        Returns:
            The image tag
        """
        # Generate tag from content hash
        tag = f"minimodal:{image.content_hash()}"

        # Check cache
        if self.image_exists(tag):
            logger.info(f"Using cached image: {tag}")
            return tag

        logger.info(f"Building image: {tag}")

        # Generate Dockerfile
        dockerfile = image.to_dockerfile()
        if extra_commands:
            for cmd in extra_commands:
                dockerfile += f"\nRUN {cmd}"

        # Build
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            (tmppath / "Dockerfile").write_text(dockerfile)

            client = self._get_client()
            image_obj, logs = client.images.build(
                path=str(tmppath),
                tag=tag,
                rm=True,
            )

            for log in logs:
                if "stream" in log:
                    line = log["stream"].strip()
                    if line:
                        logger.debug(line)

        logger.info(f"Built image: {tag}")
        return tag

    def build(
        self,
        function_source: str,
        python_version: str = "3.11",
        requirements: list[str] | None = None,
    ) -> str:
        """
        Build a Docker image for the function.

        Args:
            function_source: Python source code of the function
            python_version: Python version to use
            requirements: List of pip packages

        Returns:
            The image tag
        """
        requirements = requirements or []

        # Compute content-based tag
        func_hash = hashlib.sha256(function_source.encode()).hexdigest()[:12]
        tag = compute_image_tag(python_version, requirements, func_hash)

        # Check cache
        if self.image_exists(tag):
            logger.info(f"Using cached image: {tag}")
            return tag

        logger.info(f"Building image: {tag}")

        # Create build context
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            # Write Dockerfile
            dockerfile = generate_dockerfile(python_version, requirements)
            (tmppath / "Dockerfile").write_text(dockerfile)

            # Write function code
            (tmppath / "function.py").write_text(function_source)

            # Build
            client = self._get_client()
            image, logs = client.images.build(
                path=str(tmppath),
                tag=tag,
                rm=True,
            )

            for log in logs:
                if "stream" in log:
                    line = log["stream"].strip()
                    if line:
                        logger.debug(line)

        logger.info(f"Built image: {tag}")
        return tag

    def build_worker_image(
        self,
        python_version: str = "3.11",
        executor_path: str | None = None,
    ) -> str:
        """
        Build the base worker image with the executor baked in.

        Args:
            python_version: Python version to use
            executor_path: Path to executor.py (uses bundled if not specified)

        Returns:
            The image tag
        """
        tag = f"minimodal-worker:{python_version}"

        if self.image_exists(tag):
            logger.info(f"Using cached worker image: {tag}")
            return tag

        logger.info(f"Building worker image: {tag}")

        # Get executor code
        if executor_path:
            executor_code = Path(executor_path).read_text()
        else:
            # Use the bundled executor
            import inspect

            from minimodal.worker import executor

            executor_code = inspect.getsource(executor)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)

            dockerfile = WORKER_DOCKERFILE.format(python_version=python_version)
            (tmppath / "Dockerfile").write_text(dockerfile)
            (tmppath / "executor.py").write_text(executor_code)

            # Copy minimodal SDK for unpickling decorated functions
            # Only copy SDK + models, not full server (which has heavy dependencies)
            import shutil
            import minimodal.sdk
            import minimodal.server.models

            # Copy SDK
            sdk_src = Path(minimodal.sdk.__file__).parent
            sdk_dst = tmppath / "minimodal" / "sdk"
            sdk_dst.parent.mkdir(parents=True)
            shutil.copytree(sdk_src, sdk_dst)

            # Copy just the models file (needed by SDK)
            server_dst = tmppath / "minimodal" / "server"
            server_dst.mkdir(parents=True)
            models_src = Path(minimodal.server.models.__file__)
            shutil.copy(models_src, server_dst / "models.py")
            (server_dst / "__init__.py").write_text("")

            # Create minimal __init__.py
            (tmppath / "minimodal" / "__init__.py").write_text(
                "from minimodal.sdk import App, Image, gather, wait_first\n"
            )

            client = self._get_client()
            image, logs = client.images.build(
                path=str(tmppath),
                tag=tag,
                rm=True,
            )

            for log in logs:
                if "stream" in log:
                    line = log["stream"].strip()
                    if line:
                        logger.debug(line)

        logger.info(f"Built worker image: {tag}")
        return tag

    def push(self, tag: str) -> str:
        """Push image to registry (if configured)."""
        if not self.registry_url:
            return tag

        client = self._get_client()
        remote_tag = f"{self.registry_url}/{tag}"

        # Tag for remote
        image = client.images.get(tag)
        image.tag(remote_tag)

        # Push
        logger.info(f"Pushing image: {remote_tag}")
        client.images.push(remote_tag)

        return remote_tag

    def cleanup_old_images(self, keep_latest: int = 10) -> int:
        """
        Remove old minimodal images to free space.

        Args:
            keep_latest: Number of recent images to keep

        Returns:
            Number of images removed
        """
        client = self._get_client()
        images = client.images.list(name="minimodal")

        # Sort by created date
        images.sort(key=lambda i: i.attrs.get("Created", ""), reverse=True)

        removed = 0
        for image in images[keep_latest:]:
            try:
                client.images.remove(image.id)
                removed += 1
            except Exception as e:
                logger.warning(f"Failed to remove image {image.id}: {e}")

        if removed:
            logger.info(f"Removed {removed} old images")
        return removed
