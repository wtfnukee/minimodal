"""
Container lifecycle management for Docker workers.

This module handles:
- Starting worker containers
- Stopping containers
- Health monitoring
- Automatic cleanup
"""

import logging
import platform
import threading
import time
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("minimodal.containers")


def _get_container_server_url(host_url: str) -> str:
    """
    Convert host URL to container-accessible URL.

    On macOS/Windows, Docker Desktop runs containers in a VM,
    so 'localhost' isn't accessible. Use 'host.docker.internal' instead.
    """
    system = platform.system().lower()
    if system in ("darwin", "windows"):
        # Replace localhost with host.docker.internal
        return host_url.replace("localhost", "host.docker.internal").replace("127.0.0.1", "host.docker.internal")
    return host_url


class ContainerInfo:
    """Information about a running container."""

    def __init__(
        self,
        container_id: str,
        image_tag: str,
        started_at: datetime,
        function_id: Optional[int] = None,
    ):
        self.container_id = container_id
        self.image_tag = image_tag
        self.started_at = started_at
        self.function_id = function_id
        self.last_activity = started_at


class ContainerManager:
    """
    Manages Docker container lifecycle for worker execution.

    Features:
    - Start containers on demand
    - Track running containers
    - Stop idle containers (scale to zero)
    - Health monitoring
    """

    def __init__(
        self,
        control_plane_url: str = "http://localhost:8000",
        idle_timeout: float = 300.0,  # 5 minutes
        max_containers: int = 10,
    ):
        self.control_plane_url = control_plane_url
        self.idle_timeout = idle_timeout
        self.max_containers = max_containers

        self._containers: dict[str, ContainerInfo] = {}
        self._client = None
        self._lock = threading.Lock()
        self._cleanup_thread: Optional[threading.Thread] = None
        self._running = False

    def _get_client(self):
        """Lazy load docker client."""
        if self._client is None:
            try:
                import docker
                self._client = docker.from_env()
            except ImportError:
                raise RuntimeError(
                    "Docker SDK not installed. "
                    "Run: pip install docker"
                )
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

    def start_container(
        self,
        image_tag: str,
        function_id: Optional[int] = None,
        environment: Optional[dict] = None,
    ) -> str:
        """
        Start a new worker container.

        Args:
            image_tag: Docker image tag to run
            function_id: Optional function ID this container serves
            environment: Additional environment variables

        Returns:
            Container ID
        """
        with self._lock:
            if len(self._containers) >= self.max_containers:
                raise RuntimeError(
                    f"Maximum container limit ({self.max_containers}) reached"
                )

        client = self._get_client()

        # Build environment - use container-accessible URL
        container_url = _get_container_server_url(self.control_plane_url)
        env = {
            "MINIMODAL_SERVER_URL": container_url,
            "MINIMODAL_POLL_INTERVAL": "0.5",
        }
        if environment:
            env.update(environment)

        # Start container
        logger.info(f"Starting container with image: {image_tag} (server: {container_url})")
        container = client.containers.run(
            image_tag,
            detach=True,
            environment=env,
            auto_remove=True,  # Remove when stopped
        )

        container_id = container.id[:12]
        now = datetime.now(timezone.utc)

        with self._lock:
            self._containers[container_id] = ContainerInfo(
                container_id=container_id,
                image_tag=image_tag,
                started_at=now,
                function_id=function_id,
            )

        logger.info(f"Started container: {container_id}")
        return container_id

    def stop_container(self, container_id: str, timeout: int = 10) -> bool:
        """
        Stop a container gracefully.

        Args:
            container_id: Container ID to stop
            timeout: Seconds to wait for graceful shutdown

        Returns:
            True if stopped successfully
        """
        client = self._get_client()

        try:
            container = client.containers.get(container_id)
            logger.info(f"Stopping container: {container_id}")
            container.stop(timeout=timeout)

            with self._lock:
                self._containers.pop(container_id, None)

            logger.info(f"Stopped container: {container_id}")
            return True

        except Exception as e:
            logger.warning(f"Failed to stop container {container_id}: {e}")
            with self._lock:
                self._containers.pop(container_id, None)
            return False

    def get_container_status(self, container_id: str) -> Optional[str]:
        """
        Get the status of a container.

        Args:
            container_id: Container ID

        Returns:
            Status string or None if not found
        """
        client = self._get_client()

        try:
            container = client.containers.get(container_id)
            return container.status
        except Exception:
            return None

    def list_containers(self) -> list[ContainerInfo]:
        """List all managed containers."""
        with self._lock:
            return list(self._containers.values())

    def mark_activity(self, container_id: str):
        """Mark a container as having recent activity."""
        with self._lock:
            if container_id in self._containers:
                self._containers[container_id].last_activity = datetime.now(timezone.utc)

    def _cleanup_idle_containers(self):
        """Stop containers that have been idle too long."""
        now = datetime.now(timezone.utc)

        containers_to_stop = []
        with self._lock:
            for container_id, info in self._containers.items():
                idle_seconds = (now - info.last_activity).total_seconds()
                if idle_seconds > self.idle_timeout:
                    containers_to_stop.append(container_id)

        for container_id in containers_to_stop:
            logger.info(f"Stopping idle container: {container_id}")
            self.stop_container(container_id)

    def _cleanup_loop(self):
        """Background thread for cleaning up idle containers."""
        while self._running:
            try:
                self._cleanup_idle_containers()
            except Exception as e:
                logger.error(f"Cleanup error: {e}")
            time.sleep(30)  # Check every 30 seconds

    def start_cleanup_thread(self):
        """Start the background cleanup thread."""
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            return

        self._running = True
        self._cleanup_thread = threading.Thread(
            target=self._cleanup_loop,
            daemon=True,
        )
        self._cleanup_thread.start()
        logger.info("Started container cleanup thread")

    def stop_cleanup_thread(self):
        """Stop the background cleanup thread."""
        self._running = False
        if self._cleanup_thread:
            self._cleanup_thread.join(timeout=5)
        logger.info("Stopped container cleanup thread")

    def stop_all_containers(self):
        """Stop all managed containers."""
        container_ids = list(self._containers.keys())
        for container_id in container_ids:
            self.stop_container(container_id)

    def scale_to(self, count: int, image_tag: str) -> list[str]:
        """
        Scale to a specific number of containers.

        Args:
            count: Desired number of containers
            image_tag: Image to use for new containers

        Returns:
            List of container IDs
        """
        current = len(self._containers)

        if count > current:
            # Scale up
            for _ in range(count - current):
                self.start_container(image_tag)
        elif count < current:
            # Scale down (stop oldest first)
            containers = sorted(
                self._containers.values(),
                key=lambda c: c.started_at,
            )
            to_stop = containers[:current - count]
            for c in to_stop:
                self.stop_container(c.container_id)

        return list(self._containers.keys())

    def __enter__(self):
        self.start_cleanup_thread()
        return self

    def __exit__(self, *args):
        self.stop_cleanup_thread()
        self.stop_all_containers()


# Global container manager
_manager: Optional[ContainerManager] = None


def get_container_manager(
    control_plane_url: str = "http://localhost:8000",
) -> ContainerManager:
    """Get or create the global container manager."""
    global _manager
    if _manager is None:
        _manager = ContainerManager(control_plane_url=control_plane_url)
    return _manager
