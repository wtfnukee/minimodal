"""HTTP client for communicating with the control plane."""

import base64
import json
import os
import time
from typing import Any, Callable, TypeVar

import cloudpickle
import httpx
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

from minimodal.server.models import (
    BatchInvokeRequest,
    BatchInvokeResponse,
    DeployRequest,
    DeployResponse,
    InvocationStatus,
    InvokeRequest,
    InvokeResponse,
    ResultResponse,
    SecretCreateRequest,
    SecretResponse,
    StreamResultsResponse,
    VolumeCreateRequest,
    VolumeResponse,
)

# === Custom Exceptions ===


class MiniModalError(Exception):
    """Base exception for minimodal errors."""


class ConnectionError(MiniModalError):
    """Failed to connect to the control plane."""


class DeploymentError(MiniModalError):
    """Failed to deploy a function."""


class InvocationError(MiniModalError):
    """Error during function invocation."""


class TimeoutError(MiniModalError):
    """Operation timed out."""


class RemoteError(MiniModalError):
    """Remote function raised an exception."""

    def __init__(self, message: str, remote_traceback: str | None = None):
        super().__init__(message)
        self.remote_traceback = remote_traceback


# === Retry Logic ===

T = TypeVar("T")


def retry_with_backoff(
    func: Callable[[], T],
    max_retries: int = 3,
    initial_delay: float = 0.1,
    max_delay: float = 5.0,
    backoff_factor: float = 2.0,
    retryable_exceptions: tuple = (httpx.ConnectError, httpx.TimeoutException),
) -> T:
    """
    Retry a function with exponential backoff.

    Args:
        func: Function to retry
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries (seconds)
        max_delay: Maximum delay between retries (seconds)
        backoff_factor: Multiplier for delay after each retry
        retryable_exceptions: Tuple of exceptions that should trigger a retry

    Returns:
        The result of the function

    Raises:
        The last exception if all retries fail
    """
    delay = initial_delay
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return func()
        except retryable_exceptions as e:
            last_exception = e
            if attempt < max_retries:
                time.sleep(delay)
                delay = min(delay * backoff_factor, max_delay)
            else:
                raise ConnectionError(
                    f"Failed to connect after {max_retries + 1} attempts: {e}"
                ) from e

    raise last_exception  # Should not reach here


class MiniModalClient:
    """Client for the minimodal control plane."""

    def __init__(
        self,
        base_url: str | None = None,
        timeout: float = 30.0,
        max_retries: int = 3,
        user_id: str | None = None,
    ):
        """
        Initialize the client.

        Args:
            base_url: URL of the control plane server (default: http://localhost:8000)
            timeout: Request timeout in seconds
            max_retries: Number of retries for transient failures
            user_id: User ID for quota tracking (default: from env or 'default')
        """
        self.base_url = base_url or os.environ.get(
            "MINIMODAL_SERVER_URL", "http://localhost:8000"
        )
        self.timeout = timeout
        self.max_retries = max_retries
        self.user_id = user_id or os.environ.get("MINIMODAL_USER_ID", "default")
        self._client = httpx.Client(timeout=timeout)

    def _request(
        self,
        method: str,
        path: str,
        **kwargs,
    ) -> httpx.Response:
        """Make an HTTP request with retry logic."""

        def do_request() -> httpx.Response:
            response = self._client.request(
                method,
                f"{self.base_url}{path}",
                **kwargs,
            )
            return response

        return retry_with_backoff(
            do_request,
            max_retries=self.max_retries,
        )

    def health_check(self) -> bool:
        """Check if the server is healthy."""
        try:
            response = self._request("GET", "/health")
            return response.status_code == 200
        except (ConnectionError, httpx.HTTPError):
            return False

    def deploy(
        self,
        app_name: str,
        function_name: str,
        func: Callable,
        python_version: str = "3.11",
        requirements: list[str] | None = None,
        cpu: int = 1,
        memory: int = 512,
        image_config: dict | None = None,
        volume_mounts: dict[str, str] | None = None,
        secrets: list[str] | None = None,
        timeout: int = 300,
        max_retries: int = 3,
    ) -> DeployResponse:
        """
        Deploy a function to the control plane.

        Args:
            app_name: Name of the application
            function_name: Name of the function
            func: The function to deploy
            python_version: Python version for the container
            requirements: List of pip packages to install
            cpu: Number of CPU cores required (default: 1)
            memory: Memory in MB required (default: 512)
            image_config: Optional custom Image configuration dict
            volume_mounts: Dict mapping mount paths to volume names
            secrets: List of secret names to inject as env vars
            timeout: Task timeout in seconds (default: 300)
            max_retries: Number of retry attempts on failure (default: 3)

        Returns:
            DeployResponse with function_id and status

        Raises:
            DeploymentError: If deployment fails
            ConnectionError: If unable to connect to server
        """
        try:
            pickled = cloudpickle.dumps(func)
        except Exception as e:
            raise DeploymentError(f"Failed to serialize function: {e}") from e

        request = DeployRequest(
            app_name=app_name,
            function_name=function_name,
            pickled_code=base64.b64encode(pickled),
            python_version=python_version,
            requirements=requirements or [],
            cpu=cpu,
            memory=memory,
            image_config=image_config,
            volume_mounts=volume_mounts,
            secrets=secrets,
            timeout=timeout,
            max_retries=max_retries,
        )

        try:
            response = self._request(
                "POST",
                "/deploy",
                json=request.model_dump(mode="json"),
            )
            response.raise_for_status()
            return DeployResponse.model_validate(response.json())
        except httpx.HTTPStatusError as e:
            raise DeploymentError(
                f"Deployment failed: {e.response.status_code} - {e.response.text}"
            ) from e

    def invoke(self, function_id: int, args: tuple, kwargs: dict) -> int:
        """
        Invoke a function and return the invocation ID.

        Args:
            function_id: ID of the deployed function
            args: Positional arguments
            kwargs: Keyword arguments

        Returns:
            Invocation ID

        Raises:
            InvocationError: If invocation fails
            ConnectionError: If unable to connect to server
        """
        try:
            pickled = cloudpickle.dumps((args, kwargs))
        except Exception as e:
            raise InvocationError(f"Failed to serialize arguments: {e}") from e

        request = InvokeRequest(
            function_id=function_id,
            pickled_args=base64.b64encode(pickled),
        )

        try:
            response = self._request(
                "POST",
                "/invoke",
                json=request.model_dump(mode="json"),
                headers={"X-User-ID": self.user_id},
            )
            response.raise_for_status()
            result = InvokeResponse.model_validate(response.json())
            return result.invocation_id
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise InvocationError(f"Function {function_id} not found") from e
            raise InvocationError(
                f"Invocation failed: {e.response.status_code} - {e.response.text}"
            ) from e

    def batch_invoke(
        self,
        function_id: int,
        args_list: list[tuple[tuple, dict]],
    ) -> list[int]:
        """
        Invoke a function multiple times with different arguments.

        This is more efficient than multiple single invokes for parallel workloads
        like map() operations. All invocations are created in a single transaction.

        Args:
            function_id: ID of the deployed function
            args_list: List of (args, kwargs) tuples for each invocation

        Returns:
            List of invocation IDs

        Raises:
            InvocationError: If invocation fails
            ConnectionError: If unable to connect to server
        """
        # Serialize all args
        pickled_args_list = []
        for args, kwargs in args_list:
            try:
                pickled = cloudpickle.dumps((args, kwargs))
                pickled_args_list.append(base64.b64encode(pickled).decode())
            except Exception as e:
                raise InvocationError(f"Failed to serialize arguments: {e}") from e

        request = BatchInvokeRequest(
            function_id=function_id,
            pickled_args_list=pickled_args_list,
        )

        try:
            response = self._request(
                "POST",
                "/invoke/batch",
                json=request.model_dump(mode="json"),
                headers={"X-User-ID": self.user_id},
            )
            response.raise_for_status()
            result = BatchInvokeResponse.model_validate(response.json())
            return result.invocation_ids
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise InvocationError(f"Function {function_id} not found") from e
            raise InvocationError(
                f"Batch invocation failed: {e.response.status_code} - {e.response.text}"
            ) from e

    def get_result(self, invocation_id: int) -> ResultResponse:
        """
        Get the result of an invocation.

        Args:
            invocation_id: ID of the invocation

        Returns:
            ResultResponse with status and optional result/error

        Raises:
            InvocationError: If result retrieval fails
            ConnectionError: If unable to connect to server
        """
        try:
            response = self._request("GET", f"/result/{invocation_id}")
            response.raise_for_status()
            return ResultResponse.model_validate(response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise InvocationError(f"Invocation {invocation_id} not found") from e
            raise InvocationError(
                f"Failed to get result: {e.response.status_code}"
            ) from e

    def get_stream_results(
        self,
        invocation_id: int,
        from_sequence: int = 0,
    ) -> "StreamResultsResponse":
        """
        Get streaming results from a generator function.

        Args:
            invocation_id: ID of the invocation
            from_sequence: Start fetching from this sequence number

        Returns:
            StreamResultsResponse with chunks and status

        Raises:
            InvocationError: If result retrieval fails
            ConnectionError: If unable to connect to server
        """
        try:
            response = self._request(
                "GET",
                f"/result/{invocation_id}/stream",
                params={"from_sequence": from_sequence},
            )
            response.raise_for_status()
            return StreamResultsResponse.model_validate(response.json())
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise InvocationError(f"Invocation {invocation_id} not found") from e
            raise InvocationError(
                f"Failed to get stream results: {e.response.status_code}"
            ) from e

    def stream_via_websocket(
        self,
        invocation_id: int,
        timeout: float = 300.0,
    ):
        """
        Stream results via WebSocket connection.

        Yields results as they arrive from the worker via push notifications.
        More efficient than polling for streaming results.

        Args:
            invocation_id: ID of the invocation
            timeout: Connection timeout in seconds

        Yields:
            Tuples of (type, data) where type is 'chunk', 'complete', or 'error'

        Raises:
            ConnectionError: If unable to connect to WebSocket
            InvocationError: If streaming fails
        """
        # Convert HTTP URL to WebSocket URL
        ws_url = self.base_url.replace("http://", "ws://").replace("https://", "wss://")
        ws_url = f"{ws_url}/ws/stream/{invocation_id}"

        try:
            with connect(ws_url, close_timeout=timeout) as ws:
                while True:
                    try:
                        message = ws.recv(timeout=timeout)
                        data = json.loads(message)
                        msg_type = data.get("type")

                        if msg_type == "chunk":
                            yield (
                                "chunk",
                                {
                                    "sequence": data.get("sequence"),
                                    "pickled_value": data.get("pickled_value"),
                                },
                            )
                        elif msg_type == "complete":
                            yield (
                                "complete",
                                {
                                    "total_chunks": data.get("total_chunks"),
                                },
                            )
                            return
                        elif msg_type == "error":
                            yield (
                                "error",
                                {
                                    "error": data.get("error"),
                                },
                            )
                            return

                    except ConnectionClosed:
                        return
        except Exception as e:
            raise ConnectionError(f"WebSocket connection failed: {e}") from e

    def wait_for_result(
        self,
        invocation_id: int,
        poll_interval: float = 0.5,
        timeout: float = 300.0,
        use_websocket: bool = True,
    ) -> Any:
        """
        Wait for invocation to complete and return deserialized result.

        Uses WebSocket for push-based notification by default, with HTTP polling as fallback.

        Args:
            invocation_id: ID of the invocation
            poll_interval: Seconds between status checks (only used if WebSocket fails)
            timeout: Maximum time to wait (seconds)
            use_websocket: Use WebSocket for result notification (default: True)

        Returns:
            The deserialized result of the function

        Raises:
            TimeoutError: If timeout is reached
            RemoteError: If the remote function raised an exception
            InvocationError: If result retrieval fails
        """
        if use_websocket:
            try:
                return self._wait_for_result_websocket(invocation_id, timeout)
            except Exception:
                # Fall back to HTTP polling if WebSocket fails
                pass

        return self._wait_for_result_polling(invocation_id, poll_interval, timeout)

    def _wait_for_result_websocket(
        self,
        invocation_id: int,
        timeout: float = 300.0,
    ) -> Any:
        """Wait for result using WebSocket push notification."""
        # Convert HTTP URL to WebSocket URL
        ws_url = self.base_url.replace("http://", "ws://").replace("https://", "wss://")
        ws_url = f"{ws_url}/ws/result/{invocation_id}"

        try:
            with connect(ws_url, close_timeout=timeout) as ws:
                message = ws.recv(timeout=timeout)
                data = json.loads(message)
                msg_type = data.get("type")

                if msg_type == "result":
                    pickled_result = data.get("pickled_result")
                    if pickled_result:
                        decoded = base64.b64decode(pickled_result)
                        return cloudpickle.loads(decoded)
                    return None

                elif msg_type == "error":
                    raise RemoteError(
                        "Remote function raised an exception",
                        remote_traceback=data.get("error"),
                    )

                else:
                    raise InvocationError(f"Unexpected message type: {msg_type}")

        except ConnectionClosed:
            raise InvocationError("WebSocket connection closed unexpectedly")
        except Exception as e:
            if isinstance(e, (RemoteError, InvocationError, TimeoutError)):
                raise
            raise ConnectionError(f"WebSocket connection failed: {e}") from e

    def _wait_for_result_polling(
        self,
        invocation_id: int,
        poll_interval: float = 0.5,
        timeout: float = 300.0,
    ) -> Any:
        """Wait for result using HTTP polling (fallback)."""
        start = time.time()

        while time.time() - start < timeout:
            result = self.get_result(invocation_id)

            if result.status == InvocationStatus.COMPLETED:
                if result.pickled_result:
                    decoded = base64.b64decode(result.pickled_result)
                    return cloudpickle.loads(decoded)
                return None

            if result.status == InvocationStatus.FAILED:
                raise RemoteError(
                    "Remote function raised an exception",
                    remote_traceback=result.error_message,
                )

            time.sleep(poll_interval)

        raise TimeoutError(f"Invocation {invocation_id} timed out after {timeout}s")

    # === Volume Methods ===

    def create_volume(self, name: str) -> int:
        """
        Create a volume or get existing.

        Args:
            name: Volume name

        Returns:
            Volume ID

        Raises:
            ConnectionError: If unable to connect to server
        """
        request = VolumeCreateRequest(name=name)
        response = self._request(
            "POST",
            "/volumes",
            json=request.model_dump(),
        )
        response.raise_for_status()
        result = VolumeResponse.model_validate(response.json())
        return result.id

    def upload_volume_file(
        self,
        volume_name: str,
        remote_path: str,
        content: bytes,
    ) -> None:
        """
        Upload a file to a volume.

        Args:
            volume_name: Name of the volume
            remote_path: Path in the volume
            content: File content as bytes

        Raises:
            InvocationError: If upload fails
            ConnectionError: If unable to connect to server
        """
        try:
            response = self._client.post(
                f"{self.base_url}/volumes/{volume_name}/files",
                params={"path": remote_path},
                files={"file": ("file", content)},
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise InvocationError(f"Volume {volume_name} not found") from e
            raise InvocationError(
                f"Upload failed: {e.response.status_code} - {e.response.text}"
            ) from e

    def download_volume_file(
        self,
        volume_name: str,
        remote_path: str,
    ) -> bytes:
        """
        Download a file from a volume.

        Args:
            volume_name: Name of the volume
            remote_path: Path in the volume

        Returns:
            File content as bytes

        Raises:
            FileNotFoundError: If file doesn't exist
            ConnectionError: If unable to connect to server
        """
        try:
            response = self._request(
                "GET",
                f"/volumes/{volume_name}/files/{remote_path.lstrip('/')}",
            )
            response.raise_for_status()
            return response.content
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise FileNotFoundError(
                    f"File not found: {remote_path} in volume {volume_name}"
                ) from e
            raise InvocationError(f"Download failed: {e.response.status_code}") from e

    def list_volume_files(
        self,
        volume_name: str,
        path: str = "/",
    ) -> list[str]:
        """
        List files in a volume.

        Args:
            volume_name: Name of the volume
            path: Directory path

        Returns:
            List of file names

        Raises:
            ConnectionError: If unable to connect to server
        """
        try:
            response = self._request(
                "GET",
                f"/volumes/{volume_name}/list",
                params={"path": path},
            )
            response.raise_for_status()
            data = response.json()
            return [f["name"] for f in data.get("files", [])]
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise FileNotFoundError("Volume or path not found") from e
            raise InvocationError(f"List failed: {e.response.status_code}") from e

    def delete_volume_file(
        self,
        volume_name: str,
        remote_path: str,
    ) -> None:
        """
        Delete a file from a volume.

        Args:
            volume_name: Name of the volume
            remote_path: Path in the volume

        Raises:
            FileNotFoundError: If file doesn't exist
            ConnectionError: If unable to connect to server
        """
        try:
            response = self._request(
                "DELETE",
                f"/volumes/{volume_name}/files/{remote_path.lstrip('/')}",
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise FileNotFoundError(
                    f"File not found: {remote_path} in volume {volume_name}"
                ) from e
            raise InvocationError(f"Delete failed: {e.response.status_code}") from e

    # === Secret Methods ===

    def create_secret(self, name: str, value: str) -> int:
        """
        Create or update a secret.

        Args:
            name: Secret name
            value: Secret value

        Returns:
            Secret ID

        Raises:
            ConnectionError: If unable to connect to server
        """
        request = SecretCreateRequest(name=name, value=value)
        response = self._request(
            "POST",
            "/secrets",
            json=request.model_dump(),
        )
        response.raise_for_status()
        result = SecretResponse.model_validate(response.json())
        return result.id

    def get_secret_value(self, name: str) -> str:
        """
        Get a secret's decrypted value (internal use).

        Args:
            name: Secret name

        Returns:
            Decrypted secret value

        Raises:
            InvocationError: If secret doesn't exist
            ConnectionError: If unable to connect to server
        """
        try:
            response = self._request("GET", f"/secrets/internal/{name}")
            response.raise_for_status()
            return response.json().get("value", "")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise InvocationError(f"Secret {name} not found") from e
            raise InvocationError(
                f"Failed to get secret: {e.response.status_code}"
            ) from e

    def close(self):
        """Close the HTTP client."""
        self._client.close()

    def __enter__(self) -> "MiniModalClient":
        return self

    def __exit__(self, *args):
        self.close()


# Global client instance
_client: MiniModalClient | None = None
# You should know that I hate this choice as much as you do,
# but I can't come up with better solution


def get_client(base_url: str | None = None) -> MiniModalClient:
    """
    Get or create a global client instance.

    Args:
        base_url: Optional server URL (only used when creating new client)

    Returns:
        MiniModalClient instance
    """
    global _client
    if _client is None:
        _client = MiniModalClient(base_url=base_url)
    return _client


def reset_client():
    """Reset the global client (for testing)"""
    global _client
    if _client is not None:
        _client.close()
        _client = None
