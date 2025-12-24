"""Async execution support with FunctionCall futures."""

import base64
import time
from typing import TYPE_CHECKING, Any, Iterator

import cloudpickle

if TYPE_CHECKING:
    from minimodal.sdk.client import MiniModalClient

from minimodal.sdk.client import (
    InvocationError,
    RemoteError,
    TimeoutError,
)
from minimodal.server.models import InvocationStatus


class FunctionCall:
    """
    Handle to a pending function invocation.

    Allows non-blocking execution with deferred result retrieval.

    Usage:
        # Submit work without blocking
        future = func.spawn(x, y)

        # Do other work...

        # Get result when needed (blocks until complete)
        result = future.result()

        # Or check if done without blocking
        if future.done is not None:
            result = future.result()
    """

    def __init__(self, invocation_id: int, client: "MiniModalClient"):
        """
        Initialize a FunctionCall.

        Args:
            invocation_id: ID of the invocation in the control plane
            client: MiniModalClient instance for API calls
        """
        self.invocation_id = invocation_id
        self._client = client
        self._result: Any = None
        self._error: RemoteError | None = None
        self._completed = False
        self._status: InvocationStatus | None = None

    def result(self, timeout: float = 300.0) -> Any:
        """
        Wait for and return the result.

        If the result is already available (from a previous call),
        returns it immediately without making another request.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            The deserialized result of the function

        Raises:
            TimeoutError: If timeout is reached
            RemoteError: If the remote function raised an exception
            InvocationError: If result retrieval fails
        """
        if self._completed:
            if self._error:
                raise self._error
            return self._result

        try:
            self._result = self._client.wait_for_result(
                self.invocation_id,
                timeout=timeout,
            )
            self._completed = True
            self._status = InvocationStatus.COMPLETED
            return self._result
        except RemoteError as e:
            self._error = e
            self._completed = True
            self._status = InvocationStatus.FAILED
            raise

    @property
    def done(self) -> bool:
        """
        Check if the invocation is complete without blocking.

        Returns:
            True if completed (success or failure), False if still running/queued
        """
        if self._completed:
            return True

        try:
            response = self._client.get_result(self.invocation_id)
            self._status = response.status

            if response.status == InvocationStatus.COMPLETED:
                # Cache the result
                if response.pickled_result:
                    decoded = base64.b64decode(response.pickled_result)
                    self._result = cloudpickle.loads(decoded)
                self._completed = True
                return True

            if response.status == InvocationStatus.FAILED:
                self._error = RemoteError(
                    "Remote function raised an exception",
                    remote_traceback=response.error_message,
                )
                self._completed = True
                return True

            return False

        except InvocationError:
            # Can't determine status, assume not done
            return False

    @property
    def status(self) -> InvocationStatus | None:
        """
        Get the current status of the invocation.

        Note: This makes a request to the server if the status is not cached.
        Use `done` property for a simple completion check.

        Returns:
            Current InvocationStatus or None if status cannot be determined
        """
        if self._completed:
            return self._status

        try:
            response = self._client.get_result(self.invocation_id)
            self._status = response.status
            return self._status
        except InvocationError:
            return None

    @property
    def is_generator(self) -> bool:
        try:
            response = self._client.get_result(self.invocation_id)
            return response.is_generator
        except InvocationError:
            return False

    def stream(
        self,
        poll_interval: float = 0.1,
        timeout: float = 300.0,
        use_websocket: bool = True,
    ) -> Iterator[Any]:
        """
        Iterate over streaming results from a generator function.

        Yields results as they arrive from the worker. Uses WebSocket for
        real-time push notifications by default.

        Args:
            poll_interval: Time between polling for new chunks (only used if use_websocket=False)
            timeout: Maximum time to wait for all results (seconds)
            use_websocket: Use WebSocket for streaming (default: True, falls back to HTTP if connection fails)

        Yields:
            Individual values yielded by the generator function

        Raises:
            TimeoutError: If timeout is reached
            RemoteError: If the remote function raised an exception

        Example:
            @app.function()
            def generate_items():
                for i in range(10):
                    yield i

            future = generate_items.spawn()
            for item in future.stream():
                print(item)  # Prints 0, 1, 2, ... as they arrive
        """
        if use_websocket:
            try:
                yield from self._stream_via_websocket(timeout)
                return
            except Exception:
                # Fall back to HTTP polling if WebSocket fails
                pass

        yield from self._stream_via_polling(poll_interval, timeout)

    def _stream_via_websocket(self, timeout: float) -> Iterator[Any]:
        for msg_type, data in self._client.stream_via_websocket(
            self.invocation_id,
            timeout=timeout,
        ):
            if msg_type == "chunk":
                decoded = base64.b64decode(data["pickled_value"])
                value = cloudpickle.loads(decoded)
                yield value

            elif msg_type == "complete":
                self._completed = True
                self._status = InvocationStatus.COMPLETED
                return

            elif msg_type == "error":
                self._completed = True
                self._status = InvocationStatus.FAILED
                self._error = RemoteError(
                    "Remote function raised an exception",
                    remote_traceback=data.get("error"),
                )
                raise self._error

    def _stream_via_polling(
        self,
        poll_interval: float,
        timeout: float,
    ) -> Iterator[Any]:
        """Stream results using HTTP polling (fallback)."""
        start = time.time()
        next_sequence = 0

        while time.time() - start < timeout:
            # Get new chunks
            response = self._client.get_stream_results(
                self.invocation_id,
                from_sequence=next_sequence,
            )

            # Yield new chunks
            for chunk in response.chunks:
                decoded = base64.b64decode(chunk.pickled_value)
                value = cloudpickle.loads(decoded)
                yield value
                next_sequence = chunk.sequence + 1

            # Check if complete
            if response.status == InvocationStatus.COMPLETED:
                self._completed = True
                self._status = InvocationStatus.COMPLETED
                return

            if response.status == InvocationStatus.FAILED:
                self._completed = True
                self._status = InvocationStatus.FAILED
                self._error = RemoteError(
                    "Remote function raised an exception",
                    remote_traceback=response.error_message,
                )
                raise self._error

            # Wait before polling again
            time.sleep(poll_interval)

        raise TimeoutError(f"Stream timed out after {timeout}s")

    def __repr__(self) -> str:
        status = self._status.value if self._status else "unknown"
        return f"FunctionCall(invocation_id={self.invocation_id}, status={status})"


def gather(
    *futures: FunctionCall,
    return_exceptions: bool = False,
) -> list[Any]:
    """
    Wait for multiple FunctionCalls to complete and return their results.

    Similar to asyncio.gather(), this collects results from multiple
    concurrent invocations.

    Args:
        *futures: FunctionCall objects to wait for
        return_exceptions: If True, exceptions are returned as results
                          instead of being raised. Default is False.

    Returns:
        List of results in the same order as the input futures

    Raises:
        RemoteError: If any function raised an exception (when return_exceptions=False)
        TimeoutError: If any function times out (when return_exceptions=False)

    Example:
        futures = [func.spawn(i) for i in range(10)]
        results = gather(*futures)  # Wait for all to complete
    """
    results = []

    for future in futures:
        try:
            result = future.result()
            results.append(result)
        except (RemoteError, TimeoutError) as e:
            if return_exceptions:
                results.append(e)
            else:
                raise

    return results


def wait_first(*futures: FunctionCall, timeout: float = 300.0) -> FunctionCall:
    """
    Wait for the first FunctionCall to complete.

    Args:
        *futures: FunctionCall objects to wait for
        timeout: Maximum time to wait in seconds

    Returns:
        The first FunctionCall that completed

    Raises:
        TimeoutError: If timeout is reached before any complete
        ValueError: If no futures are provided
    """
    if not futures:
        raise ValueError("At least one future is required")

    start = time.time()

    # Sorry
    while time.time() - start < timeout:
        for future in futures:
            if future.done:
                return future
        time.sleep(0.1)

    raise TimeoutError(f"No futures completed within {timeout}s")
