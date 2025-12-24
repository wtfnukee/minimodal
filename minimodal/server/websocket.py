"""
WebSocket manager for real-time task distribution to workers.

This replaces polling with push-based notifications:
- Workers connect via WebSocket and wait for tasks
- Server pushes tasks directly to available workers
- Results can be sent back via WebSocket or HTTP
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from fastapi import WebSocket

logger = logging.getLogger("minimodal.websocket")


@dataclass
class ConnectedWorker:
    """Tracks a connected worker's state."""
    worker_id: str
    websocket: WebSocket
    cpu_cores: int = 1
    memory_mb: int = 1024
    is_busy: bool = False
    current_task_id: int | None = None
    connected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    tasks_completed: int = 0


class WorkerConnectionManager:
    """
    Manages WebSocket connections from workers.

    Responsibilities:
    - Track connected workers
    - Distribute tasks to available workers
    - Handle worker disconnections gracefully
    """

    def __init__(self):
        self._workers: dict[str, ConnectedWorker] = {}
        self._lock = asyncio.Lock()
        # Queue for tasks waiting to be assigned
        self._pending_tasks: asyncio.Queue = asyncio.Queue()
        # Event to signal when a worker becomes available
        self._worker_available = asyncio.Event()

    async def connect(
        self,
        websocket: WebSocket,
        worker_id: str,
        cpu_cores: int = 1,
        memory_mb: int = 1024,
    ) -> ConnectedWorker:
        """Register a new worker connection."""
        await websocket.accept()

        async with self._lock:
            worker = ConnectedWorker(
                worker_id=worker_id,
                websocket=websocket,
                cpu_cores=cpu_cores,
                memory_mb=memory_mb,
            )
            self._workers[worker_id] = worker
            logger.info(f"Worker {worker_id} connected (cpu={cpu_cores}, memory={memory_mb}MB)")

            # Signal that a worker is available
            self._worker_available.set()

        return worker

    async def disconnect(self, worker_id: str):
        """Handle worker disconnection."""
        async with self._lock:
            if worker_id in self._workers:
                worker = self._workers[worker_id]
                del self._workers[worker_id]
                logger.info(f"Worker {worker_id} disconnected (completed {worker.tasks_completed} tasks)")

                # If worker was busy, the task needs to be re-queued
                if worker.current_task_id:
                    logger.warning(f"Worker {worker_id} disconnected while processing task {worker.current_task_id}")
                    # The task will timeout and be handled by the server

    def get_worker(self, worker_id: str) -> ConnectedWorker | None:
        """Get a connected worker by ID."""
        return self._workers.get(worker_id)

    def get_connected_workers(self) -> list[ConnectedWorker]:
        """Get all connected workers."""
        return list(self._workers.values())

    def get_available_workers(self) -> list[ConnectedWorker]:
        """Get workers that are not busy."""
        return [w for w in self._workers.values() if not w.is_busy]

    async def find_worker_for_task(
        self,
        required_cpu: int = 1,
        required_memory: int = 512,
    ) -> ConnectedWorker | None:
        """
        Find an available worker that can handle the task.

        Args:
            required_cpu: CPU cores required
            required_memory: Memory in MB required

        Returns:
            A suitable worker or None if none available
        """
        async with self._lock:
            for worker in self._workers.values():
                if (not worker.is_busy and
                    worker.cpu_cores >= required_cpu and
                    worker.memory_mb >= required_memory):
                    return worker
        return None

    async def assign_task(
        self,
        worker: ConnectedWorker,
        task_data: dict,
    ) -> bool:
        """
        Send a task to a specific worker via WebSocket.

        Args:
            worker: The worker to send the task to
            task_data: Task information including invocation_id, pickled_code, etc.

        Returns:
            True if task was sent successfully
        """
        try:
            async with self._lock:
                worker.is_busy = True
                worker.current_task_id = task_data.get("id")

            # Send task via WebSocket
            await worker.websocket.send_json({
                "type": "task",
                "task": task_data,
            })
            logger.info(f"Assigned task {task_data.get('id')} to worker {worker.worker_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to assign task to worker {worker.worker_id}: {e}")
            async with self._lock:
                worker.is_busy = False
                worker.current_task_id = None
            return False

    async def mark_task_complete(self, worker_id: str):
        """Mark a worker as no longer busy."""
        async with self._lock:
            if worker_id in self._workers:
                worker = self._workers[worker_id]
                worker.is_busy = False
                worker.current_task_id = None
                worker.tasks_completed += 1
                # Signal that a worker is available
                self._worker_available.set()

    async def broadcast(self, message: dict):
        """Send a message to all connected workers."""
        disconnected = []
        for worker_id, worker in self._workers.items():
            try:
                await worker.websocket.send_json(message)
            except Exception:
                disconnected.append(worker_id)

        # Clean up disconnected workers
        for worker_id in disconnected:
            await self.disconnect(worker_id)

    async def send_to_worker(self, worker_id: str, message: dict) -> bool:
        """Send a message to a specific worker."""
        worker = self._workers.get(worker_id)
        if not worker:
            return False

        try:
            await worker.websocket.send_json(message)
            return True
        except Exception as e:
            logger.error(f"Failed to send message to worker {worker_id}: {e}")
            await self.disconnect(worker_id)
            return False

    async def wait_for_available_worker(self, timeout: float = 30.0) -> bool:
        """
        Wait for a worker to become available.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if a worker is available, False if timeout
        """
        try:
            await asyncio.wait_for(self._worker_available.wait(), timeout=timeout)
            self._worker_available.clear()
            return True
        except asyncio.TimeoutError:
            return False

    @property
    def connected_count(self) -> int:
        """Number of connected workers."""
        return len(self._workers)

    @property
    def busy_count(self) -> int:
        """Number of busy workers."""
        return sum(1 for w in self._workers.values() if w.is_busy)

    @property
    def available_count(self) -> int:
        """Number of available workers."""
        return sum(1 for w in self._workers.values() if not w.is_busy)


@dataclass
class StreamingClient:
    """Tracks a client connected to receive streaming results."""
    invocation_id: int
    websocket: WebSocket
    connected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class StreamingConnectionManager:
    """
    Manages WebSocket connections from clients waiting for streaming results.

    When a worker sends stream chunks, they are forwarded to connected clients.
    """

    def __init__(self):
        # Map invocation_id -> list of connected clients
        self._clients: dict[int, list[StreamingClient]] = {}
        self._lock = asyncio.Lock()

    async def connect(
        self,
        websocket: WebSocket,
        invocation_id: int,
    ) -> StreamingClient:
        """Register a client to receive streaming results."""
        await websocket.accept()

        async with self._lock:
            client = StreamingClient(
                invocation_id=invocation_id,
                websocket=websocket,
            )
            if invocation_id not in self._clients:
                self._clients[invocation_id] = []
            self._clients[invocation_id].append(client)
            logger.info(f"Client connected for streaming invocation {invocation_id}")

        return client

    async def disconnect(self, invocation_id: int, client: StreamingClient):
        """Handle client disconnection."""
        async with self._lock:
            if invocation_id in self._clients:
                self._clients[invocation_id] = [
                    c for c in self._clients[invocation_id] if c != client
                ]
                if not self._clients[invocation_id]:
                    del self._clients[invocation_id]
                logger.info(f"Client disconnected from streaming invocation {invocation_id}")

    async def send_chunk(
        self,
        invocation_id: int,
        sequence: int,
        pickled_value: str,  # base64 encoded
    ):
        """Send a stream chunk to all clients waiting for this invocation."""
        async with self._lock:
            clients = self._clients.get(invocation_id, [])

        disconnected = []
        for client in clients:
            try:
                await client.websocket.send_json({
                    "type": "chunk",
                    "sequence": sequence,
                    "pickled_value": pickled_value,
                })
            except Exception:
                disconnected.append(client)

        # Clean up disconnected clients
        for client in disconnected:
            await self.disconnect(invocation_id, client)

    async def send_complete(self, invocation_id: int, total_chunks: int):
        """Notify clients that streaming is complete."""
        async with self._lock:
            clients = self._clients.get(invocation_id, [])

        for client in clients:
            try:
                await client.websocket.send_json({
                    "type": "complete",
                    "total_chunks": total_chunks,
                })
            except Exception:
                pass

        # Clean up all clients for this invocation
        async with self._lock:
            if invocation_id in self._clients:
                del self._clients[invocation_id]

    async def send_error(self, invocation_id: int, error: str):
        """Notify clients of a streaming error."""
        async with self._lock:
            clients = self._clients.get(invocation_id, [])

        for client in clients:
            try:
                await client.websocket.send_json({
                    "type": "error",
                    "error": error,
                })
            except Exception:
                pass

        # Clean up all clients for this invocation
        async with self._lock:
            if invocation_id in self._clients:
                del self._clients[invocation_id]

    def has_clients(self, invocation_id: int) -> bool:
        """Check if there are clients waiting for this invocation."""
        return invocation_id in self._clients and len(self._clients[invocation_id]) > 0


@dataclass
class ResultClient:
    """Tracks a client connected to receive a single result."""
    invocation_id: int
    websocket: WebSocket
    connected_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class ResultConnectionManager:
    """
    Manages WebSocket connections from clients waiting for function results.

    Instead of polling, clients connect via WebSocket and receive a push
    notification when the result is ready.
    """

    def __init__(self):
        # Map invocation_id -> list of connected clients
        self._clients: dict[int, list[ResultClient]] = {}
        self._lock = asyncio.Lock()

    async def connect(
        self,
        websocket: WebSocket,
        invocation_id: int,
    ) -> ResultClient:
        """Register a client to receive result notification."""
        await websocket.accept()

        async with self._lock:
            client = ResultClient(
                invocation_id=invocation_id,
                websocket=websocket,
            )
            if invocation_id not in self._clients:
                self._clients[invocation_id] = []
            self._clients[invocation_id].append(client)
            logger.debug(f"Client connected for result {invocation_id}")

        return client

    async def disconnect(self, invocation_id: int, client: ResultClient):
        """Handle client disconnection."""
        async with self._lock:
            if invocation_id in self._clients:
                self._clients[invocation_id] = [
                    c for c in self._clients[invocation_id] if c != client
                ]
                if not self._clients[invocation_id]:
                    del self._clients[invocation_id]
                logger.debug(f"Client disconnected from result {invocation_id}")

    async def send_result(
        self,
        invocation_id: int,
        pickled_result: str | None,  # base64 encoded
    ):
        """Send result to all clients waiting for this invocation."""
        async with self._lock:
            clients = self._clients.get(invocation_id, [])

        for client in clients:
            try:
                await client.websocket.send_json({
                    "type": "result",
                    "invocation_id": invocation_id,
                    "pickled_result": pickled_result,
                })
            except Exception:
                pass

        # Clean up all clients for this invocation
        async with self._lock:
            if invocation_id in self._clients:
                del self._clients[invocation_id]

    async def send_error(self, invocation_id: int, error: str):
        """Notify clients of an error."""
        async with self._lock:
            clients = self._clients.get(invocation_id, [])

        for client in clients:
            try:
                await client.websocket.send_json({
                    "type": "error",
                    "invocation_id": invocation_id,
                    "error": error,
                })
            except Exception:
                pass

        # Clean up all clients for this invocation
        async with self._lock:
            if invocation_id in self._clients:
                del self._clients[invocation_id]

    def has_clients(self, invocation_id: int) -> bool:
        """Check if there are clients waiting for this invocation."""
        return invocation_id in self._clients and len(self._clients[invocation_id]) > 0


# Global connection manager instances
manager = WorkerConnectionManager()
streaming_manager = StreamingConnectionManager()
result_manager = ResultConnectionManager()
