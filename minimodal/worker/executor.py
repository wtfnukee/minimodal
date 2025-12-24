"""
Worker executor — receives tasks via WebSocket push.

The server pushes tasks directly to workers via WebSocket connection.
This eliminates polling overhead and enables instant task distribution.
"""

import asyncio
import base64
import inspect
import json
import logging
import os
import signal
import subprocess
import tempfile
import traceback
import uuid
from pathlib import Path
from typing import Callable

import cloudpickle
import httpx
import websockets
from websockets.exceptions import ConnectionClosed

# === Logging Setup ===

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("minimodal.worker")


class Worker:
    """
    Worker that receives tasks via WebSocket push.

    Maintains a persistent WebSocket connection to the control plane
    and receives tasks as they become available.

    Lifecycle:
        1. Connect to control plane via WebSocket
        2. Wait for task push from server
        3. Execute task
        4. Send result back via WebSocket
        5. Repeat

    Graceful Shutdown:
        - On SIGTERM/SIGINT, finish current task then disconnect
        - Won't accept new tasks during shutdown
    """

    def __init__(
        self,
        control_plane_url: str = "http://localhost:8000",
        worker_id: str | None = None,
        cpu_cores: int = 1,
        memory_mb: int = 1024,
    ):
        self.control_plane_url = control_plane_url
        self.worker_id = worker_id or self._generate_worker_id()
        # Store requested values for validation
        self._requested_cpu = cpu_cores
        self._requested_memory = memory_mb
        self.cpu_cores = cpu_cores
        self.memory_mb = memory_mb
        self._http_client = httpx.AsyncClient(timeout=30.0)
        self._running = False
        self._shutting_down = False
        self._current_task: int | None = None
        self._ws: websockets.WebSocketClientProtocol | None = None

        # Validate and cap resources to actual system limits
        self.cpu_cores, self.memory_mb = self._validate_resources()

    def _generate_worker_id(self) -> str:
        return f"worker-{uuid.uuid4().hex[:8]}"

    def _validate_resources(self) -> tuple[int, int]:
        """
        Validate and cap worker resources to actual system limits.

        Returns:
            (actual_cpu, actual_memory_mb) - the validated resource values
        """
        try:
            import psutil

            system_cpu = psutil.cpu_count()
            system_memory_mb = psutil.virtual_memory().total // (1024 * 1024)

            actual_cpu = min(self.cpu_cores, system_cpu)
            actual_memory_mb = min(self.memory_mb, system_memory_mb)

            if self.cpu_cores > system_cpu:
                logger.warning(
                    f"Requested {self.cpu_cores} CPU cores, but system only has {system_cpu}. "
                    f"Capping to {actual_cpu}."
                )
            if self.memory_mb > system_memory_mb:
                logger.warning(
                    f"Requested {self.memory_mb}MB memory, but system only has {system_memory_mb}MB. "
                    f"Capping to {actual_memory_mb}MB."
                )

            return actual_cpu, actual_memory_mb

        except ImportError:
            logger.warning(
                "psutil not installed - cannot validate system resources. "
                "Run: pip install psutil"
            )
            return self.cpu_cores, self.memory_mb

    def _get_ws_url(self) -> str:
        """Convert HTTP URL to WebSocket URL."""
        url = self.control_plane_url
        if url.startswith("https://"):
            url = "wss://" + url[8:]
        elif url.startswith("http://"):
            url = "ws://" + url[7:]
        return f"{url}/ws/worker/{self.worker_id}?cpu_cores={self.cpu_cores}&memory_mb={self.memory_mb}"

    async def wait_for_server(self, timeout: float = 30.0) -> bool:
        """Wait for the control plane server to be ready."""
        logger.info(f"Waiting for control plane at {self.control_plane_url}")
        start = asyncio.get_event_loop().time()

        while asyncio.get_event_loop().time() - start < timeout:
            try:
                response = await self._http_client.get(
                    f"{self.control_plane_url}/health"
                )
                if response.status_code == 200:
                    logger.info("Control plane is ready")
                    return True
            except Exception:
                pass
            await asyncio.sleep(0.5)

        logger.error(f"Control plane not available after {timeout}s")
        return False

    async def _get_secret_value(self, secret_name: str) -> str | None:
        """Fetch secret value from control plane."""
        try:
            response = await self._http_client.get(
                f"{self.control_plane_url}/secrets/internal/{secret_name}"
            )
            response.raise_for_status()
            return response.json().get("value")
        except Exception as e:
            logger.warning(f"Failed to get secret {secret_name}: {e}")
            return None

    def _docker_available(self) -> bool:
        """Check if Docker is available."""
        try:
            result = subprocess.run(
                ["docker", "info"],
                capture_output=True,
                timeout=5,
            )
            return result.returncode == 0
        except Exception:
            return False

    async def execute(self, task: dict) -> tuple[bytes | None, str | None, bool]:
        """
        Execute a task, optionally in a container if image_tag is provided.

        Args:
            task: Dict with pickled_code, pickled_args, and optional image_tag

        Returns:
            (pickled_result, error_message, is_generator) - result/error will be None for generators
        """
        image_tag = task.get("image_tag")
        if image_tag and self._docker_available():
            return await self._execute_in_container(task, image_tag)
        return await self._execute_local(task)

    async def _execute_local(
        self, task: dict
    ) -> tuple[bytes | None, str | None, bool]:
        """Execute task locally (without container).

        WARNING: Local execution does NOT enforce CPU/memory limits.
        For resource enforcement, use Docker (set image_tag on function).

        Returns:
            (pickled_result, error_message, is_generator)
            For generators, result/error are None - streaming is handled separately.
        """
        required_cpu = task.get("required_cpu", 1)
        required_memory = task.get("required_memory", 512)
        if required_cpu > 1 or required_memory > 512:
            logger.warning(
                f"Local execution: resource limits (cpu={required_cpu}, memory={required_memory}MB) "
                "are NOT enforced. Use Docker for resource isolation."
            )

        # Inject secrets as environment variables
        secret_names = task.get("secrets", [])
        original_env = {}
        for secret_name in secret_names:
            secret_value = await self._get_secret_value(secret_name)
            if secret_value:
                original_env[secret_name] = os.environ.get(secret_name)
                os.environ[secret_name] = secret_value
                logger.info(f"Injected secret: {secret_name}")

        try:
            # Deserialize function
            code_bytes = base64.b64decode(task["pickled_code"])
            func = cloudpickle.loads(code_bytes)

            # Check if function is a generator
            is_generator = inspect.isgeneratorfunction(func)

            # Deserialize args
            args_bytes = base64.b64decode(task["pickled_args"])
            args, kwargs = cloudpickle.loads(args_bytes)

            if is_generator:
                # Handle generator - stream results
                logger.info(
                    f"Executing generator locally: {func.__name__}({args}, {kwargs})"
                )
                await self._stream_generator_results(task["id"], func, args, kwargs)
                return None, None, True

            # Execute regular function in thread pool
            loop = asyncio.get_event_loop()
            logger.info(f"Executing locally: {func.__name__}({args}, {kwargs})")
            result = await loop.run_in_executor(None, lambda: func(*args, **kwargs))

            # Serialize result
            pickled_result = cloudpickle.dumps(result)
            return pickled_result, None, False

        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            logger.error(f"Execution error: {error_msg}")
            return None, error_msg, False

        finally:
            # Restore original environment
            for secret_name in secret_names:
                if original_env.get(secret_name) is None:
                    os.environ.pop(secret_name, None)
                else:
                    os.environ[secret_name] = original_env[secret_name]

    async def _stream_generator_results(
        self,
        invocation_id: int,
        func: Callable,
        args: tuple,
        kwargs: dict,
    ):
        """Execute a generator function and stream results via WebSocket."""
        # Notify server that streaming is starting
        await self._ws.send(
            json.dumps(
                {
                    "type": "stream_start",
                    "invocation_id": invocation_id,
                }
            )
        )

        try:
            # Execute generator in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            generator = await loop.run_in_executor(None, lambda: func(*args, **kwargs))

            sequence = 0
            for value in generator:
                # Serialize and send each yielded value
                pickled_value = cloudpickle.dumps(value)
                await self._ws.send(
                    json.dumps(
                        {
                            "type": "stream_chunk",
                            "invocation_id": invocation_id,
                            "sequence": sequence,
                            "pickled_value": base64.b64encode(pickled_value).decode(),
                        }
                    )
                )
                logger.debug(
                    f"Streamed chunk {sequence} for invocation {invocation_id}"
                )
                sequence += 1

            # Notify server that streaming is complete
            await self._ws.send(
                json.dumps(
                    {
                        "type": "stream_end",
                        "invocation_id": invocation_id,
                        "total_chunks": sequence,
                    }
                )
            )
            logger.info(f"Streamed {sequence} chunks for invocation {invocation_id}")

        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            logger.error(f"Generator streaming error: {error_msg}")
            await self._ws.send(
                json.dumps(
                    {
                        "type": "stream_error",
                        "invocation_id": invocation_id,
                        "error": error_msg,
                    }
                )
            )

    async def _execute_in_container(
        self, task: dict, image_tag: str
    ) -> tuple[bytes | None, str | None, bool]:
        """Execute task in a Docker container with volume mounts, secrets, and resource limits.

        Supports both regular functions and generator functions with streaming.
        For generators, chunks are streamed via WebSocket as they're produced.
        """
        logger.info(f"Executing in container: {image_tag}")
        invocation_id = task.get("id")
        timeout_seconds = task.get("timeout_seconds", 300)
        required_cpu = task.get("required_cpu", 1)
        required_memory = task.get("required_memory", 512)

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                tmppath = Path(tmpdir)

                # Write pickled code and args to files
                code_file = tmppath / "code.pkl"
                args_file = tmppath / "args.pkl"
                result_file = tmppath / "result.pkl"
                error_file = tmppath / "error.txt"

                code_file.write_bytes(base64.b64decode(task["pickled_code"]))
                args_file.write_bytes(base64.b64decode(task["pickled_args"]))

                # Python script to execute inside container
                # Supports both regular functions and generators with streaming
                script = """
import cloudpickle
import traceback
import sys
import json
import base64
import inspect

def is_generator(obj):
    return inspect.isgenerator(obj) or inspect.isgeneratorfunction(obj)

try:
    with open("/work/code.pkl", "rb") as f:
        func = cloudpickle.loads(f.read())
    with open("/work/args.pkl", "rb") as f:
        args, kwargs = cloudpickle.loads(f.read())

    result = func(*args, **kwargs)

    # Check if result is a generator
    if inspect.isgenerator(result):
        # Stream each value as JSON line to stdout
        print(json.dumps({"type": "stream_start"}), flush=True)
        sequence = 0
        for value in result:
            pickled = base64.b64encode(cloudpickle.dumps(value)).decode()
            print(json.dumps({
                "type": "chunk",
                "sequence": sequence,
                "pickled_value": pickled
            }), flush=True)
            sequence += 1
        print(json.dumps({"type": "stream_end", "total_chunks": sequence}), flush=True)
    else:
        # Regular result - write to file
        with open("/work/result.pkl", "wb") as f:
            f.write(cloudpickle.dumps(result))

except Exception as e:
    error_msg = f"{type(e).__name__}: {e}\\n{traceback.format_exc()}"
    # Write to both error file and stdout for streaming case
    with open("/work/error.txt", "w") as f:
        f.write(error_msg)
    print(json.dumps({"type": "error", "error": error_msg}), flush=True)
    sys.exit(1)
"""
                script_file = tmppath / "run.py"
                script_file.write_text(script)

                # Build docker command
                docker_cmd = ["docker", "run", "--rm"]

                # Add resource limits (enforced by Docker cgroups)
                docker_cmd.extend(["--cpus", str(required_cpu)])
                docker_cmd.extend(["--memory", f"{required_memory}m"])
                # Disable swap to enforce hard memory limit
                docker_cmd.extend(["--memory-swap", f"{required_memory}m"])
                logger.info(
                    f"Resource limits: cpu={required_cpu}, memory={required_memory}MB"
                )

                # Add volume mounts
                docker_cmd.extend(["-v", f"{tmppath}:/work"])

                # Mount user volumes
                volume_mounts = task.get("volume_mounts", {})
                volume_base = os.environ.get(
                    "MINIMODAL_VOLUME_PATH", "./minimodal_volumes"
                )
                for mount_path, volume_name in volume_mounts.items():
                    volume_path = Path(volume_base) / volume_name
                    volume_path.mkdir(parents=True, exist_ok=True)
                    docker_cmd.extend(["-v", f"{volume_path.absolute()}:{mount_path}"])
                    logger.info(f"Mounting volume {volume_name} at {mount_path}")

                # Inject secrets as environment variables
                secret_names = task.get("secrets", [])
                for secret_name in secret_names:
                    secret_value = await self._get_secret_value(secret_name)
                    if secret_value:
                        docker_cmd.extend(["-e", f"{secret_name}={secret_value}"])
                        logger.info(f"Injecting secret: {secret_name}")

                # Add image and command
                docker_cmd.extend([image_tag, "python", "/work/run.py"])

                # Run container and stream output
                is_generator = False
                total_chunks = 0

                process = await asyncio.create_subprocess_exec(
                    *docker_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )

                try:
                    # Read stdout line by line for streaming
                    while True:
                        try:
                            line = await asyncio.wait_for(
                                process.stdout.readline(), timeout=timeout_seconds
                            )
                        except asyncio.TimeoutError:
                            process.kill()
                            return (
                                None,
                                f"Container execution timed out ({timeout_seconds}s)",
                                False,
                            )

                        if not line:
                            break

                        line = line.decode().strip()
                        if not line:
                            continue

                        try:
                            msg = json.loads(line)
                            msg_type = msg.get("type")

                            if msg_type == "stream_start":
                                is_generator = True
                                # Notify server that streaming started
                                if self._ws:
                                    await self._ws.send(
                                        json.dumps(
                                            {
                                                "type": "stream_start",
                                                "invocation_id": invocation_id,
                                            }
                                        )
                                    )
                                logger.info(
                                    f"Container streaming started for invocation {invocation_id}"
                                )

                            elif msg_type == "chunk":
                                # Forward chunk to server
                                if self._ws:
                                    await self._ws.send(
                                        json.dumps(
                                            {
                                                "type": "stream_chunk",
                                                "invocation_id": invocation_id,
                                                "sequence": msg["sequence"],
                                                "pickled_value": msg["pickled_value"],
                                            }
                                        )
                                    )
                                total_chunks = msg["sequence"] + 1
                                logger.debug(
                                    f"Container chunk {msg['sequence']} for invocation {invocation_id}"
                                )

                            elif msg_type == "stream_end":
                                total_chunks = msg.get("total_chunks", total_chunks)
                                # Notify server that streaming ended
                                if self._ws:
                                    await self._ws.send(
                                        json.dumps(
                                            {
                                                "type": "stream_end",
                                                "invocation_id": invocation_id,
                                                "total_chunks": total_chunks,
                                            }
                                        )
                                    )
                                logger.info(
                                    f"Container streaming ended for invocation {invocation_id} ({total_chunks} chunks)"
                                )
                                return (
                                    None,
                                    None,
                                    True,
                                )  # is_generator=True, handled via streaming

                            elif msg_type == "error":
                                error_msg = msg.get("error", "Unknown error")
                                if is_generator and self._ws:
                                    await self._ws.send(
                                        json.dumps(
                                            {
                                                "type": "stream_error",
                                                "invocation_id": invocation_id,
                                                "error": error_msg,
                                            }
                                        )
                                    )
                                return None, error_msg, is_generator

                        except json.JSONDecodeError:
                            # Not JSON, might be regular output
                            logger.debug(f"Container output: {line}")
                            continue

                    # Wait for process to complete
                    await process.wait()

                except Exception:
                    process.kill()
                    raise

                # Check results for non-streaming case
                if result_file.exists():
                    return result_file.read_bytes(), None, False
                elif error_file.exists():
                    return None, error_file.read_text(), False
                else:
                    stderr = await process.stderr.read()
                    stderr_text = stderr.decode() if stderr else "Unknown error"
                    return None, f"Container execution failed: {stderr_text}", False

        except asyncio.TimeoutError:
            return None, f"Container execution timed out ({timeout_seconds}s)", False
        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            logger.error(f"Container execution error: {error_msg}")
            return None, error_msg, False

    async def handle_task(self, task: dict):
        """Handle a task received via WebSocket."""
        invocation_id = task["id"]
        self._current_task = invocation_id
        logger.info(f"Received task: invocation_id={invocation_id}")

        try:
            pickled_result, error, is_generator = await self.execute(task)

            # For generators, streaming was already handled in execute()
            if is_generator:
                logger.info(f"Task {invocation_id} completed (streaming)")
            elif error:
                # Send error response
                await self._ws.send(
                    json.dumps(
                        {
                            "type": "error",
                            "invocation_id": invocation_id,
                            "error": error,
                        }
                    )
                )
                logger.info(f"Task {invocation_id} failed")
            else:
                # Send result
                await self._ws.send(
                    json.dumps(
                        {
                            "type": "result",
                            "invocation_id": invocation_id,
                            "pickled_result": base64.b64encode(pickled_result).decode(),
                        }
                    )
                )
                logger.info(f"Task {invocation_id} completed")

        finally:
            self._current_task = None

            # If we were shutting down, stop now
            if self._shutting_down:
                logger.info("Shutdown: finished current task, disconnecting")
                self._running = False

    async def run(self):
        """Run the worker loop."""
        self._running = True
        logger.info(f"Starting worker {self.worker_id}")
        logger.info(f"Connecting to {self.control_plane_url}")
        logger.info(f"Resources: cpu={self.cpu_cores}, memory={self.memory_mb}MB")

        # Wait for server before connecting
        if not await self.wait_for_server():
            logger.error("Could not connect to control plane, exiting")
            return

        ws_url = self._get_ws_url()
        logger.info(f"WebSocket URL: {ws_url}")

        reconnect_delay = 1.0
        max_reconnect_delay = 30.0

        while self._running and not self._shutting_down:
            try:
                async with websockets.connect(ws_url) as ws:
                    self._ws = ws
                    reconnect_delay = 1.0  # Reset on successful connection
                    logger.info("Connected to control plane via WebSocket")

                    while self._running and not self._shutting_down:
                        try:
                            # Wait for messages from server
                            message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                            data = json.loads(message)
                            msg_type = data.get("type")

                            if msg_type == "task":
                                # Execute the task
                                task = data.get("task")
                                if task:
                                    await self.handle_task(task)

                            elif msg_type == "ping":
                                # Respond to keep-alive
                                await ws.send(json.dumps({"type": "pong"}))

                        except asyncio.TimeoutError:
                            # Send a pong to keep connection alive
                            try:
                                await ws.send(json.dumps({"type": "pong"}))
                            except Exception:
                                break

            except ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: {e}")
            except Exception as e:
                logger.error(f"WebSocket error: {e}")

            if self._running and not self._shutting_down:
                logger.info(f"Reconnecting in {reconnect_delay}s...")
                await asyncio.sleep(reconnect_delay)
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

        # Clean up
        await self._http_client.aclose()
        logger.info(f"Worker {self.worker_id} stopped")

    def stop(self):
        """Stop the worker."""
        logger.info("Initiating graceful shutdown...")
        self._shutting_down = True
        if not self._current_task:
            self._running = False
        else:
            logger.info(f"Finishing current task {self._current_task} before exit")


def main():
    """Entry point for the worker."""
    import argparse

    parser = argparse.ArgumentParser(description="MiniModal Worker")
    parser.add_argument(
        "--url",
        default=os.environ.get("MINIMODAL_SERVER_URL", "http://localhost:8000"),
        help="Control plane URL",
    )
    parser.add_argument(
        "--worker-id",
        default=os.environ.get("MINIMODAL_WORKER_ID"),
        help="Worker ID (auto-generated if not specified)",
    )
    parser.add_argument(
        "--cpu",
        type=int,
        default=int(os.environ.get("MINIMODAL_WORKER_CPU", "1")),
        help="Number of CPU cores available (default: 1)",
    )
    parser.add_argument(
        "--memory",
        type=int,
        default=int(os.environ.get("MINIMODAL_WORKER_MEMORY", "1024")),
        help="Memory available in MB (default: 1024)",
    )
    args = parser.parse_args()

    worker = Worker(
        control_plane_url=args.url,
        worker_id=args.worker_id,
        cpu_cores=args.cpu,
        memory_mb=args.memory,
    )

    # Handle shutdown signals
    loop = asyncio.new_event_loop()

    def handle_signal():
        worker.stop()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    try:
        loop.run_until_complete(worker.run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
