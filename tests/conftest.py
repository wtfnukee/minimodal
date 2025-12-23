"""Pytest fixtures for minimodal tests."""

import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Generator

import pytest
import httpx


# Use a unique port for each test run to avoid conflicts
_test_port = 8765


@pytest.fixture(scope="module")
def temp_dir() -> Generator[Path, None, None]:
    """Create temp directory for test artifacts."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture(scope="module")
def temp_db(temp_dir: Path) -> Path:
    """Create temp database file."""
    return temp_dir / "test.db"


@pytest.fixture(scope="module")
def volume_path(temp_dir: Path) -> Path:
    """Create temp volume storage."""
    path = temp_dir / "volumes"
    path.mkdir()
    return path


@pytest.fixture(scope="module")
def server_port(request) -> int:
    """Use a unique port per module."""
    # Hash the module name to get a unique port offset
    module_hash = hash(request.module.__name__) % 100
    return 8700 + module_hash


@pytest.fixture(scope="module")
def server_url(server_port: int) -> str:
    """Return the test server URL."""
    return f"http://localhost:{server_port}"


@pytest.fixture(scope="module")
def server_process(
    temp_db: Path, volume_path: Path, server_port: int, server_url: str
) -> Generator[subprocess.Popen, None, None]:
    """Start the minimodal server for testing."""
    env = os.environ.copy()
    env["MINIMODAL_DATABASE_URL"] = f"sqlite:///{temp_db}"
    env["MINIMODAL_VOLUME_PATH"] = str(volume_path)
    env["MINIMODAL_PORT"] = str(server_port)

    # Start server
    proc = subprocess.Popen(
        [sys.executable, "-m", "minimodal.server.api", "--port", str(server_port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for server to be ready
    max_retries = 50
    for _ in range(max_retries):
        try:
            response = httpx.get(f"{server_url}/ready", timeout=1.0)
            if response.status_code == 200:
                break
        except Exception:
            pass
        time.sleep(0.1)
    else:
        proc.terminate()
        raise RuntimeError("Server failed to start")

    yield proc

    # Cleanup
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@pytest.fixture(scope="module")
def worker_process(
    server_url: str, server_process: subprocess.Popen, volume_path: Path
) -> Generator[subprocess.Popen, None, None]:
    """Start a minimodal worker for testing."""
    env = os.environ.copy()
    env["MINIMODAL_VOLUME_PATH"] = str(volume_path)

    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "minimodal.worker.executor",
            "--url",
            server_url,
            "--cpu",
            "4",
            "--memory",
            "4096",
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Give worker time to connect via WebSocket
    # Need enough time for: Python startup + wait_for_server + WebSocket connect
    time.sleep(4)

    yield proc

    # Cleanup
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


@pytest.fixture
def client(server_url: str):
    """Get SDK client configured for test server."""
    from minimodal import get_client, reset_client

    reset_client()
    os.environ["MINIMODAL_SERVER_URL"] = server_url
    yield get_client(server_url)
    reset_client()
