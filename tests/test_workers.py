"""Tests for worker registration, scheduling, and execution."""

import pytest
import httpx


class TestWorkerRegistration:
    """Test worker registration with control plane."""

    def test_worker_appears_in_stats(self, server_process, worker_process, server_url):
        """Test that registered worker appears in stats."""
        r = httpx.get(f"{server_url}/stats/workers")
        workers = r.json().get("workers", [])
        assert len(workers) >= 1
        assert any(w["status"] in ["idle", "busy"] for w in workers)

    def test_worker_health_endpoint(self, server_process, worker_process, server_url):
        """Test /workers/health endpoint."""
        r = httpx.get(f"{server_url}/workers/health")
        assert r.status_code == 200
        data = r.json()
        assert "healthy" in data or "summary" in data

    def test_worker_has_resources(self, server_process, worker_process, server_url):
        """Test that worker reports CPU and memory."""
        r = httpx.get(f"{server_url}/stats/workers")
        workers = r.json().get("workers", [])
        assert len(workers) >= 1
        worker = workers[0]
        assert "cpu_cores" in worker or "cpu" in worker
        assert "memory_mb" in worker or "memory" in worker


class TestTaskScheduling:
    """Test that tasks get scheduled to workers."""

    def test_task_assigned_to_worker(
        self, server_process, worker_process, client, server_url
    ):
        """Test that task is assigned to and processed by worker."""
        from minimodal import App

        app = App("test-scheduling")

        @app.function()
        def scheduled_task():
            return "scheduled"

        app.deploy()
        result = scheduled_task.remote()

        assert result == "scheduled"

        # Verify worker processed it
        r = httpx.get(f"{server_url}/stats/workers")
        workers = r.json().get("workers", [])
        total_completed = sum(w.get("tasks_completed", 0) for w in workers)
        assert total_completed >= 1

    def test_multiple_tasks_distributed(
        self, server_process, worker_process, client
    ):
        """Test multiple tasks are processed."""
        from minimodal import App, gather

        app = App("test-distribution")

        @app.function()
        def work(n):
            return n * 2

        app.deploy()

        futures = work.map(range(10))
        results = gather(*futures)

        assert results == [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

    def test_task_with_resource_requirements(
        self, server_process, worker_process, client
    ):
        """Test task with specific resource requirements."""
        from minimodal import App

        app = App("test-resources")

        @app.function(cpu=1, memory=256)
        def lightweight_task():
            return "light"

        app.deploy()
        result = lightweight_task.remote()
        assert result == "light"


class TestWorkerExecution:
    """Test actual code execution on workers."""

    def test_worker_executes_code(self, server_process, worker_process, client):
        """Test worker executes code correctly."""
        from minimodal import App

        app = App("test-execution")

        @app.function()
        def compute():
            result = sum(range(100))
            return result

        app.deploy()

        result = compute.remote()
        assert result == 4950

    def test_worker_handles_imports(self, server_process, worker_process, client):
        """Test worker handles standard library imports."""
        from minimodal import App

        app = App("test-imports")

        @app.function()
        def use_stdlib():
            import json
            import os

            return json.dumps({"pid": os.getpid()})

        app.deploy()

        result = use_stdlib.remote()
        assert "pid" in result

    def test_worker_handles_complex_data(self, server_process, worker_process, client):
        """Test worker handles complex data structures."""
        from minimodal import App

        app = App("test-complex-data")

        @app.function()
        def complex_computation():
            data = {
                "numbers": list(range(100)),
                "nested": {"a": {"b": {"c": 123}}},
                "strings": ["hello", "world"],
            }
            return data

        app.deploy()

        result = complex_computation.remote()
        assert result["numbers"] == list(range(100))
        assert result["nested"]["a"]["b"]["c"] == 123
        assert result["strings"] == ["hello", "world"]

    def test_worker_handles_exceptions(self, server_process, worker_process, client):
        """Test worker properly reports exceptions."""
        from minimodal import App
        from minimodal.sdk.client import RemoteError

        app = App("test-exceptions")

        @app.function()
        def failing_func():
            raise RuntimeError("intentional failure")

        app.deploy()

        with pytest.raises(RemoteError):
            failing_func.remote()

    def test_worker_handles_large_return(self, server_process, worker_process, client):
        """Test worker handles large return values."""
        from minimodal import App

        app = App("test-large-return")

        @app.function()
        def large_data():
            return list(range(10000))

        app.deploy()

        result = large_data.remote()
        assert len(result) == 10000
        assert result[0] == 0
        assert result[-1] == 9999


class TestWorkerConcurrency:
    """Test worker handling of concurrent tasks."""

    def test_sequential_tasks(self, server_process, worker_process, client):
        """Test sequential task execution."""
        from minimodal import App

        app = App("test-sequential")

        @app.function()
        def seq_task(n):
            return n

        app.deploy()

        results = []
        for i in range(5):
            results.append(seq_task.remote(i))

        assert results == [0, 1, 2, 3, 4]

    def test_parallel_spawn(self, server_process, worker_process, client):
        """Test parallel task spawning."""
        from minimodal import App, gather

        app = App("test-parallel")

        @app.function()
        def parallel_task(n):
            return n * n

        app.deploy()

        futures = [parallel_task.spawn(i) for i in range(5)]
        results = gather(*futures)

        assert results == [0, 1, 4, 9, 16]


class TestWorkerStatus:
    """Test worker status tracking."""

    def test_worker_status_updates(
        self, server_process, worker_process, client, server_url
    ):
        """Test that worker status updates during execution."""
        from minimodal import App

        app = App("test-status")

        @app.function()
        def status_task():
            import time

            time.sleep(0.1)
            return "done"

        app.deploy()

        # Execute task
        status_task.remote()

        # Worker should be idle after completion
        r = httpx.get(f"{server_url}/stats/workers")
        workers = r.json().get("workers", [])
        assert any(w["status"] == "idle" for w in workers)


class TestWorkerResourceValidation:
    """Test worker resource validation and enforcement."""

    def test_worker_validates_resources_on_startup(self):
        """Test that worker validates resources against system limits."""
        from minimodal.worker.executor import Worker

        # Create worker with absurdly high resources
        worker = Worker(
            control_plane_url="http://localhost:8000",
            cpu_cores=10000,  # No system has this many
            memory_mb=100000000,  # 100TB, unlikely
        )

        # Worker should cap resources to actual system limits
        import psutil
        system_cpu = psutil.cpu_count()
        system_memory_mb = psutil.virtual_memory().total // (1024 * 1024)

        assert worker.cpu_cores <= system_cpu
        assert worker.memory_mb <= system_memory_mb

    def test_worker_accepts_valid_resources(self):
        """Test that worker accepts valid resource values."""
        from minimodal.worker.executor import Worker

        # Create worker with minimal valid resources
        worker = Worker(
            control_plane_url="http://localhost:8000",
            cpu_cores=1,
            memory_mb=512,
        )

        assert worker.cpu_cores == 1
        assert worker.memory_mb == 512

    def test_resource_requirements_in_task_data(
        self, server_process, worker_process, client, server_url
    ):
        """Test that resource requirements are passed to worker in task data."""
        from minimodal import App

        app = App("test-resource-task")

        @app.function(cpu=2, memory=1024)
        def resource_task():
            return "executed"

        app.deploy()
        result = resource_task.remote()

        assert result == "executed"

        # Verify the function was stored with correct requirements
        r = httpx.get(f"{server_url}/stats/functions")
        functions = r.json().get("functions", [])
        func = next((f for f in functions if f["name"] == "resource_task"), None)
        if func:
            assert func.get("required_cpu", func.get("cpu")) == 2
            assert func.get("required_memory", func.get("memory")) == 1024
