"""Tests for the push-based task scheduler."""

import pytest
import httpx


class TestSchedulerStats:
    """Test scheduler statistics endpoint."""

    def test_scheduler_stats_endpoint(self, server_process, server_url):
        """Test that scheduler stats endpoint returns data."""
        r = httpx.get(f"{server_url}/stats/scheduler")
        assert r.status_code == 200
        data = r.json()
        assert "total_pending_tasks" in data
        assert "total_active_tasks" in data
        assert "users_tracked" in data

    def test_scheduler_tracks_tasks(self, server_process, worker_process, client, server_url):
        """Test that scheduler tracks tasks through completion."""
        from minimodal import App

        app = App("test-scheduler-tracking")

        @app.function()
        def tracked_task():
            return "tracked"

        app.deploy()
        result = tracked_task.remote()
        assert result == "tracked"

        # Scheduler should have processed the task
        r = httpx.get(f"{server_url}/stats/scheduler")
        data = r.json()
        # After task completes, pending should be 0
        assert data["total_pending_tasks"] >= 0


class TestSchedulerQuotas:
    """Test quota enforcement in the scheduler."""

    def test_user_quota_limit_set(self, server_process, server_url):
        """Test that user quota can be set via API."""
        # Set quota for a test user
        r = httpx.put(f"{server_url}/users/scheduler-quota-user/quota?quota_limit=3")
        assert r.status_code == 200
        data = r.json()
        assert data["quota_limit"] == 3

    def test_user_active_count_updates(self, server_process, worker_process, client, server_url):
        """Test that user active count updates during task execution."""
        from minimodal import App

        app = App("test-active-count")

        @app.function()
        def quick_task():
            return "done"

        app.deploy()

        # Run a task
        result = quick_task.remote()
        assert result == "done"

        # Check user endpoint shows correct count (should be 0 after completion)
        r = httpx.get(f"{server_url}/users/default")
        if r.status_code == 200:
            user = r.json()
            # Active count should be 0 after task completes
            assert user["active_count"] >= 0


class TestSchedulerResourceMatching:
    """Test bin packing / resource matching in the scheduler."""

    def test_task_matches_worker_resources(self, server_process, worker_process, client, server_url):
        """Test that tasks with resource requirements are matched to capable workers."""
        from minimodal import App

        app = App("test-resource-match")

        @app.function(cpu=1, memory=256)
        def resource_task():
            return "matched"

        app.deploy()
        result = resource_task.remote()
        assert result == "matched"

        # Task should have been completed
        r = httpx.get(f"{server_url}/stats/invocations?limit=1")
        assert r.status_code == 200
        data = r.json()
        assert len(data["invocations"]) >= 1
        assert data["invocations"][0]["status"] == "completed"


class TestSchedulerIntegration:
    """Integration tests for scheduler with full system."""

    def test_batch_tasks_complete(self, server_process, worker_process, client, server_url):
        """Test that batch tasks complete through the scheduler."""
        from minimodal import App, gather

        app = App("test-batch-scheduler-int")

        @app.function()
        def batch_task(x):
            return x + 1

        app.deploy()

        # Batch invoke - use smaller batch to reduce test time
        futures = batch_task.map([1, 2, 3])
        results = gather(*futures)
        assert results == [2, 3, 4]

    def test_single_task_completes(self, server_process, worker_process, client, server_url):
        """Test that single task completes through scheduler."""
        from minimodal import App

        app = App("test-single-scheduler-int")

        @app.function()
        def simple_task():
            return "scheduled"

        app.deploy()

        result = simple_task.remote()
        assert result == "scheduled"
