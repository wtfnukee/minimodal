"""Tests for the autoscaler."""

import asyncio
from unittest.mock import Mock
from datetime import datetime, timezone, timedelta

from minimodal.server.autoscaler import Autoscaler


class MockScheduler:
    """Mock scheduler for testing."""

    def __init__(self, pending=0, active=0):
        self._pending = pending
        self._active = active

    def get_stats(self):
        return {
            "total_pending_tasks": self._pending,
            "total_active_tasks": self._active,
        }


class MockWSManager:
    """Mock WebSocket manager for testing."""

    def __init__(self, connected=0, available=0):
        self._connected = connected
        self._available = available

    @property
    def connected_count(self):
        return self._connected

    @property
    def available_count(self):
        return self._available


class MockContainerManager:
    """Mock container manager for testing."""

    def __init__(self):
        self.containers = []
        self.start_calls = []
        self.stop_calls = []

    def start_container(self, image_tag, environment=None):
        container_id = f"container-{len(self.containers)}"
        self.containers.append(
            Mock(
                container_id=container_id,
                started_at=datetime.now(timezone.utc),
            )
        )
        self.start_calls.append({"image_tag": image_tag, "environment": environment})
        return container_id

    def stop_container(self, container_id):
        self.stop_calls.append(container_id)
        self.containers = [c for c in self.containers if c.container_id != container_id]
        return True

    def list_containers(self):
        return self.containers


class TestAutoscalerScaleUp:
    """Test autoscaler scale up behavior."""

    def test_scale_up_when_pending_tasks(self):
        """Test that autoscaler scales up when there are pending tasks."""
        scheduler = MockScheduler(pending=3, active=0)
        ws_manager = MockWSManager(connected=0, available=0)
        container_manager = MockContainerManager()

        autoscaler = Autoscaler(
            scheduler=scheduler,
            ws_manager=ws_manager,
            container_manager=container_manager,
            min_workers=0,
            max_workers=5,
            check_interval=0.1,
        )

        # Run one check cycle
        asyncio.run(autoscaler._check_and_scale())

        # Should have started containers for pending tasks
        assert len(container_manager.start_calls) == 3

    def test_scale_up_respects_max_workers(self):
        """Test that autoscaler doesn't exceed max_workers."""
        scheduler = MockScheduler(pending=10, active=0)
        ws_manager = MockWSManager(connected=3, available=0)
        container_manager = MockContainerManager()

        autoscaler = Autoscaler(
            scheduler=scheduler,
            ws_manager=ws_manager,
            container_manager=container_manager,
            min_workers=0,
            max_workers=5,
            check_interval=0.1,
        )

        asyncio.run(autoscaler._check_and_scale())

        # Should only start 2 more containers (5 max - 3 connected)
        assert len(container_manager.start_calls) == 2

    def test_no_scale_up_when_workers_available(self):
        """Test that autoscaler doesn't scale up when workers are available."""
        scheduler = MockScheduler(pending=3, active=0)
        ws_manager = MockWSManager(connected=5, available=3)
        container_manager = MockContainerManager()

        autoscaler = Autoscaler(
            scheduler=scheduler,
            ws_manager=ws_manager,
            container_manager=container_manager,
            min_workers=0,
            max_workers=10,
            check_interval=0.1,
        )

        asyncio.run(autoscaler._check_and_scale())

        # Should not start any containers since workers are available
        assert len(container_manager.start_calls) == 0


class TestAutoscalerScaleDown:
    """Test autoscaler scale down behavior."""

    def test_scale_down_after_idle_delay(self):
        """Test that autoscaler scales down after idle period."""
        scheduler = MockScheduler(pending=0, active=0)
        ws_manager = MockWSManager(connected=3, available=3)
        container_manager = MockContainerManager()

        # Add some containers
        container_manager.start_container("test:latest")
        container_manager.start_container("test:latest")

        autoscaler = Autoscaler(
            scheduler=scheduler,
            ws_manager=ws_manager,
            container_manager=container_manager,
            min_workers=0,
            max_workers=10,
            scale_down_delay=0.0,  # Immediate scale down
            check_interval=0.1,
        )

        # Set last activity to past
        autoscaler._last_activity = datetime.now(timezone.utc) - timedelta(seconds=120)

        asyncio.run(autoscaler._check_and_scale())

        # Should have stopped one container
        assert len(container_manager.stop_calls) == 1

    def test_scale_down_respects_min_workers(self):
        """Test that autoscaler doesn't go below min_workers."""
        scheduler = MockScheduler(pending=0, active=0)
        ws_manager = MockWSManager(connected=2, available=2)
        container_manager = MockContainerManager()

        # Add some containers
        container_manager.start_container("test:latest")
        container_manager.start_container("test:latest")

        autoscaler = Autoscaler(
            scheduler=scheduler,
            ws_manager=ws_manager,
            container_manager=container_manager,
            min_workers=2,
            max_workers=10,
            scale_down_delay=0.0,
            check_interval=0.1,
        )

        # Set last activity to past
        autoscaler._last_activity = datetime.now(timezone.utc) - timedelta(seconds=120)

        asyncio.run(autoscaler._check_and_scale())

        # Should not stop any containers since we're at min_workers
        assert len(container_manager.stop_calls) == 0

    def test_no_scale_down_when_work_pending(self):
        """Test that autoscaler doesn't scale down when work is pending."""
        scheduler = MockScheduler(pending=1, active=0)
        ws_manager = MockWSManager(connected=3, available=3)
        container_manager = MockContainerManager()

        autoscaler = Autoscaler(
            scheduler=scheduler,
            ws_manager=ws_manager,
            container_manager=container_manager,
            min_workers=0,
            max_workers=10,
            scale_down_delay=0.0,
            check_interval=0.1,
        )

        # Set last activity to past
        autoscaler._last_activity = datetime.now(timezone.utc) - timedelta(seconds=120)

        asyncio.run(autoscaler._check_and_scale())

        # Should not stop any containers since work is pending
        assert len(container_manager.stop_calls) == 0


class TestAutoscalerStats:
    """Test autoscaler statistics."""

    def test_stats_include_config(self):
        """Test that stats include configuration values."""
        scheduler = MockScheduler()
        ws_manager = MockWSManager()
        container_manager = MockContainerManager()

        autoscaler = Autoscaler(
            scheduler=scheduler,
            ws_manager=ws_manager,
            container_manager=container_manager,
            min_workers=1,
            max_workers=8,
            scale_down_delay=30.0,
        )

        stats = autoscaler.get_stats()

        assert stats["min_workers"] == 1
        assert stats["max_workers"] == 8
        assert stats["scale_down_delay"] == 30.0
        assert stats["scale_up_count"] == 0
        assert stats["scale_down_count"] == 0

    def test_stats_track_scaling_events(self):
        """Test that stats track scaling events."""
        scheduler = MockScheduler(pending=2, active=0)
        ws_manager = MockWSManager(connected=0, available=0)
        container_manager = MockContainerManager()

        autoscaler = Autoscaler(
            scheduler=scheduler,
            ws_manager=ws_manager,
            container_manager=container_manager,
            min_workers=0,
            max_workers=10,
        )

        asyncio.run(autoscaler._check_and_scale())

        stats = autoscaler.get_stats()
        assert stats["scale_up_count"] == 2


class TestAutoscalerLifecycle:
    """Test autoscaler start/stop lifecycle."""

    def test_start_and_stop(self):
        """Test that autoscaler can start and stop cleanly."""
        scheduler = MockScheduler()
        ws_manager = MockWSManager()
        container_manager = MockContainerManager()

        autoscaler = Autoscaler(
            scheduler=scheduler,
            ws_manager=ws_manager,
            container_manager=container_manager,
            check_interval=0.01,
        )

        async def run_test():
            await autoscaler.start()
            assert autoscaler._running is True

            await asyncio.sleep(0.05)  # Let it run a few cycles

            await autoscaler.stop()
            assert autoscaler._running is False

        asyncio.run(run_test())

    def test_callbacks_called(self):
        """Test that scaling callbacks are called."""
        scheduler = MockScheduler(pending=2, active=0)
        ws_manager = MockWSManager(connected=0, available=0)
        container_manager = MockContainerManager()

        autoscaler = Autoscaler(
            scheduler=scheduler,
            ws_manager=ws_manager,
            container_manager=container_manager,
        )

        scale_up_calls = []
        autoscaler.set_callbacks(on_scale_up=lambda n: scale_up_calls.append(n))

        asyncio.run(autoscaler._check_and_scale())

        assert scale_up_calls == [2]
