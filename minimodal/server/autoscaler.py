"""
Autoscaler for dynamic worker pool management.

Monitors queue depth and scales workers up/down based on demand.

Scaling policy:
- Scale UP: When pending tasks > available workers
- Scale DOWN: When idle for scale_down_delay seconds
- Respects min_workers and max_workers bounds
"""

import asyncio
import logging
from collections.abc import Callable
from datetime import datetime, timezone

logger = logging.getLogger("minimodal.autoscaler")


class Autoscaler:
    """
    Monitors queue depth and scales workers up/down.

    Works with:
    - TaskScheduler: Gets pending task count
    - WebSocketManager: Gets worker availability
    - ContainerManager: Starts/stops containers
    """

    def __init__(
        self,
        scheduler,
        ws_manager,
        container_manager,
        *,
        min_workers: int = 0,
        max_workers: int = 10,
        scale_up_threshold: int = 1,  # Pending tasks to trigger scale up
        scale_down_delay: float = 60.0,  # Seconds idle before scale down
        check_interval: float = 5.0,  # Seconds between checks
        default_image: str = "minimodal-worker:latest",
    ):
        """
        Initialize the autoscaler.

        Args:
            scheduler: TaskScheduler instance
            ws_manager: WebSocketManager instance
            container_manager: ContainerManager instance
            min_workers: Minimum workers to maintain
            max_workers: Maximum workers allowed
            scale_up_threshold: Pending tasks before scaling up
            scale_down_delay: Seconds of idle before scaling down
            check_interval: Seconds between autoscaling checks
            default_image: Docker image for new workers
        """
        self._scheduler = scheduler
        self._ws_manager = ws_manager
        self._container_manager = container_manager

        self.min_workers = min_workers
        self.max_workers = max_workers
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_delay = scale_down_delay
        self.check_interval = check_interval
        self.default_image = default_image

        self._running = False
        self._task: asyncio.Task | None = None
        self._last_activity = datetime.now(timezone.utc)
        self._scale_up_count = 0
        self._scale_down_count = 0

        # Callbacks for custom scaling logic
        self._on_scale_up: Callable[[int], None] | None = None
        self._on_scale_down: Callable[[int], None] | None = None

    async def start(self):
        """Start the autoscaler loop."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._autoscale_loop())
        logger.info(
            f"Autoscaler started: min={self.min_workers}, max={self.max_workers}, "
            f"scale_down_delay={self.scale_down_delay}s"
        )

    async def stop(self):
        """Stop the autoscaler loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Autoscaler stopped")

    async def _autoscale_loop(self):
        """Main autoscaling loop."""
        while self._running:
            try:
                await self._check_and_scale()
            except Exception as e:
                logger.error(f"Autoscaler error: {e}")

            await asyncio.sleep(self.check_interval)

    async def _check_and_scale(self):
        """Check metrics and scale if needed."""
        # Get current state
        stats = self._scheduler.get_stats()
        pending_tasks = stats.get("total_pending_tasks", 0)
        active_tasks = stats.get("total_active_tasks", 0)

        worker_count = self._ws_manager.connected_count
        available_workers = self._ws_manager.available_count

        # Update last activity if there's work
        if pending_tasks > 0 or active_tasks > 0:
            self._last_activity = datetime.now(timezone.utc)

        # Decide scaling action
        if pending_tasks >= self.scale_up_threshold and available_workers == 0:
            # Need more workers
            await self._scale_up(pending_tasks, worker_count)
        elif self._should_scale_down(pending_tasks, active_tasks, worker_count):
            # Too many idle workers
            await self._scale_down(worker_count)

    async def _scale_up(self, pending_tasks: int, current_workers: int):
        """Scale up workers based on demand."""
        if current_workers >= self.max_workers:
            logger.debug(f"At max workers ({self.max_workers}), cannot scale up")
            return

        # Calculate how many workers to add
        # Simple policy: 1 worker per pending task, up to max
        needed = min(pending_tasks, self.max_workers - current_workers)

        if needed <= 0:
            return

        logger.info(
            f"Scaling UP: adding {needed} workers "
            f"(pending={pending_tasks}, current={current_workers})"
        )

        # Start containers
        try:
            for _ in range(needed):
                self._container_manager.start_container(
                    image_tag=self.default_image,
                    environment={
                        "MINIMODAL_AUTO_SCALED": "true",
                    },
                )
                self._scale_up_count += 1

            if self._on_scale_up:
                self._on_scale_up(needed)

        except Exception as e:
            logger.error(f"Failed to scale up: {e}")

    def _should_scale_down(
        self,
        pending_tasks: int,
        active_tasks: int,
        current_workers: int,
    ) -> bool:
        """Determine if we should scale down."""
        if current_workers <= self.min_workers:
            return False

        if pending_tasks > 0 or active_tasks > 0:
            return False

        # Check idle duration
        idle_seconds = (datetime.now(timezone.utc) - self._last_activity).total_seconds()
        return idle_seconds >= self.scale_down_delay

    async def _scale_down(self, current_workers: int):
        """Scale down workers."""
        target = max(self.min_workers, current_workers - 1)
        to_remove = current_workers - target

        if to_remove <= 0:
            return

        logger.info(
            f"Scaling DOWN: removing {to_remove} workers "
            f"(current={current_workers}, target={target})"
        )

        try:
            # Stop oldest containers first
            containers = self._container_manager.list_containers()
            containers_sorted = sorted(containers, key=lambda c: c.started_at)

            for container in containers_sorted[:to_remove]:
                self._container_manager.stop_container(container.container_id)
                self._scale_down_count += 1

            if self._on_scale_down:
                self._on_scale_down(to_remove)

        except Exception as e:
            logger.error(f"Failed to scale down: {e}")

    def set_callbacks(
        self,
        on_scale_up: Callable[[int], None] | None = None,
        on_scale_down: Callable[[int], None] | None = None,
    ):
        """Set optional callbacks for scaling events."""
        self._on_scale_up = on_scale_up
        self._on_scale_down = on_scale_down

    def get_stats(self) -> dict:
        """Get autoscaler statistics."""
        now = datetime.now(timezone.utc)
        idle_seconds = (now - self._last_activity).total_seconds()

        return {
            "running": self._running,
            "min_workers": self.min_workers,
            "max_workers": self.max_workers,
            "scale_up_threshold": self.scale_up_threshold,
            "scale_down_delay": self.scale_down_delay,
            "idle_seconds": round(idle_seconds, 1),
            "scale_up_count": self._scale_up_count,
            "scale_down_count": self._scale_down_count,
            "last_activity": self._last_activity.isoformat(),
        }


# Global autoscaler instance
_autoscaler: Autoscaler | None = None


def get_autoscaler() -> Autoscaler | None:
    """Get the global autoscaler instance."""
    return _autoscaler


def init_autoscaler(
    scheduler,
    ws_manager,
    container_manager,
    **kwargs,
) -> Autoscaler:
    """Initialize the global autoscaler."""
    global _autoscaler
    _autoscaler = Autoscaler(
        scheduler=scheduler,
        ws_manager=ws_manager,
        container_manager=container_manager,
        **kwargs,
    )
    return _autoscaler
