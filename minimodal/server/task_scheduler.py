"""
Push-based task scheduler with user isolation and quota management.

Based on YTsaurus-inspired design from SCHEDULER.md:
- User Isolation: Each user has their own queue
- Concurrency Limits: Users are capped at quota_limit parallel tasks
- Push Scheduling: Server assigns tasks to workers via WebSocket
- Round Robin: Fair distribution across eligible users
- Bin Packing: Match task resources to worker capacity
"""

import asyncio
import base64
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Optional

from sqlmodel import select

from minimodal.server.models import (
    InvocationRecord,
    InvocationStatus,
    FunctionRecord,
    UserRecord,
    WorkerRecord,
    WorkerStatus,
)

logger = logging.getLogger("minimodal.scheduler")


@dataclass
class SchedulerTask:
    """A task waiting to be scheduled."""
    invocation_id: int
    function_id: int
    user_id: str
    required_cpu: int
    required_memory: int
    # Pre-computed task data for WebSocket dispatch
    task_data: dict = field(default_factory=dict)


@dataclass
class SchedulerUser:
    """In-memory user state for scheduling."""
    user_id: str
    queue: list[SchedulerTask] = field(default_factory=list)
    active_count: int = 0
    quota_limit: int = 5

    @property
    def can_run_more(self) -> bool:
        return self.active_count < self.quota_limit

    @property
    def has_pending(self) -> bool:
        return len(self.queue) > 0


class TaskScheduler:
    """
    Push-based task scheduler with user isolation.

    Key features:
    - Per-user queues prevent one user from starving others
    - Quota limits control concurrency per user
    - Round-robin ensures fairness across users
    - Bin packing matches tasks to workers by resources
    """

    def __init__(self, get_session_func, ws_manager):
        """
        Initialize the scheduler.

        Args:
            get_session_func: Function that returns a database session
            ws_manager: WebSocket connection manager for workers
        """
        self._get_session = get_session_func
        self._ws_manager = ws_manager
        self._users: dict[str, SchedulerUser] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._schedule_event = asyncio.Event()

    async def start(self):
        """Start the scheduler loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._schedule_loop())
        logger.info("Task scheduler started")

    async def stop(self):
        """Stop the scheduler loop."""
        self._running = False
        self._schedule_event.set()  # Wake up the loop
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Task scheduler stopped")

    async def enqueue_task(
        self,
        invocation_id: int,
        function_id: int,
        user_id: str,
        required_cpu: int,
        required_memory: int,
        task_data: dict,
    ):
        """
        Add a task to the scheduler queue.

        Called when a new invocation is created.
        """
        async with self._lock:
            # Get or create user
            if user_id not in self._users:
                # Load quota from database
                quota_limit = 5  # Default
                with self._get_session() as session:
                    db_user = session.get(UserRecord, user_id)
                    if db_user:
                        quota_limit = db_user.quota_limit
                self._users[user_id] = SchedulerUser(
                    user_id=user_id,
                    quota_limit=quota_limit,
                )

            task = SchedulerTask(
                invocation_id=invocation_id,
                function_id=function_id,
                user_id=user_id,
                required_cpu=required_cpu,
                required_memory=required_memory,
                task_data=task_data,
            )

            self._users[user_id].queue.append(task)
            logger.debug(f"Enqueued task {invocation_id} for user {user_id}")

        # Signal the scheduler to wake up
        self._schedule_event.set()

    async def task_completed(self, user_id: str, invocation_id: int):
        """
        Mark a task as completed, freeing up user quota.

        Called when a worker reports task completion.
        """
        async with self._lock:
            user = self._users.get(user_id)
            if user and user.active_count > 0:
                user.active_count -= 1
                logger.debug(
                    f"Task {invocation_id} completed for user {user_id}, "
                    f"active: {user.active_count}/{user.quota_limit}"
                )

        # Signal scheduler to potentially assign more work
        self._schedule_event.set()

    async def task_failed(self, user_id: str, invocation_id: int):
        """
        Mark a task as failed, freeing up user quota.

        Called when a worker reports task failure.
        """
        # Same as completed - frees up quota
        await self.task_completed(user_id, invocation_id)

    async def update_user_quota(self, user_id: str, quota_limit: int):
        """Update a user's quota limit."""
        async with self._lock:
            if user_id in self._users:
                self._users[user_id].quota_limit = quota_limit
                logger.info(f"Updated quota for user {user_id} to {quota_limit}")

    async def sync_from_database(self):
        """
        Sync scheduler state from database.

        Called on startup to restore state from any pending invocations.
        """
        async with self._lock:
            with self._get_session() as session:
                # Load all users with their quotas
                db_users = session.exec(select(UserRecord)).all()
                for db_user in db_users:
                    if db_user.id not in self._users:
                        self._users[db_user.id] = SchedulerUser(
                            user_id=db_user.id,
                            quota_limit=db_user.quota_limit,
                        )
                    else:
                        self._users[db_user.id].quota_limit = db_user.quota_limit

                # Count active (RUNNING) invocations per user
                running = session.exec(
                    select(InvocationRecord)
                    .where(InvocationRecord.status == InvocationStatus.RUNNING)
                ).all()

                for inv in running:
                    user_id = inv.user_id
                    if user_id not in self._users:
                        self._users[user_id] = SchedulerUser(user_id=user_id)
                    self._users[user_id].active_count += 1

                # Load queued invocations into user queues
                queued = session.exec(
                    select(InvocationRecord, FunctionRecord)
                    .join(FunctionRecord, InvocationRecord.function_id == FunctionRecord.id)
                    .where(InvocationRecord.status == InvocationStatus.QUEUED)
                    .order_by(InvocationRecord.created_at)
                ).all()

                for inv, func in queued:
                    user_id = inv.user_id
                    if user_id not in self._users:
                        self._users[user_id] = SchedulerUser(user_id=user_id)

                    # Prepare task data
                    volume_mounts = json.loads(func.volume_mounts) if func.volume_mounts else {}
                    secrets = json.loads(func.secrets) if func.secrets else []

                    task_data = {
                        "id": inv.id,
                        "function_id": inv.function_id,
                        "pickled_code": base64.b64encode(func.pickled_code).decode(),
                        "pickled_args": base64.b64encode(inv.pickled_args).decode(),
                        "image_tag": func.image_tag,
                        "volume_mounts": volume_mounts,
                        "secrets": secrets,
                        "timeout_seconds": func.timeout_seconds,
                        "required_cpu": func.required_cpu,
                        "required_memory": func.required_memory,
                    }

                    task = SchedulerTask(
                        invocation_id=inv.id,
                        function_id=inv.function_id,
                        user_id=user_id,
                        required_cpu=func.required_cpu,
                        required_memory=func.required_memory,
                        task_data=task_data,
                    )
                    self._users[user_id].queue.append(task)

        logger.info(f"Synced scheduler state: {len(self._users)} users")

    async def _schedule_loop(self):
        """
        Main scheduling loop.

        Runs continuously, matching eligible users to available workers.
        Uses round-robin for fairness across users.
        """
        while self._running:
            try:
                # Wait for signal or timeout
                try:
                    await asyncio.wait_for(self._schedule_event.wait(), timeout=0.1)
                except asyncio.TimeoutError:
                    pass
                self._schedule_event.clear()

                if not self._running:
                    break

                # Skip if no workers available
                if self._ws_manager.available_count == 0:
                    continue

                await self._schedule_round()

            except Exception as e:
                logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(0.1)

    async def _schedule_round(self):
        """
        Execute one round of scheduling.

        Round-robin through eligible users, assigning one task per user per round.
        """
        async with self._lock:
            # Get eligible users (have pending tasks + under quota)
            eligible = [
                user for user in self._users.values()
                if user.has_pending and user.can_run_more
            ]

            if not eligible:
                return

            # Round-robin through eligible users
            for user in eligible:
                if not user.queue:
                    continue

                task = user.queue[0]  # Peek at first task

                # Find a worker that can handle this task (bin packing)
                worker = await self._ws_manager.find_worker_for_task(
                    required_cpu=task.required_cpu,
                    required_memory=task.required_memory,
                )

                if not worker:
                    # No capable worker available, try next user
                    continue

                # Assign task to worker
                if await self._ws_manager.assign_task(worker, task.task_data):
                    # Remove from queue and update state
                    user.queue.pop(0)
                    user.active_count += 1

                    # Update database
                    await self._update_invocation_started(
                        task.invocation_id,
                        worker.worker_id,
                        task.task_data.get("timeout_seconds", 300),
                        task.task_data.get("max_retries", 3),
                    )

                    logger.debug(
                        f"Assigned task {task.invocation_id} to worker {worker.worker_id}, "
                        f"user {user.user_id} active: {user.active_count}/{user.quota_limit}"
                    )

    async def _update_invocation_started(
        self,
        invocation_id: int,
        worker_id: str,
        timeout_seconds: int,
        max_retries: int,
    ):
        """Update database when invocation starts running."""
        with self._get_session() as session:
            now = datetime.now(timezone.utc)

            invocation = session.get(InvocationRecord, invocation_id)
            if invocation:
                invocation.status = InvocationStatus.RUNNING
                invocation.started_at = now
                invocation.timeout_at = now + timedelta(seconds=timeout_seconds)
                invocation.max_retries = max_retries
                invocation.worker_id = worker_id
                session.add(invocation)

            # Update worker record
            db_worker = session.exec(
                select(WorkerRecord).where(WorkerRecord.worker_id == worker_id)
            ).first()
            if db_worker:
                db_worker.status = WorkerStatus.BUSY
                db_worker.current_invocation_id = invocation_id
                db_worker.last_heartbeat = now
                session.add(db_worker)

            # Update user active_count in DB
            if invocation:
                db_user = session.get(UserRecord, invocation.user_id)
                if db_user:
                    db_user.active_count += 1
                    db_user.updated_at = now
                    session.add(db_user)

            session.commit()

    def get_stats(self) -> dict:
        """Get scheduler statistics."""
        total_pending = sum(len(u.queue) for u in self._users.values())
        total_active = sum(u.active_count for u in self._users.values())
        users_with_pending = sum(1 for u in self._users.values() if u.has_pending)

        return {
            "total_pending_tasks": total_pending,
            "total_active_tasks": total_active,
            "users_tracked": len(self._users),
            "users_with_pending": users_with_pending,
            "users": {
                user_id: {
                    "pending": len(user.queue),
                    "active": user.active_count,
                    "quota": user.quota_limit,
                }
                for user_id, user in self._users.items()
            },
        }


# Global scheduler instance
_scheduler: Optional[TaskScheduler] = None


def get_scheduler() -> Optional[TaskScheduler]:
    """Get the global scheduler instance."""
    return _scheduler


def init_scheduler(get_session_func, ws_manager) -> TaskScheduler:
    """Initialize the global scheduler."""
    global _scheduler
    _scheduler = TaskScheduler(get_session_func, ws_manager)
    return _scheduler
