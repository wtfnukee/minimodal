"""
Cron scheduler for periodic/scheduled function execution.

This module handles running functions on a schedule:
- Cron expressions (e.g., "0 * * * *" for hourly)
- Period-based (e.g., every 60 seconds)

For task distribution to workers, see task_scheduler.py
"""

import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional


class CronScheduler:
    """
    Background service that triggers scheduled functions.

    The cron scheduler runs in a background thread and periodically checks
    for scheduled functions that are due to run. When a schedule is due,
    it creates an invocation for the associated function.

    Usage:
        cron_scheduler = CronScheduler(engine)
        cron_scheduler.start()
        # ... later ...
        cron_scheduler.stop()
    """

    def __init__(
        self,
        engine,
        check_interval: float = 10.0,
    ):
        """
        Initialize the cron scheduler.

        Args:
            engine: SQLAlchemy engine for database access
            check_interval: Seconds between schedule checks
        """
        self.engine = engine
        self.check_interval = check_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._logger = logging.getLogger("minimodal.cron_scheduler")

    def start(self) -> None:
        """Start the cron scheduler background thread."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        self._logger.info("Cron scheduler started")

    def stop(self) -> None:
        """Stop the cron scheduler."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
        self._logger.info("Cron scheduler stopped")

    def _run(self) -> None:
        """Main cron scheduler loop."""
        while self._running:
            try:
                self._check_schedules()
            except Exception as e:
                self._logger.error(f"Cron scheduler error: {e}")

            time.sleep(self.check_interval)

    def _check_schedules(self) -> None:
        """Check for due schedules and trigger invocations."""
        from sqlmodel import Session, select
        from minimodal.server.models import (
            ScheduleRecord,
            InvocationRecord,
            InvocationStatus,
            FunctionRecord,
            FunctionStatus,
        )
        import cloudpickle

        now = datetime.now(timezone.utc)

        with Session(self.engine) as session:
            # Find all enabled schedules that are due
            due_schedules = session.exec(
                select(ScheduleRecord).where(
                    ScheduleRecord.enabled == True,
                    ScheduleRecord.next_run <= now,
                )
            ).all()

            for schedule in due_schedules:
                # Check if function is ready
                func = session.get(FunctionRecord, schedule.function_id)
                if not func or func.status != FunctionStatus.READY:
                    self._logger.warning(
                        f"Skipping schedule {schedule.id}: function not ready"
                    )
                    continue

                # Create invocation with empty args
                pickled_args = cloudpickle.dumps(((), {}))

                invocation = InvocationRecord(
                    function_id=schedule.function_id,
                    pickled_args=pickled_args,
                    status=InvocationStatus.QUEUED,
                )
                session.add(invocation)

                # Update schedule
                schedule.last_run = now
                schedule.next_run = self._calculate_next_run(schedule, now)
                session.add(schedule)

                self._logger.info(
                    f"Triggered scheduled invocation for function {schedule.function_id}, "
                    f"next run at {schedule.next_run}"
                )

            session.commit()

    def _calculate_next_run(
        self,
        schedule,
        from_time: datetime,
    ) -> datetime:
        """
        Calculate the next run time for a schedule.

        Args:
            schedule: ScheduleRecord to calculate for
            from_time: Time to calculate from

        Returns:
            Next run datetime
        """
        if schedule.cron_expression:
            # Use croniter for cron expressions
            try:
                from croniter import croniter
                cron = croniter(schedule.cron_expression, from_time)
                return cron.get_next(datetime)
            except Exception as e:
                self._logger.error(f"Invalid cron expression: {e}")
                return from_time + timedelta(hours=1)
        elif schedule.period_seconds:
            # Simple period calculation
            return from_time.replace(microsecond=0) + timedelta(
                seconds=schedule.period_seconds
            )
        else:
            # Default: run again in 1 hour
            return from_time + timedelta(hours=1)


def calculate_initial_next_run(
    cron_expression: Optional[str] = None,
    period_seconds: Optional[int] = None,
) -> datetime:
    """
    Calculate the initial next_run time for a new schedule.

    Args:
        cron_expression: Cron expression string
        period_seconds: Period in seconds

    Returns:
        Initial next_run datetime
    """
    now = datetime.now(timezone.utc)

    if cron_expression:
        try:
            from croniter import croniter
            cron = croniter(cron_expression, now)
            return cron.get_next(datetime)
        except Exception:
            return now + timedelta(hours=1)
    elif period_seconds:
        return now + timedelta(seconds=period_seconds)
    else:
        return now + timedelta(hours=1)


# Global cron scheduler instance
_cron_scheduler: Optional[CronScheduler] = None


def get_cron_scheduler(engine) -> CronScheduler:
    """Get or create the global cron scheduler instance."""
    global _cron_scheduler
    if _cron_scheduler is None:
        _cron_scheduler = CronScheduler(engine)
    return _cron_scheduler


def start_cron_scheduler(engine) -> None:
    """Start the global cron scheduler."""
    scheduler = get_cron_scheduler(engine)
    scheduler.start()


def stop_cron_scheduler() -> None:
    """Stop the global cron scheduler."""
    global _cron_scheduler
    if _cron_scheduler:
        _cron_scheduler.stop()
        _cron_scheduler = None
