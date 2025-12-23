"""Schedule management endpoints."""

from fastapi import APIRouter, HTTPException
from sqlmodel import select

from minimodal.server.models import (
    FunctionRecord,
    InvocationRecord,
    InvocationStatus,
    ScheduleListResponse,
    ScheduleRecord,
    ScheduleResponse,
)

router = APIRouter(tags=["Schedules"])

# These will be set by api.py during startup
_get_session = None


def init_schedules_router(get_session_func):
    """Initialize the schedules router with dependencies."""
    global _get_session
    _get_session = get_session_func


@router.post("/schedules")
def create_schedule(
    function_id: int,
    cron_expression: str | None = None,
    period_seconds: int | None = None,
):
    """Create a schedule for a function."""
    from minimodal.server.cron_scheduler import calculate_initial_next_run

    if not cron_expression and not period_seconds:
        raise HTTPException(
            status_code=400,
            detail="Either cron_expression or period_seconds must be provided",
        )

    with _get_session() as session:
        # Verify function exists
        func = session.get(FunctionRecord, function_id)
        if not func:
            raise HTTPException(status_code=404, detail="Function not found")

        # Check if schedule already exists for this function
        existing = session.exec(
            select(ScheduleRecord).where(ScheduleRecord.function_id == function_id)
        ).first()

        if existing:
            # Update existing schedule
            existing.cron_expression = cron_expression
            existing.period_seconds = period_seconds
            existing.next_run = calculate_initial_next_run(
                cron_expression, period_seconds
            )
            session.add(existing)
            session.commit()
            return {"id": existing.id, "function_id": function_id, "updated": True}

        # Calculate initial next_run
        next_run = calculate_initial_next_run(cron_expression, period_seconds)

        schedule = ScheduleRecord(
            function_id=function_id,
            cron_expression=cron_expression,
            period_seconds=period_seconds,
            next_run=next_run,
        )
        session.add(schedule)
        session.commit()
        session.refresh(schedule)

        return {
            "id": schedule.id,
            "function_id": function_id,
            "next_run": next_run.isoformat(),
        }


@router.get("/schedules", response_model=ScheduleListResponse)
def list_schedules():
    """List all scheduled functions."""
    with _get_session() as session:
        schedules = session.exec(select(ScheduleRecord)).all()
        return ScheduleListResponse(
            schedules=[
                ScheduleResponse(
                    id=s.id,
                    function_id=s.function_id,
                    cron_expression=s.cron_expression,
                    period_seconds=s.period_seconds,
                    last_run=s.last_run,
                    next_run=s.next_run,
                    enabled=s.enabled,
                )
                for s in schedules
            ]
        )


@router.get("/dead-letter-queue")
def get_dead_letter_queue(limit: int = 100):
    """Get invocations in the dead letter queue."""
    with _get_session() as session:
        invocations = session.exec(
            select(InvocationRecord)
            .where(InvocationRecord.status == InvocationStatus.DEAD_LETTER)
            .order_by(InvocationRecord.completed_at.desc())
            .limit(limit)
        ).all()

        result = []
        for inv in invocations:
            func = session.get(FunctionRecord, inv.function_id)
            result.append(
                {
                    "id": inv.id,
                    "function_id": inv.function_id,
                    "function_name": func.name if func else "unknown",
                    "retry_count": inv.retry_count,
                    "dead_letter_reason": inv.dead_letter_reason,
                    "error_message": inv.error_message,
                    "created_at": inv.created_at.isoformat(),
                    "completed_at": inv.completed_at.isoformat()
                    if inv.completed_at
                    else None,
                }
            )

        return {"dead_letter_queue": result, "count": len(result)}


@router.post("/dead-letter-queue/{invocation_id}/retry")
def retry_dead_letter(invocation_id: int):
    """Manually retry an invocation from the dead letter queue."""
    with _get_session() as session:
        invocation = session.get(InvocationRecord, invocation_id)
        if not invocation:
            raise HTTPException(status_code=404, detail="Invocation not found")

        if invocation.status != InvocationStatus.DEAD_LETTER:
            raise HTTPException(
                status_code=400,
                detail="Invocation is not in dead letter queue",
            )

        # Reset to queued for manual retry
        invocation.status = InvocationStatus.QUEUED
        invocation.retry_count = 0
        invocation.started_at = None
        invocation.completed_at = None
        invocation.worker_id = None
        invocation.error_message = None
        invocation.dead_letter_reason = None
        invocation.next_retry_at = None

        session.add(invocation)
        session.commit()

        return {"status": "requeued", "invocation_id": invocation_id}
