"""Health check and statistics endpoints."""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from sqlmodel import select

from minimodal.server.models import (
    FunctionRecord,
    InvocationRecord,
    InvocationStatus,
    ScheduleRecord,
    SecretRecord,
    VolumeRecord,
    WebEndpointRecord,
    WorkerRecord,
    WorkerStatus,
)
from minimodal.server.task_scheduler import get_scheduler

router = APIRouter(tags=["Health"])

# These will be set by api.py during startup
_get_session = None
_server_state = None
_check_db_connection = None


def init_health_router(get_session_func, server_state, check_db_func):
    """Initialize the health router with dependencies."""
    global _get_session, _server_state, _check_db_connection
    _get_session = get_session_func
    _server_state = server_state
    _check_db_connection = check_db_func


@router.get("/health")
def health():
    """
    Basic health check endpoint.

    Returns 200 if the server is running, regardless of readiness.
    Use /ready for a full readiness check.
    """
    return {"status": "ok"}


@router.get("/ready")
def ready():
    """
    Readiness check endpoint.

    Returns 200 if the server is ready to accept requests.
    Returns 503 if the server is not ready or is shutting down.

    This checks:
    - Server initialization complete
    - Database connectivity
    - Not in shutdown state
    """
    if _server_state.is_shutting_down:
        raise HTTPException(
            status_code=503,
            detail="Server is shutting down",
        )

    if not _server_state.is_ready:
        raise HTTPException(
            status_code=503,
            detail="Server is not ready",
        )

    if not _check_db_connection():
        raise HTTPException(
            status_code=503,
            detail="Database connection failed",
        )

    uptime_seconds = None
    if _server_state.start_time:
        uptime_seconds = (
            datetime.now(timezone.utc) - _server_state.start_time
        ).total_seconds()

    return {
        "status": "ready",
        "uptime_seconds": uptime_seconds,
    }


@router.get("/stats")
def stats():
    """
    Get comprehensive server statistics.

    Returns information about functions, invocations, workers, and resources.
    """
    with _get_session() as session:
        # Function stats
        functions = session.exec(select(FunctionRecord)).all()
        total_functions = len(functions)
        functions_by_status = {}
        for f in functions:
            status = f.status.value
            functions_by_status[status] = functions_by_status.get(status, 0) + 1

        # Invocation stats
        invocations = session.exec(select(InvocationRecord)).all()
        total_invocations = len(invocations)

        queued = 0
        running = 0
        completed = 0
        failed = 0
        streaming = 0
        total_duration = 0.0
        completed_count = 0

        for inv in invocations:
            if inv.status == InvocationStatus.QUEUED:
                queued += 1
            elif inv.status == InvocationStatus.RUNNING:
                running += 1
            elif inv.status == InvocationStatus.COMPLETED:
                completed += 1
                if inv.started_at and inv.completed_at:
                    duration = (inv.completed_at - inv.started_at).total_seconds()
                    total_duration += duration
                    completed_count += 1
            elif inv.status == InvocationStatus.FAILED:
                failed += 1

            if inv.result_type == "stream":
                streaming += 1

        avg_duration = total_duration / completed_count if completed_count > 0 else 0

        # Worker stats
        workers = session.exec(select(WorkerRecord)).all()
        total_workers = len(workers)
        idle_workers = sum(1 for w in workers if w.status == WorkerStatus.IDLE)
        busy_workers = sum(1 for w in workers if w.status == WorkerStatus.BUSY)
        total_cpu = sum(w.cpu_cores for w in workers)
        total_memory = sum(w.memory_mb for w in workers)

        # Resource stats
        volumes = len(session.exec(select(VolumeRecord)).all())
        secrets = len(session.exec(select(SecretRecord)).all())
        web_endpoints = len(session.exec(select(WebEndpointRecord)).all())
        schedules = len(session.exec(select(ScheduleRecord)).all())

        # Uptime
        uptime_seconds = None
        if _server_state.start_time:
            uptime_seconds = (
                datetime.now(timezone.utc) - _server_state.start_time
            ).total_seconds()

    return {
        "functions": {
            "total": total_functions,
            "by_status": functions_by_status,
        },
        "invocations": {
            "total": total_invocations,
            "queued": queued,
            "running": running,
            "completed": completed,
            "failed": failed,
            "streaming": streaming,
            "avg_duration_seconds": round(avg_duration, 3),
        },
        "workers": {
            "total": total_workers,
            "idle": idle_workers,
            "busy": busy_workers,
            "total_cpu_cores": total_cpu,
            "total_memory_mb": total_memory,
        },
        "resources": {
            "volumes": volumes,
            "secrets": secrets,
            "web_endpoints": web_endpoints,
            "schedules": schedules,
        },
        "uptime_seconds": uptime_seconds,
    }


@router.get("/stats/functions")
def stats_functions():
    """Get detailed per-function statistics."""
    with _get_session() as session:
        functions = session.exec(select(FunctionRecord)).all()
        result = []

        for func in functions:
            # Get invocations for this function
            invocations = session.exec(
                select(InvocationRecord).where(InvocationRecord.function_id == func.id)
            ).all()

            total = len(invocations)
            completed = sum(
                1 for i in invocations if i.status == InvocationStatus.COMPLETED
            )
            failed = sum(1 for i in invocations if i.status == InvocationStatus.FAILED)
            success_rate = completed / total if total > 0 else 0

            # Calculate avg duration for completed invocations
            durations = []
            for inv in invocations:
                if (
                    inv.status == InvocationStatus.COMPLETED
                    and inv.started_at
                    and inv.completed_at
                ):
                    durations.append(
                        (inv.completed_at - inv.started_at).total_seconds()
                    )
            avg_duration = sum(durations) / len(durations) if durations else 0

            # Check if this function has any streaming invocations
            has_streaming = any(inv.result_type == "stream" for inv in invocations)

            result.append(
                {
                    "id": func.id,
                    "name": func.name,
                    "app_name": func.app_name,
                    "status": func.status.value,
                    "required_cpu": func.required_cpu,
                    "required_memory": func.required_memory,
                    "supports_streaming": has_streaming,
                    "invocations": {
                        "total": total,
                        "completed": completed,
                        "failed": failed,
                        "success_rate": round(success_rate, 3),
                        "avg_duration_seconds": round(avg_duration, 3),
                    },
                    "created_at": func.created_at.isoformat(),
                }
            )

        return {"functions": result}


@router.get("/stats/workers")
def stats_workers():
    """Get detailed per-worker statistics."""
    with _get_session() as session:
        workers = session.exec(select(WorkerRecord)).all()
        result = []

        for worker in workers:
            result.append(
                {
                    "id": worker.id,
                    "worker_id": worker.worker_id,
                    "status": worker.status.value,
                    "cpu_cores": worker.cpu_cores,
                    "memory_mb": worker.memory_mb,
                    "current_invocation_id": worker.current_invocation_id,
                    "tasks_completed": worker.tasks_completed,
                    "tasks_failed": worker.tasks_failed,
                    "last_heartbeat": worker.last_heartbeat.isoformat(),
                    "registered_at": worker.registered_at.isoformat(),
                }
            )

        # Summary
        total_workers = len(workers)
        idle = sum(1 for w in workers if w.status == WorkerStatus.IDLE)
        busy = sum(1 for w in workers if w.status == WorkerStatus.BUSY)
        total_cpu = sum(w.cpu_cores for w in workers)
        total_memory = sum(w.memory_mb for w in workers)
        total_tasks = sum(w.tasks_completed + w.tasks_failed for w in workers)
        total_completed = sum(w.tasks_completed for w in workers)

        return {
            "workers": result,
            "summary": {
                "total": total_workers,
                "idle": idle,
                "busy": busy,
                "total_cpu_cores": total_cpu,
                "total_memory_mb": total_memory,
                "total_tasks_processed": total_tasks,
                "total_tasks_completed": total_completed,
            },
        }


@router.get("/stats/invocations")
def stats_invocations(limit: int = 50):
    """Get recent invocation details."""
    with _get_session() as session:
        # Get recent invocations ordered by creation time
        invocations = session.exec(
            select(InvocationRecord)
            .order_by(InvocationRecord.created_at.desc())
            .limit(limit)
        ).all()

        result = []
        for inv in invocations:
            # Get function name
            func = session.get(FunctionRecord, inv.function_id)
            func_name = func.name if func else "unknown"

            duration = None
            if inv.started_at and inv.completed_at:
                duration = (inv.completed_at - inv.started_at).total_seconds()

            result.append(
                {
                    "id": inv.id,
                    "function_id": inv.function_id,
                    "function_name": func_name,
                    "user_id": inv.user_id,
                    "status": inv.status.value,
                    "worker_id": inv.worker_id,
                    "result_type": inv.result_type.value
                    if inv.result_type
                    else "single",
                    "duration_seconds": round(duration, 3) if duration else None,
                    "error_message": inv.error_message,
                    "created_at": inv.created_at.isoformat(),
                    "started_at": inv.started_at.isoformat()
                    if inv.started_at
                    else None,
                    "completed_at": inv.completed_at.isoformat()
                    if inv.completed_at
                    else None,
                }
            )

        return {"invocations": result}


@router.get("/stats/scheduler")
def stats_scheduler():
    """Get task scheduler statistics."""
    scheduler = get_scheduler()
    if not scheduler:
        return {"error": "Scheduler not initialized"}

    return scheduler.get_stats()


@router.get("/stats/autoscaler")
def stats_autoscaler():
    """Get autoscaler statistics."""
    from minimodal.server.autoscaler import get_autoscaler

    autoscaler = get_autoscaler()
    if not autoscaler:
        return {
            "enabled": False,
            "message": "Autoscaling is disabled. Set MINIMODAL_AUTOSCALING=true to enable.",
        }

    stats = autoscaler.get_stats()
    stats["enabled"] = True
    return stats
