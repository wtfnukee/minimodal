"""Dashboard and system health endpoints."""

from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from sqlmodel import select

from minimodal.server.models import (
    FunctionRecord,
    InvocationRecord,
    InvocationStatus,
    WorkerRecord,
    VolumeRecord,
    SecretRecord,
    WebEndpointRecord,
    ScheduleRecord,
    UserRecord,
)

router = APIRouter(tags=["Dashboard"])

# These will be set by api.py during startup
_get_session = None
_jinja_env = None
_stats_func = None
_stats_functions_func = None
_stats_workers_func = None
_stats_invocations_func = None


def init_dashboard_router(
    get_session_func,
    jinja_env,
    stats_func,
    stats_functions_func,
    stats_workers_func,
    stats_invocations_func,
):
    """Initialize the dashboard router with dependencies."""
    global _get_session, _jinja_env, _stats_func, _stats_functions_func
    global _stats_workers_func, _stats_invocations_func
    _get_session = get_session_func
    _jinja_env = jinja_env
    _stats_func = stats_func
    _stats_functions_func = stats_functions_func
    _stats_workers_func = stats_workers_func
    _stats_invocations_func = stats_invocations_func


def _format_bytes(size_bytes: int) -> str:
    """Format bytes to human-readable string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


def _format_period(seconds: int) -> str:
    """Format period seconds to human-readable string."""
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m"
    else:
        hours = seconds // 3600
        mins = (seconds % 3600) // 60
        if mins:
            return f"{hours}h {mins}m"
        return f"{hours}h"


def _format_duration(seconds: float) -> str:
    """Format duration to human-readable string."""
    if seconds <= 0:
        return "now"
    elif seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        return f"{int(seconds // 60)}m {int(seconds % 60)}s"
    else:
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"


@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(request: Request):
    """
    Render the HTML dashboard with live stats.

    Auto-refreshes every 5 seconds to show real-time updates.
    """
    from minimodal.server.crypto import decrypt_value

    # Get all the data we need
    stats_data = _stats_func()
    functions_data = _stats_functions_func()
    workers_data = _stats_workers_func()
    invocations_data = _stats_invocations_func()

    # Get detailed resource data
    with _get_session() as session:
        # Volumes with details
        volumes_list = []
        for v in session.exec(select(VolumeRecord)).all():
            volumes_list.append({
                "id": v.id,
                "name": v.name,
                "size_bytes": v.size_bytes,
                "size_human": _format_bytes(v.size_bytes),
                "created_at": v.created_at.isoformat(),
            })

        # Secrets with decrypted values (for dev dashboard only!)
        secrets_list = []
        for s in session.exec(select(SecretRecord)).all():
            try:
                decrypted = decrypt_value(s.encrypted_value)
                # Mask the value for display (show first/last 2 chars)
                if len(decrypted) > 6:
                    masked = decrypted[:2] + "*" * (len(decrypted) - 4) + decrypted[-2:]
                else:
                    masked = "*" * len(decrypted)
            except Exception:
                decrypted = "[error decrypting]"
                masked = "[error]"

            secrets_list.append({
                "id": s.id,
                "name": s.name,
                "value": decrypted,
                "masked_value": masked,
                "created_at": s.created_at.isoformat(),
                "updated_at": s.updated_at.isoformat(),
            })

        # Web endpoints with full URLs
        base_url = str(request.base_url).rstrip("/")
        web_endpoints_list = []
        for e in session.exec(select(WebEndpointRecord)).all():
            func = session.get(FunctionRecord, e.function_id)
            func_name = func.name if func else "unknown"
            web_endpoints_list.append({
                "id": e.id,
                "path": e.path,
                "method": e.method,
                "function_id": e.function_id,
                "function_name": func_name,
                "full_url": f"{base_url}/web{e.path}",
                "created_at": e.created_at.isoformat(),
            })

        # Schedules with execution history
        schedules_list = []
        now = datetime.now(timezone.utc)
        for s in session.exec(select(ScheduleRecord)).all():
            func = session.get(FunctionRecord, s.function_id)
            func_name = func.name if func else "unknown"

            # Calculate time until next run (handle timezone-naive datetimes)
            next_run = s.next_run
            if next_run.tzinfo is None:
                # Assume UTC for naive datetimes
                next_run = next_run.replace(tzinfo=timezone.utc)
            time_until = (next_run - now).total_seconds() if next_run > now else 0

            schedules_list.append({
                "id": s.id,
                "function_id": s.function_id,
                "function_name": func_name,
                "cron_expression": s.cron_expression,
                "period_seconds": s.period_seconds,
                "period_human": _format_period(s.period_seconds) if s.period_seconds else None,
                "last_run": s.last_run.isoformat() if s.last_run else None,
                "next_run": s.next_run.isoformat(),
                "time_until_seconds": int(time_until),
                "time_until_human": _format_duration(time_until),
                "enabled": s.enabled,
            })

        # Users with quota information
        users_list = []
        for u in session.exec(select(UserRecord)).all():
            # Count active and pending invocations for this user
            active_invocations = session.exec(
                select(InvocationRecord)
                .where(InvocationRecord.user_id == u.id)
                .where(InvocationRecord.status.in_([InvocationStatus.RUNNING, InvocationStatus.QUEUED]))
            ).all()
            
            active_count = sum(1 for inv in active_invocations if inv.status == InvocationStatus.RUNNING)
            pending_count = sum(1 for inv in active_invocations if inv.status == InvocationStatus.QUEUED)
            
            users_list.append({
                "id": u.id,
                "quota_limit": u.quota_limit,
                "active_count": active_count,
                "pending_count": pending_count,
                "created_at": u.created_at.isoformat(),
                "updated_at": u.updated_at.isoformat(),
            })

    # Render the template
    template = _jinja_env.get_template("dashboard.html")
    html = template.render(
        stats=stats_data,
        functions=functions_data,
        workers=workers_data,
        invocations=invocations_data,
        volumes=volumes_list,
        secrets=secrets_list,
        web_endpoints=web_endpoints_list,
        schedules=schedules_list,
        users=users_list,
        base_url=base_url,
    )
    return HTMLResponse(content=html)


@router.get("/system/health")
def get_system_health():
    """
    Get overall system health metrics.

    Returns degradation indicators and capacity information.
    """
    with _get_session() as session:
        now = datetime.now(timezone.utc)

        # Worker health
        workers = session.exec(select(WorkerRecord)).all()

        def get_heartbeat_age(w):
            last_heartbeat = w.last_heartbeat
            if last_heartbeat.tzinfo is None:
                last_heartbeat = last_heartbeat.replace(tzinfo=timezone.utc)
            return (now - last_heartbeat).total_seconds()

        healthy_workers = sum(
            1 for w in workers
            if w.is_healthy and get_heartbeat_age(w) < 30
        )
        total_workers = len(workers)

        # Task queue depth
        queued_tasks = session.exec(
            select(InvocationRecord)
            .where(InvocationRecord.status == InvocationStatus.QUEUED)
        ).all()
        queue_depth = len(queued_tasks)

        # Recent failure rate
        recent_invocations = session.exec(
            select(InvocationRecord)
            .where(InvocationRecord.created_at >= now - timedelta(minutes=5))
        ).all()
        recent_failed = sum(
            1 for inv in recent_invocations
            if inv.status in (InvocationStatus.FAILED, InvocationStatus.DEAD_LETTER)
        )
        recent_total = len(recent_invocations)
        failure_rate = recent_failed / recent_total if recent_total > 0 else 0

        # Dead letter queue size
        dlq_count = session.exec(
            select(InvocationRecord)
            .where(InvocationRecord.status == InvocationStatus.DEAD_LETTER)
        ).all()

        # Determine overall health status
        status = "healthy"
        issues = []

        if total_workers == 0:
            status = "degraded"
            issues.append("No workers registered")
        elif healthy_workers < total_workers * 0.5:
            status = "degraded"
            issues.append(f"Only {healthy_workers}/{total_workers} workers healthy")

        if queue_depth > 100:
            status = "degraded"
            issues.append(f"High queue depth: {queue_depth}")

        if failure_rate > 0.1:
            status = "degraded"
            issues.append(f"High failure rate: {failure_rate:.1%}")

        if len(dlq_count) > 10:
            issues.append(f"{len(dlq_count)} items in dead letter queue")

        return {
            "status": status,
            "issues": issues,
            "metrics": {
                "workers": {
                    "total": total_workers,
                    "healthy": healthy_workers,
                },
                "queue_depth": queue_depth,
                "recent_failure_rate": round(failure_rate, 3),
                "dead_letter_count": len(dlq_count),
            },
        }
