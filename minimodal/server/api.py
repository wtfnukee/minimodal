"""Control plane API server."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, Request
from sqlmodel import Session, SQLModel, create_engine, select, text
from jinja2 import Environment, FileSystemLoader

from minimodal.server.models import (
    InvocationRecord,
    InvocationStatus,
    WorkerRecord,
    WorkerStatus,
    UserRecord,
)
from minimodal.server.websocket import manager as ws_manager
from minimodal.server.task_scheduler import init_scheduler
from minimodal.server.autoscaler import init_autoscaler
from minimodal.server.containers import get_container_manager

# Import routers
from minimodal.server.routes import (
    health_router,
    functions_router,
    workers_router,
    volumes_router,
    secrets_router,
    users_router,
    web_endpoints_router,
    schedules_router,
    dashboard_router,
    init_health_router,
    init_functions_router,
    init_workers_router,
    init_volumes_router,
    init_secrets_router,
    init_users_router,
    init_web_endpoints_router,
    init_schedules_router,
    init_dashboard_router,
)
from minimodal.server.routes.health import stats, stats_functions, stats_workers, stats_invocations


# === Logging Setup ===

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("minimodal.server")


# === Database Setup ===

DATABASE_URL = os.environ.get("MINIMODAL_DATABASE_URL", "sqlite:///minimodal.db")
engine = create_engine(DATABASE_URL, echo=False)

# Volume storage directory
VOLUME_STORAGE_PATH = Path(
    os.environ.get("MINIMODAL_VOLUME_PATH", "./minimodal_volumes")
)

# Jinja2 template setup
TEMPLATES_DIR = Path(__file__).parent / "templates"
jinja_env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))


def init_db():
    SQLModel.metadata.create_all(engine)


def get_session() -> Session:
    return Session(engine)


def get_user_id_from_request(request: Request) -> str:
    """Extract user ID from X-User-ID header, defaulting to 'default'."""
    return request.headers.get("X-User-ID", "default")


def get_or_create_user(session: Session, user_id: str) -> UserRecord:
    """Get existing user or create a new one with default quota."""
    user = session.get(UserRecord, user_id)
    if not user:
        user = UserRecord(id=user_id)
        session.add(user)
        session.commit()
        session.refresh(user)
        logger.info(f"Created new user: {user_id}")
    return user


def check_db_connection() -> bool:
    """Check if the database is accessible."""
    try:
        with get_session() as session:
            session.exec(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"Database connection check failed: {e}")
        return False


# === Server State ===

class ServerState:
    """Track server state for health checks."""

    def __init__(self):
        self.is_ready = False
        self.is_shutting_down = False
        self.start_time: datetime | None = None

    def mark_ready(self):
        self.is_ready = True
        self.start_time = datetime.now(timezone.utc)

    def mark_shutting_down(self):
        self.is_shutting_down = True
        self.is_ready = False


server_state = ServerState()


# === Background Tasks ===

async def _process_retries():
    """Background task to process failed invocations for retry."""
    while True:
        try:
            await asyncio.sleep(5)  # Check every 5 seconds

            with get_session() as session:
                now = datetime.now(timezone.utc)

                # Move to dead letter queue after max retries
                dead_letter_candidates = session.exec(
                    select(InvocationRecord)
                    .where(
                        InvocationRecord.status == InvocationStatus.FAILED,
                        InvocationRecord.retry_count >= InvocationRecord.max_retries,
                    )
                ).all()

                for invocation in dead_letter_candidates:
                    logger.warning(
                        f"Moving invocation {invocation.id} to dead letter queue "
                        f"after {invocation.retry_count} failed retries"
                    )
                    invocation.status = InvocationStatus.DEAD_LETTER
                    invocation.dead_letter_reason = (
                        f"Failed after {invocation.retry_count} retry attempts. "
                        f"Last error: {invocation.error_message}"
                    )
                    session.add(invocation)

                # Find failed invocations eligible for retry
                failed_invocations = session.exec(
                    select(InvocationRecord)
                    .where(
                        InvocationRecord.status == InvocationStatus.FAILED,
                        InvocationRecord.retry_count < InvocationRecord.max_retries,
                        InvocationRecord.next_retry_at <= now,
                    )
                ).all()

                for invocation in failed_invocations:
                    logger.info(
                        f"Retrying invocation {invocation.id} "
                        f"(attempt {invocation.retry_count + 1}/{invocation.max_retries})"
                    )

                    # Reset to queued status
                    invocation.status = InvocationStatus.QUEUED
                    invocation.retry_count += 1
                    invocation.started_at = None
                    invocation.completed_at = None
                    invocation.worker_id = None
                    invocation.next_retry_at = None
                    session.add(invocation)

                session.commit()
        except Exception as e:
            logger.error(f"Error in retry processor: {e}")


async def _monitor_worker_health():
    """Background task to detect stale workers and mark them offline."""
    while True:
        try:
            await asyncio.sleep(30)  # Check every 30 seconds

            with get_session() as session:
                now = datetime.now(timezone.utc)
                heartbeat_timeout = timedelta(seconds=60)  # Consider dead after 60s

                # Find workers with stale heartbeats
                stale_workers = []
                workers = session.exec(
                    select(WorkerRecord)
                    .where(WorkerRecord.status != WorkerStatus.OFFLINE)
                ).all()
                
                for worker in workers:
                    # Handle both timezone-aware and naive datetimes
                    if worker.last_heartbeat.tzinfo is None:
                        # Assume naive datetime is in UTC
                        worker_heartbeat = worker.last_heartbeat.replace(tzinfo=timezone.utc)
                    else:
                        worker_heartbeat = worker.last_heartbeat
                    
                    if worker_heartbeat < now - heartbeat_timeout:
                        stale_workers.append(worker)

                for worker in stale_workers:
                    logger.warning(
                        f"Worker {worker.worker_id} heartbeat timeout "
                        f"(last seen {(now - worker.last_heartbeat).total_seconds():.0f}s ago), "
                        f"marking as OFFLINE"
                    )

                    worker.status = WorkerStatus.OFFLINE

                    # Requeue current task if any
                    if worker.current_invocation_id:
                        invocation = session.get(InvocationRecord, worker.current_invocation_id)
                        if invocation and invocation.status == InvocationStatus.RUNNING:
                            logger.info(
                                f"Requeuing invocation {invocation.id} from offline worker {worker.worker_id}"
                            )
                            invocation.status = InvocationStatus.QUEUED
                            invocation.started_at = None
                            invocation.worker_id = None
                            # Don't increment retry count for worker failures
                            session.add(invocation)

                        worker.current_invocation_id = None

                    session.add(worker)

                if stale_workers:
                    session.commit()
                    logger.info(f"Marked {len(stale_workers)} workers as offline")

        except Exception as e:
            logger.error(f"Error in worker health monitor: {e}")


# === App Lifecycle ===

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    # Startup
    logger.info("Starting MiniModal Control Plane")
    init_db()
    logger.info("Database initialized")

    # Initialize routers with dependencies
    init_health_router(get_session, server_state, check_db_connection)
    init_functions_router(get_session, get_user_id_from_request, get_or_create_user)
    init_workers_router(get_session, ws_manager)
    init_volumes_router(get_session, VOLUME_STORAGE_PATH)
    init_secrets_router(get_session)
    init_users_router(get_session)
    init_web_endpoints_router(get_session)
    init_schedules_router(get_session)
    init_dashboard_router(
        get_session, jinja_env,
        stats, stats_functions, stats_workers, stats_invocations
    )
    logger.info("Routers initialized")

    # Start cron scheduler for scheduled functions
    from minimodal.server.cron_scheduler import start_cron_scheduler, stop_cron_scheduler
    start_cron_scheduler(engine)
    logger.info("Cron scheduler started")

    # Initialize and start the task scheduler
    task_scheduler = init_scheduler(get_session, ws_manager)
    await task_scheduler.sync_from_database()
    await task_scheduler.start()
    logger.info("Task scheduler started")

    # Initialize and start the autoscaler
    autoscaling_enabled = os.environ.get("MINIMODAL_AUTOSCALING", "false").lower() == "true"
    autoscaler = None
    if autoscaling_enabled:
        # Determine worker image - build if needed
        import sys
        from minimodal.server.builder import ImageBuilder

        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
        default_worker_image = os.environ.get("MINIMODAL_WORKER_IMAGE")

        if not default_worker_image:
            # Build the worker image for current Python version if not specified
            builder = ImageBuilder()
            try:
                default_worker_image = builder.build_worker_image(python_version)
                logger.info(f"Worker image ready: {default_worker_image}")
            except Exception as e:
                logger.error(f"Failed to build worker image: {e}")
                default_worker_image = f"minimodal-worker:{python_version}"

        container_manager = get_container_manager()
        autoscaler = init_autoscaler(
            scheduler=task_scheduler,
            ws_manager=ws_manager,
            container_manager=container_manager,
            min_workers=int(os.environ.get("MINIMODAL_MIN_WORKERS", "0")),
            max_workers=int(os.environ.get("MINIMODAL_MAX_WORKERS", "10")),
            scale_down_delay=float(os.environ.get("MINIMODAL_SCALE_DOWN_DELAY", "60")),
            default_image=default_worker_image,
        )
        await autoscaler.start()
        logger.info("Autoscaler started")
    else:
        logger.info("Autoscaling disabled (set MINIMODAL_AUTOSCALING=true to enable)")

    # Start background tasks
    retry_task = asyncio.create_task(_process_retries())
    health_task = asyncio.create_task(_monitor_worker_health())
    logger.info("Background tasks started")

    server_state.mark_ready()
    logger.info("Server ready to accept requests")

    yield

    # Shutdown
    logger.info("Shutting down...")
    server_state.mark_shutting_down()

    # Stop autoscaler
    if autoscaler:
        await autoscaler.stop()
        logger.info("Autoscaler stopped")

    # Stop task scheduler
    await task_scheduler.stop()
    logger.info("Task scheduler stopped")

    # Cancel background tasks
    retry_task.cancel()
    health_task.cancel()
    try:
        await retry_task
    except asyncio.CancelledError:
        pass
    try:
        await health_task
    except asyncio.CancelledError:
        pass
    logger.info("Background tasks stopped")

    # Stop cron scheduler
    stop_cron_scheduler()
    logger.info("Cron scheduler stopped")

    logger.info("Shutdown complete")


# === FastAPI App ===

app = FastAPI(
    title="MiniModal Control Plane",
    description="A minimal serverless platform",
    version="0.1.0",
    lifespan=lifespan,
)

# Include all routers
app.include_router(health_router)
app.include_router(functions_router)
app.include_router(workers_router)
app.include_router(volumes_router)
app.include_router(secrets_router)
app.include_router(users_router)
app.include_router(web_endpoints_router)
app.include_router(schedules_router)
app.include_router(dashboard_router)


# === Main ===

def main():
    """Run the server."""
    import uvicorn

    host = os.environ.get("MINIMODAL_HOST", "0.0.0.0")
    port = int(os.environ.get("MINIMODAL_PORT", "8000"))

    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
