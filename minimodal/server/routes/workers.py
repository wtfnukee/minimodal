"""Worker registration, polling, and WebSocket endpoints."""

import base64
import json
import logging
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from sqlmodel import select

from minimodal.server.models import (
    FunctionRecord,
    InvocationRecord,
    InvocationStatus,
    WorkerRecord,
    WorkerStatus,
    UserRecord,
    WorkerRegisterRequest,
    StreamChunkRecord,
    ResultType,
)
from minimodal.server.task_scheduler import get_scheduler
from minimodal.server.websocket import streaming_manager, result_manager

logger = logging.getLogger("minimodal.server")

router = APIRouter(tags=["Workers"])

# These will be set by api.py during startup
_get_session = None
_ws_manager = None


def init_workers_router(get_session_func, ws_manager):
    """Initialize the workers router with dependencies."""
    global _get_session, _ws_manager
    _get_session = get_session_func
    _ws_manager = ws_manager


@router.post("/internal/register")
def register_worker(request: WorkerRegisterRequest):
    """Register a worker with its resource capabilities."""
    with _get_session() as session:
        # Check if worker already exists
        existing = session.exec(
            select(WorkerRecord).where(WorkerRecord.worker_id == request.worker_id)
        ).first()

        if existing:
            # Update existing worker
            existing.cpu_cores = request.cpu_cores
            existing.memory_mb = request.memory_mb
            existing.status = WorkerStatus.IDLE
            existing.last_heartbeat = datetime.now(timezone.utc)
            session.add(existing)
            session.commit()
            return {"status": "updated", "worker_id": request.worker_id}

        # Create new worker
        worker = WorkerRecord(
            worker_id=request.worker_id,
            cpu_cores=request.cpu_cores,
            memory_mb=request.memory_mb,
        )
        session.add(worker)
        session.commit()

        return {"status": "registered", "worker_id": request.worker_id}


@router.post("/internal/heartbeat")
def worker_heartbeat(worker_id: str):
    """Update worker heartbeat."""
    with _get_session() as session:
        worker = session.exec(
            select(WorkerRecord).where(WorkerRecord.worker_id == worker_id)
        ).first()

        if not worker:
            raise HTTPException(status_code=404, detail="Worker not registered")

        worker.last_heartbeat = datetime.now(timezone.utc)
        session.add(worker)
        session.commit()

        return {"status": "ok"}


@router.post("/internal/unregister")
def unregister_worker(worker_id: str):
    """Unregister a worker."""
    with _get_session() as session:
        worker = session.exec(
            select(WorkerRecord).where(WorkerRecord.worker_id == worker_id)
        ).first()

        if worker:
            session.delete(worker)
            session.commit()

        return {"status": "unregistered"}


@router.get("/internal/poll")
def poll_for_work(worker_id: str = None):
    """
    Workers call this to get work.

    With resource matching: worker only gets tasks it can handle.
    If worker_id not provided, falls back to simple FIFO (backward compatible).
    """
    with _get_session() as session:
        worker = None

        # If worker_id provided, do resource-aware scheduling
        if worker_id:
            worker = session.exec(
                select(WorkerRecord).where(WorkerRecord.worker_id == worker_id)
            ).first()

            if not worker:
                raise HTTPException(
                    status_code=400,
                    detail="Worker not registered. Call /internal/register first.",
                )

            now = datetime.now(timezone.utc)

            # Update heartbeat
            worker.last_heartbeat = now

            # Check circuit breaker
            if not worker.is_healthy:
                circuit_open_until = worker.circuit_open_until
                if circuit_open_until and circuit_open_until.tzinfo is None:
                    circuit_open_until = circuit_open_until.replace(tzinfo=timezone.utc)
                if circuit_open_until and now >= circuit_open_until:
                    # Try to close circuit
                    worker.is_healthy = True
                    worker.failure_count = 0
                    worker.failure_window_start = None
                    worker.status = WorkerStatus.IDLE
                    logger.info(f"Worker {worker_id} circuit breaker closed, attempting recovery")
                else:
                    session.add(worker)
                    session.commit()
                    return {"invocation": None, "reason": "circuit_breaker_open"}

            # Check if worker is already busy
            if worker.status == WorkerStatus.BUSY:
                return {"invocation": None, "reason": "worker_busy"}

            # Find a task this worker can handle (resource matching)
            results = session.exec(
                select(InvocationRecord, FunctionRecord)
                .join(FunctionRecord, InvocationRecord.function_id == FunctionRecord.id)
                .where(InvocationRecord.status == InvocationStatus.QUEUED)
                .where(FunctionRecord.required_cpu <= worker.cpu_cores)
                .where(FunctionRecord.required_memory <= worker.memory_mb)
                .order_by(InvocationRecord.created_at)
                .limit(1)
            ).first()

            if not results:
                worker.status = WorkerStatus.IDLE
                session.add(worker)
                session.commit()
                return {"invocation": None}

            invocation, func = results
        else:
            # Backward compatible: simple FIFO without resource matching
            invocation = session.exec(
                select(InvocationRecord)
                .where(InvocationRecord.status == InvocationStatus.QUEUED)
                .order_by(InvocationRecord.created_at)
                .limit(1)
            ).first()

            if not invocation:
                return {"invocation": None}

            func = session.get(FunctionRecord, invocation.function_id)

        # Mark invocation as running
        now = datetime.now(timezone.utc)
        invocation.status = InvocationStatus.RUNNING
        invocation.started_at = now
        invocation.timeout_at = now + timedelta(seconds=func.timeout_seconds)
        invocation.max_retries = func.max_retries
        if worker_id:
            invocation.worker_id = worker_id
        session.add(invocation)

        # Mark worker as busy
        if worker:
            worker.status = WorkerStatus.BUSY
            worker.current_invocation_id = invocation.id
            session.add(worker)

        session.commit()

        # Parse volume mounts and secrets from JSON
        volume_mounts = json.loads(func.volume_mounts) if func.volume_mounts else {}
        secrets = json.loads(func.secrets) if func.secrets else []

        return {
            "invocation": {
                "id": invocation.id,
                "function_id": invocation.function_id,
                "pickled_code": base64.b64encode(func.pickled_code).decode(),
                "pickled_args": base64.b64encode(invocation.pickled_args).decode(),
                "image_tag": func.image_tag,
                "volume_mounts": volume_mounts,
                "secrets": secrets,
                "timeout_seconds": func.timeout_seconds,
            }
        }


@router.post("/internal/complete/{invocation_id}")
async def complete_invocation(
    invocation_id: int,
    worker_id: str = None,
    pickled_result: str = None,
    error: str = None,
):
    """Workers call this to report completion."""
    with _get_session() as session:
        invocation = session.get(InvocationRecord, invocation_id)
        if not invocation:
            raise HTTPException(status_code=404, detail="Invocation not found")

        now = datetime.now(timezone.utc)

        # Get function config for retry settings
        func = session.get(FunctionRecord, invocation.function_id)

        if error:
            invocation.status = InvocationStatus.FAILED
            invocation.error_message = error

            # Set retry timing if retries remaining
            if func and invocation.retry_count < invocation.max_retries:
                # Exponential backoff: base^retry_count seconds
                delay = func.retry_backoff_base ** invocation.retry_count
                invocation.next_retry_at = now + timedelta(seconds=delay)
                logger.info(
                    f"Will retry invocation {invocation_id} in {delay}s "
                    f"(attempt {invocation.retry_count + 1}/{invocation.max_retries})"
                )
        else:
            invocation.status = InvocationStatus.COMPLETED
            if pickled_result:
                invocation.pickled_result = base64.b64decode(pickled_result)

        invocation.completed_at = now
        session.add(invocation)

        # Mark worker as idle and update stats + circuit breaker
        if worker_id:
            worker = session.exec(
                select(WorkerRecord).where(WorkerRecord.worker_id == worker_id)
            ).first()
            if worker:
                worker.status = WorkerStatus.IDLE
                worker.current_invocation_id = None

                if error:
                    worker.tasks_failed += 1

                    # Circuit breaker logic
                    # Reset window if it's been > 5 minutes
                    failure_window_start = worker.failure_window_start
                    if failure_window_start is not None and failure_window_start.tzinfo is None:
                        failure_window_start = failure_window_start.replace(tzinfo=timezone.utc)
                    if (failure_window_start is None or
                        (now - failure_window_start).total_seconds() > 300):
                        worker.failure_window_start = now
                        worker.failure_count = 1
                    else:
                        worker.failure_count += 1

                    # Open circuit if 5 failures in 5 minute window
                    if worker.failure_count >= 5:
                        worker.is_healthy = False
                        worker.circuit_open_until = now + timedelta(seconds=60)  # 1 minute cooldown
                        worker.status = WorkerStatus.OFFLINE
                        logger.warning(
                            f"Worker {worker_id} circuit breaker opened due to {worker.failure_count} failures"
                        )
                else:
                    worker.tasks_completed += 1
                    # Reset failure tracking on success
                    worker.failure_count = 0
                    worker.failure_window_start = None

                session.add(worker)

        session.commit()

    # Notify clients waiting for result via WebSocket
    if error:
        await result_manager.send_error(invocation_id, error)
    else:
        await result_manager.send_result(invocation_id, pickled_result)

    return {"status": "ok"}


@router.websocket("/ws/worker/{worker_id}")
async def worker_websocket(
    websocket: WebSocket,
    worker_id: str,
    cpu_cores: int = 1,
    memory_mb: int = 1024,
):
    """
    WebSocket endpoint for workers to receive tasks.

    Workers connect here instead of polling /internal/poll.
    Tasks are pushed to workers as they become available.

    Protocol:
    - Server sends: {"type": "task", "task": {...}} when work is available
    - Worker sends: {"type": "result", "invocation_id": int, "pickled_result": str}
    - Worker sends: {"type": "error", "invocation_id": int, "error": str}
    - Server sends: {"type": "ping"} for keep-alive
    - Worker sends: {"type": "pong"} in response to ping
    """
    # Register worker
    await _ws_manager.connect(
        websocket=websocket,
        worker_id=worker_id,
        cpu_cores=cpu_cores,
        memory_mb=memory_mb,
    )

    # Also register in database
    with _get_session() as session:
        existing = session.exec(
            select(WorkerRecord).where(WorkerRecord.worker_id == worker_id)
        ).first()

        if existing:
            existing.cpu_cores = cpu_cores
            existing.memory_mb = memory_mb
            existing.status = WorkerStatus.IDLE
            existing.last_heartbeat = datetime.now(timezone.utc)
            session.add(existing)
        else:
            worker_record = WorkerRecord(
                worker_id=worker_id,
                cpu_cores=cpu_cores,
                memory_mb=memory_mb,
                status=WorkerStatus.IDLE,
            )
            session.add(worker_record)
        session.commit()

    logger.info(f"WebSocket worker {worker_id} connected (cpu={cpu_cores}, memory={memory_mb}MB)")

    try:
        while True:
            # Receive messages from worker
            data = await websocket.receive_json()
            msg_type = data.get("type")

            if msg_type == "result":
                # Worker completed a task successfully
                invocation_id = data.get("invocation_id")
                pickled_result = data.get("pickled_result")
                user_id = None

                with _get_session() as session:
                    invocation = session.get(InvocationRecord, invocation_id)
                    if invocation:
                        user_id = invocation.user_id
                        invocation.status = InvocationStatus.COMPLETED
                        if pickled_result:
                            invocation.pickled_result = base64.b64decode(pickled_result)
                        invocation.completed_at = datetime.now(timezone.utc)
                        session.add(invocation)

                        # Update user active_count
                        db_user = session.get(UserRecord, user_id)
                        if db_user and db_user.active_count > 0:
                            db_user.active_count -= 1
                            db_user.updated_at = datetime.now(timezone.utc)
                            session.add(db_user)

                        # Update worker stats
                        db_worker = session.exec(
                            select(WorkerRecord).where(WorkerRecord.worker_id == worker_id)
                        ).first()
                        if db_worker:
                            db_worker.status = WorkerStatus.IDLE
                            db_worker.current_invocation_id = None
                            db_worker.tasks_completed += 1
                            db_worker.last_heartbeat = datetime.now(timezone.utc)
                            # Reset failure tracking on success
                            db_worker.failure_count = 0
                            db_worker.failure_window_start = None
                            session.add(db_worker)

                        session.commit()

                # Notify scheduler that task completed
                scheduler = get_scheduler()
                if scheduler and user_id:
                    await scheduler.task_completed(user_id, invocation_id)

                # Notify clients waiting for result via WebSocket
                await result_manager.send_result(invocation_id, pickled_result)

                # Mark WebSocket worker as available
                await _ws_manager.mark_task_complete(worker_id)
                logger.info(f"Task {invocation_id} completed by WebSocket worker {worker_id}")

            elif msg_type == "error":
                # Worker encountered an error
                invocation_id = data.get("invocation_id")
                error_msg = data.get("error")
                user_id = None

                with _get_session() as session:
                    invocation = session.get(InvocationRecord, invocation_id)
                    if invocation:
                        user_id = invocation.user_id
                        func = session.get(FunctionRecord, invocation.function_id)
                        now = datetime.now(timezone.utc)

                        invocation.status = InvocationStatus.FAILED
                        invocation.error_message = error_msg
                        invocation.completed_at = now

                        # Set retry timing if retries remaining
                        if func and invocation.retry_count < invocation.max_retries:
                            delay = func.retry_backoff_base ** invocation.retry_count
                            invocation.next_retry_at = now + timedelta(seconds=delay)

                        session.add(invocation)

                        # Update user active_count
                        db_user = session.get(UserRecord, user_id)
                        if db_user and db_user.active_count > 0:
                            db_user.active_count -= 1
                            db_user.updated_at = now
                            session.add(db_user)

                        # Update worker stats
                        db_worker = session.exec(
                            select(WorkerRecord).where(WorkerRecord.worker_id == worker_id)
                        ).first()
                        if db_worker:
                            db_worker.status = WorkerStatus.IDLE
                            db_worker.current_invocation_id = None
                            db_worker.tasks_failed += 1
                            db_worker.last_heartbeat = now

                            # Circuit breaker logic
                            failure_window_start = db_worker.failure_window_start
                            if failure_window_start is not None and failure_window_start.tzinfo is None:
                                failure_window_start = failure_window_start.replace(tzinfo=timezone.utc)
                            if (failure_window_start is None or
                                (now - failure_window_start).total_seconds() > 300):
                                db_worker.failure_window_start = now
                                db_worker.failure_count = 1
                            else:
                                db_worker.failure_count += 1

                            if db_worker.failure_count >= 5:
                                db_worker.is_healthy = False
                                db_worker.circuit_open_until = now + timedelta(seconds=60)
                                db_worker.status = WorkerStatus.OFFLINE
                                logger.warning(f"WebSocket worker {worker_id} circuit breaker opened")

                            session.add(db_worker)

                        session.commit()

                # Notify scheduler that task failed
                scheduler = get_scheduler()
                if scheduler and user_id:
                    await scheduler.task_failed(user_id, invocation_id)

                # Notify clients waiting for result via WebSocket
                await result_manager.send_error(invocation_id, error_msg or "Unknown error")

                # Mark WebSocket worker as available
                await _ws_manager.mark_task_complete(worker_id)
                logger.info(f"Task {invocation_id} failed on WebSocket worker {worker_id}")

            elif msg_type == "pong":
                # Response to keep-alive ping
                with _get_session() as session:
                    db_worker = session.exec(
                        select(WorkerRecord).where(WorkerRecord.worker_id == worker_id)
                    ).first()
                    if db_worker:
                        db_worker.last_heartbeat = datetime.now(timezone.utc)
                        session.add(db_worker)
                        session.commit()

            elif msg_type == "stream_start":
                # Worker is starting to stream results from a generator
                invocation_id = data.get("invocation_id")
                with _get_session() as session:
                    invocation = session.get(InvocationRecord, invocation_id)
                    if invocation:
                        invocation.result_type = ResultType.STREAM
                        invocation.is_generator = True
                        session.add(invocation)
                        session.commit()
                logger.info(f"Streaming started for invocation {invocation_id}")

            elif msg_type == "stream_chunk":
                # Worker is sending a chunk from a generator
                invocation_id = data.get("invocation_id")
                sequence = data.get("sequence")
                pickled_value = data.get("pickled_value")

                with _get_session() as session:
                    # Store the chunk
                    chunk = StreamChunkRecord(
                        invocation_id=invocation_id,
                        sequence=sequence,
                        pickled_value=base64.b64decode(pickled_value),
                    )
                    session.add(chunk)

                    # Update chunk count
                    invocation = session.get(InvocationRecord, invocation_id)
                    if invocation:
                        invocation.stream_chunks_count = sequence + 1
                        session.add(invocation)

                    session.commit()

                # Forward chunk to connected clients via WebSocket
                await streaming_manager.send_chunk(invocation_id, sequence, pickled_value)
                logger.debug(f"Received chunk {sequence} for invocation {invocation_id}")

            elif msg_type == "stream_end":
                # Worker finished streaming a generator
                invocation_id = data.get("invocation_id")
                total_chunks = data.get("total_chunks", 0)
                user_id = None

                with _get_session() as session:
                    invocation = session.get(InvocationRecord, invocation_id)
                    if invocation:
                        user_id = invocation.user_id
                        invocation.status = InvocationStatus.COMPLETED
                        invocation.completed_at = datetime.now(timezone.utc)
                        invocation.stream_chunks_count = total_chunks
                        session.add(invocation)

                        # Update user active_count
                        db_user = session.get(UserRecord, user_id)
                        if db_user and db_user.active_count > 0:
                            db_user.active_count -= 1
                            db_user.updated_at = datetime.now(timezone.utc)
                            session.add(db_user)

                        # Update worker stats
                        db_worker = session.exec(
                            select(WorkerRecord).where(WorkerRecord.worker_id == worker_id)
                        ).first()
                        if db_worker:
                            db_worker.status = WorkerStatus.IDLE
                            db_worker.current_invocation_id = None
                            db_worker.tasks_completed += 1
                            db_worker.last_heartbeat = datetime.now(timezone.utc)
                            db_worker.failure_count = 0
                            db_worker.failure_window_start = None
                            session.add(db_worker)

                        session.commit()

                # Notify scheduler that task completed
                scheduler = get_scheduler()
                if scheduler and user_id:
                    await scheduler.task_completed(user_id, invocation_id)

                # Notify connected clients that streaming is complete
                await streaming_manager.send_complete(invocation_id, total_chunks)

                # Mark WebSocket worker as available
                await _ws_manager.mark_task_complete(worker_id)
                logger.info(f"Streaming completed for invocation {invocation_id} ({total_chunks} chunks)")

            elif msg_type == "stream_error":
                # Worker encountered an error during streaming
                invocation_id = data.get("invocation_id")
                error_msg = data.get("error")
                user_id = None

                with _get_session() as session:
                    invocation = session.get(InvocationRecord, invocation_id)
                    if invocation:
                        user_id = invocation.user_id
                        invocation.status = InvocationStatus.FAILED
                        invocation.error_message = error_msg
                        invocation.completed_at = datetime.now(timezone.utc)
                        session.add(invocation)

                        # Update user active_count
                        db_user = session.get(UserRecord, user_id)
                        if db_user and db_user.active_count > 0:
                            db_user.active_count -= 1
                            db_user.updated_at = datetime.now(timezone.utc)
                            session.add(db_user)

                        # Update worker stats
                        db_worker = session.exec(
                            select(WorkerRecord).where(WorkerRecord.worker_id == worker_id)
                        ).first()
                        if db_worker:
                            db_worker.status = WorkerStatus.IDLE
                            db_worker.current_invocation_id = None
                            db_worker.tasks_failed += 1
                            db_worker.last_heartbeat = datetime.now(timezone.utc)
                            session.add(db_worker)

                        session.commit()

                # Notify connected clients of the error
                await streaming_manager.send_error(invocation_id, error_msg)

                # Notify scheduler that task failed
                scheduler = get_scheduler()
                if scheduler and user_id:
                    await scheduler.task_failed(user_id, invocation_id)

                # Mark WebSocket worker as available
                await _ws_manager.mark_task_complete(worker_id)
                logger.info(f"Streaming failed for invocation {invocation_id}: {error_msg}")

    except WebSocketDisconnect:
        logger.info(f"WebSocket worker {worker_id} disconnected")
    except Exception as e:
        logger.error(f"WebSocket error for worker {worker_id}: {e}")
    finally:
        # Clean up
        await _ws_manager.disconnect(worker_id)

        # Update database
        with _get_session() as session:
            db_worker = session.exec(
                select(WorkerRecord).where(WorkerRecord.worker_id == worker_id)
            ).first()
            if db_worker:
                # Requeue current task if any
                if db_worker.current_invocation_id:
                    invocation = session.get(InvocationRecord, db_worker.current_invocation_id)
                    if invocation and invocation.status == InvocationStatus.RUNNING:
                        invocation.status = InvocationStatus.QUEUED
                        invocation.started_at = None
                        invocation.worker_id = None
                        session.add(invocation)
                        logger.info(f"Requeued task {invocation.id} from disconnected worker {worker_id}")

                db_worker.status = WorkerStatus.OFFLINE
                db_worker.current_invocation_id = None
                session.add(db_worker)
                session.commit()


@router.get("/workers/health")
def get_worker_health():
    """Get health status of all workers."""
    with _get_session() as session:
        now = datetime.now(timezone.utc)
        workers = session.exec(select(WorkerRecord)).all()

        healthy = []
        unhealthy = []
        offline = []

        for worker in workers:
            # Handle timezone-naive datetimes
            last_heartbeat = worker.last_heartbeat
            if last_heartbeat.tzinfo is None:
                last_heartbeat = last_heartbeat.replace(tzinfo=timezone.utc)
            heartbeat_age = (now - last_heartbeat).total_seconds()

            worker_info = {
                "worker_id": worker.worker_id,
                "status": worker.status.value,
                "is_healthy": worker.is_healthy,
                "heartbeat_age_seconds": heartbeat_age,
                "tasks_completed": worker.tasks_completed,
                "tasks_failed": worker.tasks_failed,
                "failure_count": worker.failure_count,
                "circuit_breaker_open": not worker.is_healthy,
            }

            if worker.status == WorkerStatus.OFFLINE:
                offline.append(worker_info)
            elif heartbeat_age < 30:
                healthy.append(worker_info)
            else:
                unhealthy.append(worker_info)

        return {
            "healthy": healthy,
            "unhealthy": unhealthy,
            "offline": offline,
            "summary": {
                "total": len(workers),
                "healthy_count": len(healthy),
                "unhealthy_count": len(unhealthy),
                "offline_count": len(offline),
            },
        }
