"""Function deployment and invocation endpoints."""

import base64
import json
import logging

from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from sqlmodel import select

from minimodal.server.models import (
    BatchInvokeRequest,
    BatchInvokeResponse,
    DeployRequest,
    DeployResponse,
    FunctionRecord,
    FunctionStatus,
    InvocationRecord,
    InvocationStatus,
    InvokeRequest,
    InvokeResponse,
    ResultResponse,
    StreamChunkRecord,
    StreamChunkResponse,
    StreamResultsResponse,
)
from minimodal.server.task_scheduler import get_scheduler
from minimodal.server.websocket import streaming_manager

logger = logging.getLogger("minimodal.server")

router = APIRouter(tags=["Functions"])

# These will be set by api.py during startup
_get_session = None
_get_user_id_from_request = None
_get_or_create_user = None


def init_functions_router(get_session_func, get_user_id_func, get_or_create_user_func):
    global _get_session, _get_user_id_from_request, _get_or_create_user
    _get_session = get_session_func
    _get_user_id_from_request = get_user_id_func
    _get_or_create_user = get_or_create_user_func


@router.post("/deploy", response_model=DeployResponse)
def deploy_function(request: DeployRequest):
    """
    Deploy a function.

    This registers the function and optionally builds a Docker image
    if custom image configuration is provided and Docker is available.
    """
    # Serialize image config to JSON if provided
    image_config_json = None
    if request.image_config:
        image_config_json = json.dumps(request.image_config)

    # Serialize volume mounts and secrets to JSON
    volume_mounts_json = (
        json.dumps(request.volume_mounts) if request.volume_mounts else None
    )
    secrets_json = json.dumps(request.secrets) if request.secrets else None

    # Try to build image if image_config is provided
    image_tag = None
    if request.image_config:
        try:
            from minimodal.sdk.image import Image
            from minimodal.server.builder import ImageBuilder

            builder = ImageBuilder()
            if builder.is_available():
                image = Image.from_dict(request.image_config)
                image_tag = builder.build_from_image_config(image)
                logger.info(f"Built Docker image: {image_tag}")
        except Exception as e:
            logger.warning(f"Failed to build Docker image: {e}")

    with _get_session() as session:
        existing = session.exec(
            select(FunctionRecord).where(
                FunctionRecord.app_name == request.app_name,
                FunctionRecord.name == request.function_name,
            )
        ).first()

        if existing:
            existing.pickled_code = base64.b64decode(request.pickled_code)
            existing.python_version = request.python_version
            existing.requirements = "\n".join(request.requirements)
            existing.required_cpu = request.cpu
            existing.required_memory = request.memory
            existing.image_config = image_config_json
            existing.volume_mounts = volume_mounts_json
            existing.secrets = secrets_json
            existing.timeout_seconds = request.timeout
            existing.max_retries = request.max_retries
            existing.retry_backoff_base = request.retry_backoff_base
            if image_tag:
                existing.image_tag = image_tag
            existing.status = FunctionStatus.READY
            session.add(existing)
            session.commit()
            session.refresh(existing)
            return DeployResponse(
                function_id=existing.id,
                status=existing.status,
            )

        func_record = FunctionRecord(
            name=request.function_name,
            app_name=request.app_name,
            pickled_code=base64.b64decode(request.pickled_code),
            python_version=request.python_version,
            requirements="\n".join(request.requirements),
            required_cpu=request.cpu,
            required_memory=request.memory,
            image_config=image_config_json,
            image_tag=image_tag,
            volume_mounts=volume_mounts_json,
            secrets=secrets_json,
            timeout_seconds=request.timeout,
            max_retries=request.max_retries,
            retry_backoff_base=request.retry_backoff_base,
            status=FunctionStatus.READY,
        )
        session.add(func_record)
        session.commit()
        session.refresh(func_record)

        return DeployResponse(
            function_id=func_record.id,
            status=func_record.status,
        )


@router.post("/invoke", response_model=InvokeResponse)
async def invoke_function(request: InvokeRequest, http_request: Request):
    """
    Invoke a deployed function.

    This queues the invocation for a worker to pick up.
    User is identified by X-User-ID header (defaults to 'default').
    """
    user_id = _get_user_id_from_request(http_request)

    with _get_session() as session:
        _get_or_create_user(session, user_id)

        func = session.get(FunctionRecord, request.function_id)
        if not func:
            raise HTTPException(status_code=404, detail="Function not found")
        if func.status != FunctionStatus.READY:
            raise HTTPException(
                status_code=400, detail=f"Function not ready: {func.status}"
            )

        invocation = InvocationRecord(
            function_id=request.function_id,
            pickled_args=base64.b64decode(request.pickled_args),
            status=InvocationStatus.QUEUED,
            user_id=user_id,
        )
        session.add(invocation)
        session.commit()
        session.refresh(invocation)

        volume_mounts = json.loads(func.volume_mounts) if func.volume_mounts else {}
        secrets = json.loads(func.secrets) if func.secrets else []

        task_data = {
            "id": invocation.id,
            "function_id": invocation.function_id,
            "pickled_code": base64.b64encode(func.pickled_code).decode(),
            "pickled_args": base64.b64encode(invocation.pickled_args).decode(),
            "image_tag": func.image_tag,
            "volume_mounts": volume_mounts,
            "secrets": secrets,
            "timeout_seconds": func.timeout_seconds,
            "max_retries": func.max_retries,
            "required_cpu": func.required_cpu,
            "required_memory": func.required_memory,
        }

        scheduler = get_scheduler()
        if scheduler:
            await scheduler.enqueue_task(
                invocation_id=invocation.id,
                function_id=func.id,
                user_id=user_id,
                required_cpu=func.required_cpu,
                required_memory=func.required_memory,
                task_data=task_data,
            )

        return InvokeResponse(
            invocation_id=invocation.id,
            status=invocation.status,
        )


@router.post("/invoke/batch", response_model=BatchInvokeResponse)
async def batch_invoke_function(request: BatchInvokeRequest, http_request: Request):
    """
    Invoke a function multiple times with different arguments.

    This is more efficient than multiple single invokes for parallel workloads
    like map() operations. All invocations are created in a single transaction
    and can be distributed across multiple workers.

    User is identified by X-User-ID header (defaults to 'default').
    """
    user_id = _get_user_id_from_request(http_request)

    func_id = None
    required_cpu = 1
    required_memory = 512
    invocations_data = []
    invocation_ids = []

    with _get_session() as session:
        _get_or_create_user(session, user_id)

        func = session.get(FunctionRecord, request.function_id)
        if not func:
            raise HTTPException(status_code=404, detail="Function not found")
        if func.status != FunctionStatus.READY:
            raise HTTPException(
                status_code=400, detail=f"Function not ready: {func.status}"
            )

        func_id = func.id
        required_cpu = func.required_cpu
        required_memory = func.required_memory

        volume_mounts = json.loads(func.volume_mounts) if func.volume_mounts else {}
        secrets = json.loads(func.secrets) if func.secrets else []

        for pickled_args in request.pickled_args_list:
            decoded_args = base64.b64decode(pickled_args)
            invocation = InvocationRecord(
                function_id=request.function_id,
                pickled_args=decoded_args,
                status=InvocationStatus.QUEUED,
                user_id=user_id,
            )
            session.add(invocation)
            session.flush()  # Get ID without committing
            invocation_ids.append(invocation.id)

            task_data = {
                "id": invocation.id,
                "function_id": invocation.function_id,
                "pickled_code": base64.b64encode(func.pickled_code).decode(),
                "pickled_args": base64.b64encode(decoded_args).decode(),
                "image_tag": func.image_tag,
                "volume_mounts": volume_mounts,
                "secrets": secrets,
                "timeout_seconds": func.timeout_seconds,
                "max_retries": func.max_retries,
                "required_cpu": required_cpu,
                "required_memory": required_memory,
            }
            invocations_data.append((invocation.id, task_data))

        session.commit()

    scheduler = get_scheduler()
    if scheduler:
        for inv_id, task_data in invocations_data:
            await scheduler.enqueue_task(
                invocation_id=inv_id,
                function_id=func_id,
                user_id=user_id,
                required_cpu=required_cpu,
                required_memory=required_memory,
                task_data=task_data,
            )

    return BatchInvokeResponse(
        invocation_ids=invocation_ids,
        status=InvocationStatus.QUEUED,
    )


@router.get("/result/{invocation_id}", response_model=ResultResponse)
def get_result(invocation_id: int):
    """Get the result of an invocation."""
    with _get_session() as session:
        invocation = session.get(InvocationRecord, invocation_id)
        if not invocation:
            raise HTTPException(status_code=404, detail="Invocation not found")

        return ResultResponse(
            invocation_id=invocation.id,
            status=invocation.status,
            pickled_result=(
                base64.b64encode(invocation.pickled_result)
                if invocation.pickled_result
                else None
            ),
            error_message=invocation.error_message,
            result_type=invocation.result_type,
            is_generator=invocation.is_generator,
        )


@router.get("/result/{invocation_id}/stream", response_model=StreamResultsResponse)
def get_stream_results(invocation_id: int, from_sequence: int = 0):
    """
    Get streaming results for a generator function.

    Args:
        invocation_id: The invocation ID
        from_sequence: Start fetching from this sequence number (for pagination)

    Returns:
        StreamResultsResponse with chunks and status
    """
    with _get_session() as session:
        invocation = session.get(InvocationRecord, invocation_id)
        if not invocation:
            raise HTTPException(status_code=404, detail="Invocation not found")

        # Fetch chunks starting from the requested sequence
        chunks = session.exec(
            select(StreamChunkRecord)
            .where(
                StreamChunkRecord.invocation_id == invocation_id,
                StreamChunkRecord.sequence >= from_sequence,
            )
            .order_by(StreamChunkRecord.sequence)
        ).all()

        return StreamResultsResponse(
            invocation_id=invocation.id,
            status=invocation.status,
            result_type=invocation.result_type,
            chunks=[
                StreamChunkResponse(
                    sequence=c.sequence,
                    pickled_value=base64.b64encode(c.pickled_value),
                )
                for c in chunks
            ],
            total_chunks=invocation.stream_chunks_count,
            error_message=invocation.error_message,
        )


@router.websocket("/ws/stream/{invocation_id}")
async def stream_websocket(websocket: WebSocket, invocation_id: int):
    """
    WebSocket endpoint for receiving streaming results in real-time.

    Clients connect here to receive stream chunks as they arrive from workers.
    No polling required - chunks are pushed immediately.

    Protocol:
    - Server sends: {"type": "chunk", "sequence": int, "pickled_value": str}
    - Server sends: {"type": "complete", "total_chunks": int}
    - Server sends: {"type": "error", "error": str}
    """
    # Verify invocation exists
    with _get_session() as session:
        invocation = session.get(InvocationRecord, invocation_id)
        if not invocation:
            await websocket.close(code=404, reason="Invocation not found")
            return

        # If already completed, send existing chunks and close
        if invocation.status == InvocationStatus.COMPLETED:
            await websocket.accept()
            chunks = session.exec(
                select(StreamChunkRecord)
                .where(StreamChunkRecord.invocation_id == invocation_id)
                .order_by(StreamChunkRecord.sequence)
            ).all()

            for chunk in chunks:
                await websocket.send_json(
                    {
                        "type": "chunk",
                        "sequence": chunk.sequence,
                        "pickled_value": base64.b64encode(chunk.pickled_value).decode(),
                    }
                )

            await websocket.send_json(
                {
                    "type": "complete",
                    "total_chunks": len(chunks),
                }
            )
            await websocket.close()
            return

        if invocation.status == InvocationStatus.FAILED:
            await websocket.accept()
            await websocket.send_json(
                {
                    "type": "error",
                    "error": invocation.error_message,
                }
            )
            await websocket.close()
            return

    client = await streaming_manager.connect(websocket, invocation_id)

    try:
        # Send any existing chunks first
        with _get_session() as session:
            existing_chunks = session.exec(
                select(StreamChunkRecord)
                .where(StreamChunkRecord.invocation_id == invocation_id)
                .order_by(StreamChunkRecord.sequence)
            ).all()

            for chunk in existing_chunks:
                await websocket.send_json(
                    {
                        "type": "chunk",
                        "sequence": chunk.sequence,
                        "pickled_value": base64.b64encode(chunk.pickled_value).decode(),
                    }
                )

        # Keep connection open until streaming completes
        while True:
            try:
                # Wait for client messages (ping/pong or disconnect)
                data = await websocket.receive_json()
                # Client can send ping, we respond with pong
                if data.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
            except WebSocketDisconnect:
                break

    finally:
        await streaming_manager.disconnect(invocation_id, client)
