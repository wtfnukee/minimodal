"""Web endpoint routing for HTTP functions."""

import asyncio
import re
import time

import cloudpickle
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from sqlmodel import select

from minimodal.server.models import (
    FunctionRecord,
    FunctionStatus,
    InvocationRecord,
    InvocationStatus,
    WebEndpointRecord,
)

router = APIRouter(tags=["Web Endpoints"])

# These will be set by api.py during startup
_get_session = None


def init_web_endpoints_router(get_session_func):
    """Initialize the web endpoints router with dependencies."""
    global _get_session
    _get_session = get_session_func


@router.post("/web-endpoints")
def register_web_endpoint(
    path: str,
    method: str,
    function_id: int,
):
    """Register a web endpoint for a function."""
    with _get_session() as session:
        # Verify function exists
        func = session.get(FunctionRecord, function_id)
        if not func:
            raise HTTPException(status_code=404, detail="Function not found")

        # Check if endpoint already exists
        existing = session.exec(
            select(WebEndpointRecord).where(
                WebEndpointRecord.path == path,
                WebEndpointRecord.method == method.upper(),
            )
        ).first()

        if existing:
            # Update existing endpoint
            existing.function_id = function_id
            session.add(existing)
            session.commit()
            return {"id": existing.id, "path": path, "method": method}

        # Create new endpoint
        endpoint = WebEndpointRecord(
            path=path,
            method=method.upper(),
            function_id=function_id,
        )
        session.add(endpoint)
        session.commit()
        session.refresh(endpoint)

        return {"id": endpoint.id, "path": path, "method": method}


@router.get("/web-endpoints")
def list_web_endpoints():
    """List all registered web endpoints."""
    with _get_session() as session:
        endpoints = session.exec(select(WebEndpointRecord)).all()
        return {
            "endpoints": [
                {
                    "id": e.id,
                    "path": e.path,
                    "method": e.method,
                    "function_id": e.function_id,
                }
                for e in endpoints
            ]
        }


def match_path_pattern(pattern: str, path: str) -> dict | None:
    """
    Match a URL path against a pattern with {param} placeholders.

    Returns extracted parameters as a dict, or None if no match.

    Example:
        match_path_pattern("/api/users/{user_id}", "/api/users/123")
        -> {"user_id": "123"}
    """
    # Convert pattern to regex
    # Replace {param} with named capture groups
    regex_pattern = re.sub(r"\{(\w+)\}", r"(?P<\1>[^/]+)", pattern)
    regex_pattern = f"^{regex_pattern}$"

    match = re.match(regex_pattern, path)
    if match:
        return match.groupdict()
    return None


@router.api_route("/web/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def web_router(path: str, request: Request):
    """
    Catch-all route for web endpoints.

    Routes requests to the appropriate function based on path and method.
    Supports path parameters like /api/users/{user_id}.
    """
    # Build the path to match
    full_path = "/" + path

    with _get_session() as session:
        # Find matching endpoint - try exact match first, then pattern match
        endpoint = session.exec(
            select(WebEndpointRecord).where(
                WebEndpointRecord.path == full_path,
                WebEndpointRecord.method == request.method,
            )
        ).first()

        path_params = {}

        # If no exact match, try pattern matching
        if not endpoint:
            endpoints = session.exec(
                select(WebEndpointRecord).where(
                    WebEndpointRecord.method == request.method,
                )
            ).all()

            for ep in endpoints:
                params = match_path_pattern(ep.path, full_path)
                if params is not None:
                    endpoint = ep
                    path_params = params
                    break

        if not endpoint:
            raise HTTPException(
                status_code=404,
                detail=f"No endpoint found for {request.method} {full_path}",
            )

        # Get the function
        func = session.get(FunctionRecord, endpoint.function_id)
        if not func or func.status != FunctionStatus.READY:
            raise HTTPException(
                status_code=503,
                detail="Function not ready",
            )

        # Build request data
        request_data = {
            "method": request.method,
            "path": full_path,
            "path_params": path_params,
            "query_params": dict(request.query_params),
            "headers": dict(request.headers),
        }

        # Parse body for POST/PUT/PATCH
        if request.method in ("POST", "PUT", "PATCH"):
            try:
                body = await request.json()
                request_data["body"] = body
            except Exception:
                request_data["body"] = None

        # Create invocation
        pickled_args = cloudpickle.dumps(((), {"request": request_data}))

        invocation = InvocationRecord(
            function_id=endpoint.function_id,
            pickled_args=pickled_args,
            status=InvocationStatus.QUEUED,
        )
        session.add(invocation)
        session.commit()
        session.refresh(invocation)

        invocation_id = invocation.id

    # Wait for result (async polling)
    timeout = 30.0
    poll_interval = 0.1
    start = time.time()

    while time.time() - start < timeout:
        with _get_session() as session:
            invocation = session.get(InvocationRecord, invocation_id)

            if invocation.status == InvocationStatus.COMPLETED:
                if invocation.pickled_result:
                    result = cloudpickle.loads(invocation.pickled_result)

                    # Handle different response types
                    if isinstance(result, dict):
                        return result
                    elif isinstance(result, tuple):
                        if len(result) == 2:
                            body, status = result
                            return Response(
                                content=str(body) if not isinstance(body, (dict, list)) else None,
                                media_type="application/json" if isinstance(body, (dict, list)) else "text/plain",
                                status_code=status,
                            )
                        elif len(result) == 3:
                            body, status, headers = result
                            return Response(
                                content=str(body) if not isinstance(body, (dict, list)) else None,
                                media_type="application/json",
                                status_code=status,
                                headers=headers,
                            )
                    else:
                        return {"result": result}
                return {"status": "completed"}

            if invocation.status == InvocationStatus.FAILED:
                raise HTTPException(
                    status_code=500,
                    detail=f"Function failed: {invocation.error_message}",
                )

        await asyncio.sleep(poll_interval)

    raise HTTPException(status_code=504, detail="Function timed out")
