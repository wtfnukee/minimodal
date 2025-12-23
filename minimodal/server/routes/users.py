"""User management endpoints."""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from sqlmodel import select

from minimodal.server.models import (
    UserRecord,
    UserResponse,
    UserListResponse,
)
from minimodal.server.task_scheduler import get_scheduler

router = APIRouter(prefix="/users", tags=["Users"])

# These will be set by api.py during startup
_get_session = None


def init_users_router(get_session_func):
    """Initialize the users router with dependencies."""
    global _get_session
    _get_session = get_session_func


@router.get("", response_model=UserListResponse)
def list_users():
    """List all users with their quota information."""
    with _get_session() as session:
        users = session.exec(select(UserRecord)).all()
        return UserListResponse(
            users=[
                UserResponse(
                    id=u.id,
                    quota_limit=u.quota_limit,
                    active_count=u.active_count,
                    created_at=u.created_at,
                    updated_at=u.updated_at,
                )
                for u in users
            ]
        )


@router.get("/{user_id}", response_model=UserResponse)
def get_user(user_id: str):
    """Get a specific user's information."""
    with _get_session() as session:
        user = session.get(UserRecord, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        return UserResponse(
            id=user.id,
            quota_limit=user.quota_limit,
            active_count=user.active_count,
            created_at=user.created_at,
            updated_at=user.updated_at,
        )


@router.put("/{user_id}/quota")
async def update_user_quota(user_id: str, quota_limit: int):
    """Update a user's quota limit."""
    if quota_limit < 1:
        raise HTTPException(status_code=400, detail="Quota limit must be at least 1")

    with _get_session() as session:
        user = session.get(UserRecord, user_id)
        if not user:
            # Create user with specified quota
            user = UserRecord(id=user_id, quota_limit=quota_limit)
            session.add(user)
        else:
            user.quota_limit = quota_limit
            user.updated_at = datetime.now(timezone.utc)
            session.add(user)
        session.commit()

    # Update scheduler's in-memory quota
    scheduler = get_scheduler()
    if scheduler:
        await scheduler.update_user_quota(user_id, quota_limit)

    return {"status": "updated", "user_id": user_id, "quota_limit": quota_limit}
