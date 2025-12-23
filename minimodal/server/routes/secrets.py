"""Secret management endpoints."""

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from sqlmodel import select

from minimodal.server.models import (
    SecretRecord,
    SecretCreateRequest,
    SecretResponse,
    SecretListResponse,
)

router = APIRouter(prefix="/secrets", tags=["Secrets"])

# These will be set by api.py during startup
_get_session = None


def init_secrets_router(get_session_func):
    """Initialize the secrets router with dependencies."""
    global _get_session
    _get_session = get_session_func


@router.post("", response_model=SecretResponse)
def create_secret(request: SecretCreateRequest):
    """Create or update a secret."""
    from minimodal.server.crypto import encrypt_value

    with _get_session() as session:
        # Check if secret already exists
        existing = session.exec(
            select(SecretRecord).where(SecretRecord.name == request.name)
        ).first()

        if existing:
            # Update existing secret
            existing.encrypted_value = encrypt_value(request.value)
            existing.updated_at = datetime.now(timezone.utc)
            session.add(existing)
            session.commit()
            session.refresh(existing)

            return SecretResponse(
                id=existing.id,
                name=existing.name,
                created_at=existing.created_at,
                updated_at=existing.updated_at,
            )

        # Create new secret
        encrypted = encrypt_value(request.value)
        secret = SecretRecord(
            name=request.name,
            encrypted_value=encrypted,
        )
        session.add(secret)
        session.commit()
        session.refresh(secret)

        return SecretResponse(
            id=secret.id,
            name=secret.name,
            created_at=secret.created_at,
            updated_at=secret.updated_at,
        )


@router.get("", response_model=SecretListResponse)
def list_secrets():
    """List all secrets (names only, not values)."""
    with _get_session() as session:
        secrets = session.exec(select(SecretRecord)).all()
        return SecretListResponse(
            secrets=[
                SecretResponse(
                    id=s.id,
                    name=s.name,
                    created_at=s.created_at,
                    updated_at=s.updated_at,
                )
                for s in secrets
            ]
        )


@router.delete("/{name}")
def delete_secret(name: str):
    """Delete a secret."""
    with _get_session() as session:
        secret = session.exec(
            select(SecretRecord).where(SecretRecord.name == name)
        ).first()

        if not secret:
            raise HTTPException(status_code=404, detail="Secret not found")

        session.delete(secret)
        session.commit()

        return {"status": "deleted", "name": name}


@router.get("/internal/{name}")
def get_secret_value(name: str):
    """
    Get a secret's decrypted value.

    This is an internal endpoint used by workers to fetch secrets.
    """
    from minimodal.server.crypto import decrypt_value

    with _get_session() as session:
        secret = session.exec(
            select(SecretRecord).where(SecretRecord.name == name)
        ).first()

        if not secret:
            raise HTTPException(status_code=404, detail="Secret not found")

        decrypted = decrypt_value(secret.encrypted_value)
        return {"name": name, "value": decrypted}
