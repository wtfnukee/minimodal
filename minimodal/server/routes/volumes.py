"""Volume management endpoints."""

import shutil
from pathlib import Path

from fastapi import APIRouter, HTTPException, File, UploadFile
from fastapi.responses import FileResponse
from sqlmodel import select

from minimodal.server.models import (
    VolumeRecord,
    VolumeCreateRequest,
    VolumeResponse,
    VolumeListResponse,
    VolumeFileInfo,
    VolumeFileListResponse,
)

router = APIRouter(prefix="/volumes", tags=["Volumes"])

# These will be set by api.py during startup
_get_session = None
_volume_storage_path = None


def init_volumes_router(get_session_func, volume_storage_path: Path):
    """Initialize the volumes router with dependencies."""
    global _get_session, _volume_storage_path
    _get_session = get_session_func
    _volume_storage_path = volume_storage_path


def _get_volume_path(volume_name: str) -> Path:
    """Get the filesystem path for a volume."""
    return _volume_storage_path / volume_name


@router.post("", response_model=VolumeResponse)
def create_volume(request: VolumeCreateRequest):
    """Create a new volume."""
    with _get_session() as session:
        # Check if volume already exists
        existing = session.exec(
            select(VolumeRecord).where(VolumeRecord.name == request.name)
        ).first()

        if existing:
            return VolumeResponse(
                id=existing.id,
                name=existing.name,
                size_bytes=existing.size_bytes,
                created_at=existing.created_at,
            )

        # Create volume directory
        volume_path = _get_volume_path(request.name)
        volume_path.mkdir(parents=True, exist_ok=True)

        # Create database record
        volume = VolumeRecord(
            name=request.name,
            storage_path=str(volume_path),
        )
        session.add(volume)
        session.commit()
        session.refresh(volume)

        return VolumeResponse(
            id=volume.id,
            name=volume.name,
            size_bytes=volume.size_bytes,
            created_at=volume.created_at,
        )


@router.get("", response_model=VolumeListResponse)
def list_volumes():
    """List all volumes."""
    with _get_session() as session:
        volumes = session.exec(select(VolumeRecord)).all()
        return VolumeListResponse(
            volumes=[
                VolumeResponse(
                    id=v.id,
                    name=v.name,
                    size_bytes=v.size_bytes,
                    created_at=v.created_at,
                )
                for v in volumes
            ]
        )


@router.get("/{name}", response_model=VolumeResponse)
def get_volume(name: str):
    """Get volume information."""
    with _get_session() as session:
        volume = session.exec(
            select(VolumeRecord).where(VolumeRecord.name == name)
        ).first()

        if not volume:
            raise HTTPException(status_code=404, detail="Volume not found")

        return VolumeResponse(
            id=volume.id,
            name=volume.name,
            size_bytes=volume.size_bytes,
            created_at=volume.created_at,
        )


@router.delete("/{name}")
def delete_volume(name: str):
    """Delete a volume and all its contents."""
    with _get_session() as session:
        volume = session.exec(
            select(VolumeRecord).where(VolumeRecord.name == name)
        ).first()

        if not volume:
            raise HTTPException(status_code=404, detail="Volume not found")

        # Delete the volume directory
        volume_path = Path(volume.storage_path)
        if volume_path.exists():
            shutil.rmtree(volume_path)

        # Delete database record
        session.delete(volume)
        session.commit()

        return {"status": "deleted", "name": name}


@router.post("/{name}/files")
async def upload_volume_file(
    name: str,
    path: str,
    file: UploadFile = File(...),
):
    """Upload a file to a volume."""
    with _get_session() as session:
        volume = session.exec(
            select(VolumeRecord).where(VolumeRecord.name == name)
        ).first()

        if not volume:
            raise HTTPException(status_code=404, detail="Volume not found")

        # Sanitize path
        if not path.startswith("/"):
            path = "/" + path

        # Create file path
        volume_path = Path(volume.storage_path)
        file_path = volume_path / path.lstrip("/")

        # Create parent directories
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Write file content
        content = await file.read()
        with open(file_path, "wb") as f:
            f.write(content)

        # Update volume size
        volume.size_bytes = sum(
            f.stat().st_size for f in volume_path.rglob("*") if f.is_file()
        )
        session.add(volume)
        session.commit()

        return {"status": "uploaded", "path": path, "size": len(content)}


@router.get("/{name}/files/{path:path}")
def download_volume_file(name: str, path: str):
    """Download a file from a volume."""
    with _get_session() as session:
        volume = session.exec(
            select(VolumeRecord).where(VolumeRecord.name == name)
        ).first()

        if not volume:
            raise HTTPException(status_code=404, detail="Volume not found")

        # Build file path
        volume_path = Path(volume.storage_path)
        file_path = volume_path / path

        if not file_path.exists():
            raise HTTPException(status_code=404, detail="File not found")

        if file_path.is_dir():
            raise HTTPException(status_code=400, detail="Path is a directory")

        return FileResponse(file_path)


@router.delete("/{name}/files/{path:path}")
def delete_volume_file(name: str, path: str):
    """Delete a file from a volume."""
    with _get_session() as session:
        volume = session.exec(
            select(VolumeRecord).where(VolumeRecord.name == name)
        ).first()

        if not volume:
            raise HTTPException(status_code=404, detail="Volume not found")

        # Build file path
        volume_path = Path(volume.storage_path)
        file_path = volume_path / path

        if not file_path.exists():
            raise HTTPException(status_code=404, detail="File not found")

        if file_path.is_dir():
            shutil.rmtree(file_path)
        else:
            file_path.unlink()

        # Update volume size
        volume.size_bytes = sum(
            f.stat().st_size for f in volume_path.rglob("*") if f.is_file()
        )
        session.add(volume)
        session.commit()

        return {"status": "deleted", "path": path}


@router.get("/{name}/list", response_model=VolumeFileListResponse)
def list_volume_files(name: str, path: str = "/"):
    """List files in a volume directory."""
    with _get_session() as session:
        volume = session.exec(
            select(VolumeRecord).where(VolumeRecord.name == name)
        ).first()

        if not volume:
            raise HTTPException(status_code=404, detail="Volume not found")

        # Build directory path
        volume_path = Path(volume.storage_path)
        dir_path = volume_path / path.lstrip("/")

        if not dir_path.exists():
            raise HTTPException(status_code=404, detail="Path not found")

        if not dir_path.is_dir():
            raise HTTPException(status_code=400, detail="Path is not a directory")

        files = []
        for item in dir_path.iterdir():
            files.append(
                VolumeFileInfo(
                    name=item.name,
                    is_dir=item.is_dir(),
                    size_bytes=item.stat().st_size if item.is_file() else 0,
                )
            )

        return VolumeFileListResponse(path=path, files=files)
