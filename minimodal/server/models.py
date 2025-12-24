"""Core data models for minimodal."""

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel
from sqlmodel import Field, SQLModel


def utc_now() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(timezone.utc)


# === Enums ===

class FunctionStatus(str, Enum):
    PENDING = "pending"      # Registered, image not built
    BUILDING = "building"    # Image being built
    READY = "ready"          # Ready to execute
    FAILED = "failed"        # Build failed


class InvocationStatus(str, Enum):
    QUEUED = "queued"        # Waiting for worker
    RUNNING = "running"      # Being executed
    COMPLETED = "completed"  # Finished successfully
    FAILED = "failed"        # Execution failed
    DEAD_LETTER = "dead_letter"  # Permanently failed after all retries


class ResultType(str, Enum):
    SINGLE = "single"        # Single result (default)
    STREAM = "stream"        # Multiple streaming results (generator)


class WorkerStatus(str, Enum):
    IDLE = "idle"            # Ready for work
    BUSY = "busy"            # Processing a task
    DRAINING = "draining"    # Finishing current task, then stopping
    OFFLINE = "offline"      # Not responding


# === Database Models ===

class FunctionRecord(SQLModel, table=True):
    """A registered serverless function."""
    __tablename__ = "functions"

    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(index=True)
    app_name: str = Field(index=True)

    # Serialized function code (cloudpickle)
    pickled_code: bytes

    # Image configuration
    python_version: str = "3.11"
    requirements: str = ""  # newline-separated pip packages
    image_tag: str | None = None  # Docker image tag once built
    image_config: str | None = None  # JSON-serialized Image config

    # Resource requirements
    required_cpu: int = Field(default=1)  # Number of CPU cores
    required_memory: int = Field(default=512)  # Memory in MB

    # Volume mounts: JSON dict {"/mount/path": "volume-name"}
    volume_mounts: str | None = None

    # Secrets: JSON list ["SECRET_NAME_1", "SECRET_NAME_2"]
    secrets: str | None = None

    # Execution configuration
    timeout_seconds: int = Field(default=300)  # Task timeout (5 min default)
    max_retries: int = Field(default=3)  # Number of retry attempts
    retry_backoff_base: float = Field(default=2.0)  # Exponential backoff multiplier

    status: FunctionStatus = FunctionStatus.PENDING
    error_message: str | None = None

    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class InvocationRecord(SQLModel, table=True):
    """A single function invocation."""
    __tablename__ = "invocations"

    id: int | None = Field(default=None, primary_key=True)
    function_id: int = Field(foreign_key="functions.id", index=True)
    user_id: str = Field(default="default", index=True)  # Owner for quota tracking

    # Serialized args/kwargs (cloudpickle)
    pickled_args: bytes

    # Result (once completed)
    pickled_result: bytes | None = None
    error_message: str | None = None

    # Streaming support
    result_type: ResultType = Field(default=ResultType.SINGLE)
    is_generator: bool = Field(default=False)  # Was this a generator function?
    stream_chunks_count: int = Field(default=0)  # Number of chunks received

    status: InvocationStatus = InvocationStatus.QUEUED

    # Timing
    created_at: datetime = Field(default_factory=utc_now)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    timeout_at: datetime | None = None  # When this invocation should timeout

    # Retry tracking
    retry_count: int = Field(default=0)  # Current retry attempt number
    max_retries: int = Field(default=3)  # Max retries allowed
    next_retry_at: datetime | None = None  # When to retry next

    # Dead letter queue
    dead_letter_reason: str | None = None  # Why it ended up in DLQ

    # Which worker handled it
    worker_id: str | None = None


class StreamChunkRecord(SQLModel, table=True):
    """Individual chunk from a streaming function result."""
    __tablename__ = "stream_chunks"

    id: int | None = Field(default=None, primary_key=True)
    invocation_id: int = Field(foreign_key="invocations.id", index=True)
    sequence: int  # Order of this chunk (0-indexed)
    pickled_value: bytes  # Serialized yielded value
    created_at: datetime = Field(default_factory=utc_now)


class VolumeRecord(SQLModel, table=True):
    """Persistent volume metadata."""
    __tablename__ = "volumes"

    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(unique=True, index=True)
    created_at: datetime = Field(default_factory=utc_now)
    size_bytes: int = Field(default=0)
    storage_path: str  # Local filesystem path for storage


class SecretRecord(SQLModel, table=True):
    """Encrypted secret storage."""
    __tablename__ = "secrets"

    id: int | None = Field(default=None, primary_key=True)
    name: str = Field(unique=True, index=True)
    encrypted_value: str  # Base64-encoded encrypted value
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


class WebEndpointRecord(SQLModel, table=True):
    """HTTP endpoint routing."""
    __tablename__ = "web_endpoints"

    id: int | None = Field(default=None, primary_key=True)
    path: str = Field(index=True)
    method: str = Field(default="GET")
    function_id: int = Field(foreign_key="functions.id")
    created_at: datetime = Field(default_factory=utc_now)


class ScheduleRecord(SQLModel, table=True):
    """Cron/periodic schedule for functions."""
    __tablename__ = "schedules"

    id: int | None = Field(default=None, primary_key=True)
    function_id: int = Field(foreign_key="functions.id")
    cron_expression: str | None = None
    period_seconds: int | None = None
    last_run: datetime | None = None
    next_run: datetime
    enabled: bool = Field(default=True)
    created_at: datetime = Field(default_factory=utc_now)


class WorkerRecord(SQLModel, table=True):
    """Registered worker with resource capabilities."""
    __tablename__ = "workers"

    id: int | None = Field(default=None, primary_key=True)
    worker_id: str = Field(unique=True, index=True)

    # Resource capabilities
    cpu_cores: int = Field(default=1)
    memory_mb: int = Field(default=1024)

    # Status tracking
    status: WorkerStatus = Field(default=WorkerStatus.IDLE)
    current_invocation_id: int | None = None

    # Heartbeat
    last_heartbeat: datetime = Field(default_factory=utc_now)
    registered_at: datetime = Field(default_factory=utc_now)

    # Stats
    tasks_completed: int = Field(default=0)
    tasks_failed: int = Field(default=0)

    # Circuit breaker
    failure_count: int = Field(default=0)  # Failures in current window
    failure_window_start: datetime | None = None  # When window started
    is_healthy: bool = Field(default=True)  # Circuit breaker state
    circuit_open_until: datetime | None = None  # Cooldown end time


class UserRecord(SQLModel, table=True):
    """User with task quotas for scheduler fairness."""
    __tablename__ = "users"

    id: str = Field(primary_key=True)  # e.g., "user-123" or "default"

    # Quota management (for scheduler)
    quota_limit: int = Field(default=5)  # Max concurrent tasks
    active_count: int = Field(default=0)  # Currently running tasks

    # Timestamps
    created_at: datetime = Field(default_factory=utc_now)
    updated_at: datetime = Field(default_factory=utc_now)


# === API Schemas (not stored in DB) ===

class DeployRequest(BaseModel):
    """Request to deploy a function."""
    app_name: str
    function_name: str
    pickled_code: bytes  # base64 encoded in JSON
    python_version: str = "3.11"
    requirements: list[str] = []
    image_config: dict | None = None  # Serialized Image configuration
    cpu: int = 1  # Required CPU cores
    memory: int = 512  # Required memory in MB
    volume_mounts: dict[str, str] | None = None  # {"/mount/path": "volume-name"}
    secrets: list[str] | None = None  # ["SECRET_NAME_1", "SECRET_NAME_2"]
    timeout: int = 300  # Task timeout in seconds
    max_retries: int = 3  # Number of retry attempts
    retry_backoff_base: float = 2.0  # Exponential backoff multiplier


class DeployResponse(BaseModel):
    """Response after deploying."""
    function_id: int
    status: FunctionStatus


class InvokeRequest(BaseModel):
    """Request to invoke a function."""
    function_id: int
    pickled_args: bytes  # base64 encoded


class InvokeResponse(BaseModel):
    """Response with invocation ID."""
    invocation_id: int
    status: InvocationStatus


class BatchInvokeRequest(BaseModel):
    """Request to invoke a function multiple times with different args."""
    function_id: int
    pickled_args_list: list[bytes]  # List of base64-encoded pickled (args, kwargs)


class BatchInvokeResponse(BaseModel):
    """Response with all invocation IDs."""
    invocation_ids: list[int]
    status: InvocationStatus


class ResultResponse(BaseModel):
    """Response when fetching result."""
    invocation_id: int
    status: InvocationStatus
    pickled_result: bytes | None = None
    error_message: str | None = None
    # Streaming info
    result_type: ResultType = ResultType.SINGLE
    is_generator: bool = False


class StreamChunkResponse(BaseModel):
    """A single chunk from a streaming result."""
    sequence: int
    pickled_value: bytes  # base64 encoded


class StreamResultsResponse(BaseModel):
    """Response with streaming results."""
    invocation_id: int
    status: InvocationStatus
    result_type: ResultType
    chunks: list[StreamChunkResponse]
    total_chunks: int  # Total chunks received so far
    error_message: str | None = None


# === Volume Schemas ===

class VolumeCreateRequest(BaseModel):
    """Request to create a volume."""
    name: str


class VolumeResponse(BaseModel):
    """Volume information."""
    id: int
    name: str
    size_bytes: int
    created_at: datetime


class VolumeListResponse(BaseModel):
    """List of volumes."""
    volumes: list[VolumeResponse]


class VolumeFileInfo(BaseModel):
    """Information about a file in a volume."""
    name: str
    is_dir: bool
    size_bytes: int


class VolumeFileListResponse(BaseModel):
    """List of files in a volume."""
    path: str
    files: list[VolumeFileInfo]


# === Secret Schemas ===

class SecretCreateRequest(BaseModel):
    """Request to create/update a secret."""
    name: str
    value: str


class SecretResponse(BaseModel):
    """Secret information (without value)."""
    id: int
    name: str
    created_at: datetime
    updated_at: datetime


class SecretListResponse(BaseModel):
    """List of secrets."""
    secrets: list[SecretResponse]


# === Schedule Schemas ===

class ScheduleResponse(BaseModel):
    """Schedule information."""
    id: int
    function_id: int
    cron_expression: str | None = None
    period_seconds: int | None = None
    last_run: datetime | None = None
    next_run: datetime
    enabled: bool


class ScheduleListResponse(BaseModel):
    """List of schedules."""
    schedules: list[ScheduleResponse]


# === Worker Schemas ===

class WorkerRegisterRequest(BaseModel):
    """Request to register a worker."""
    worker_id: str
    cpu_cores: int = 1
    memory_mb: int = 1024


class WorkerResponse(BaseModel):
    """Worker information."""
    id: int
    worker_id: str
    cpu_cores: int
    memory_mb: int
    status: str
    current_invocation_id: int | None
    tasks_completed: int
    tasks_failed: int
    last_heartbeat: datetime
    registered_at: datetime


class WorkerListResponse(BaseModel):
    """List of workers."""
    workers: list[WorkerResponse]
    summary: dict


# === User Schemas ===

class UserResponse(BaseModel):
    """User information."""
    id: str
    quota_limit: int
    active_count: int
    created_at: datetime
    updated_at: datetime


class UserListResponse(BaseModel):
    """List of users."""
    users: list[UserResponse]
