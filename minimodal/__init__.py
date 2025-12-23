"""MiniModal — a minimal serverless platform for learning."""

__version__ = "0.1.0"

from minimodal.sdk import (
    App,
    Function,
    Image,
    Volume,
    Secret,
    secret_from_env,
    WebEndpoint,
    WebRequest,
    WebResponse,
    Cron,
    Period,
    MiniModalClient,
    get_client,
    reset_client,
    FunctionCall,
    gather,
    wait_first,
    MiniModalError,
    ConnectionError,
    DeploymentError,
    InvocationError,
    TimeoutError,
    RemoteError,
)

__all__ = [
    # Core
    "App",
    "Function",
    "Image",
    # Volumes
    "Volume",
    # Secrets
    "Secret",
    "secret_from_env",
    # Web Endpoints
    "WebEndpoint",
    "WebRequest",
    "WebResponse",
    # Scheduling
    "Cron",
    "Period",
    # Client
    "MiniModalClient",
    "get_client",
    "reset_client",
    # Async/Futures
    "FunctionCall",
    "gather",
    "wait_first",
    # Exceptions
    "MiniModalError",
    "ConnectionError",
    "DeploymentError",
    "InvocationError",
    "TimeoutError",
    "RemoteError",
]
