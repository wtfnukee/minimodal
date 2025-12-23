"""WebEndpoint class for HTTP endpoints backed by functions."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from minimodal.sdk.app import App


class WebEndpoint:
    """
    HTTP endpoint backed by a function.

    WebEndpoints allow you to expose functions as HTTP APIs.
    The function receives request data and returns a response.

    Usage:
        app = App("my-app")

        @app.web_endpoint(path="/api/hello", method="GET")
        def hello_handler():
            return {"message": "Hello, World!"}

        @app.web_endpoint(path="/api/users/{user_id}", method="GET")
        def get_user(user_id: str):
            return {"user_id": user_id, "name": "John"}

        @app.web_endpoint(path="/api/data", method="POST")
        def create_data(body: dict):
            # body contains the JSON request body
            return {"created": True, "data": body}

    Request handling:
        - Path parameters are passed as keyword arguments
        - Query parameters are passed as keyword arguments
        - Request body (JSON) is passed as the 'body' argument for POST/PUT
        - Headers are passed as the 'headers' argument if the function accepts it

    Response handling:
        - Return a dict for JSON response
        - Return a string for plain text response
        - Return a tuple (body, status_code) to set status
        - Return a tuple (body, status_code, headers) to set headers
    """

    def __init__(
        self,
        func: Callable,
        path: str,
        method: str = "GET",
        app: "App | None" = None,
    ):
        """
        Initialize a WebEndpoint.

        Args:
            func: The handler function
            path: URL path (can include path parameters like /users/{id})
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            app: Parent App instance
        """
        self._func = func
        self.path = path
        self.method = method.upper()
        self.app = app

        # The underlying Function wrapper (set by App)
        self._function = None

        # Set after registration
        self._endpoint_id: int | None = None

        # Preserve function metadata
        self.__name__ = func.__name__
        self.__doc__ = func.__doc__

    def __call__(self, *args, **kwargs) -> Any:
        return self._func(*args, **kwargs)

    @property
    def url(self) -> str | None:
        if self.app and hasattr(self.app, "_base_url"):
            return f"{self.app._base_url}/web{self.path}"
        return None

    def __repr__(self) -> str:
        return f"WebEndpoint({self.method} {self.path})"


class WebRequest:
    """
    Represents an incoming HTTP request.

    Attributes:
        method: HTTP method (GET, POST, etc.)
        path: Request path
        path_params: Parameters extracted from path
        query_params: URL query parameters
        headers: HTTP headers
        body: Request body (for POST/PUT/PATCH)
    """

    def __init__(
        self,
        method: str,
        path: str,
        path_params: dict[str, str] | None = None,
        query_params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
        body: Any = None,
    ):
        self.method = method
        self.path = path
        self.path_params = path_params or {}
        self.query_params = query_params or {}
        self.headers = headers or {}
        self.body = body

    def to_dict(self) -> dict:
        return {
            "method": self.method,
            "path": self.path,
            "path_params": self.path_params,
            "query_params": self.query_params,
            "headers": self.headers,
            "body": self.body,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "WebRequest":
        return cls(
            method=data.get("method", "GET"),
            path=data.get("path", "/"),
            path_params=data.get("path_params"),
            query_params=data.get("query_params"),
            headers=data.get("headers"),
            body=data.get("body"),
        )


class WebResponse:
    """
    Represents an HTTP response.

    Usage:
        # Simple JSON response
        return WebResponse({"status": "ok"})

        # With status code
        return WebResponse({"error": "Not found"}, status_code=404)

        # With headers
        return WebResponse(
            "Hello",
            content_type="text/plain",
            headers={"X-Custom": "value"},
        )
    """

    def __init__(
        self,
        body: Any = None,
        status_code: int = 200,
        content_type: str = "application/json",
        headers: dict[str, str] | None = None,
    ):
        self.body = body
        self.status_code = status_code
        self.content_type = content_type
        self.headers = headers or {}

    def to_dict(self) -> dict:
        return {
            "body": self.body,
            "status_code": self.status_code,
            "content_type": self.content_type,
            "headers": self.headers,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "WebResponse":
        return cls(
            body=data.get("body"),
            status_code=data.get("status_code", 200),
            content_type=data.get("content_type", "application/json"),
            headers=data.get("headers"),
        )
