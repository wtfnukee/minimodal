"""App class — the main entry point for users."""

from typing import TYPE_CHECKING, Callable

from minimodal.sdk.function import Function
from minimodal.sdk.web import WebEndpoint

if TYPE_CHECKING:
    from minimodal.sdk.image import Image


class Cron:
    """
    Cron expression for scheduling.

    Uses standard cron syntax:
        - minute (0-59)
        - hour (0-23)
        - day of month (1-31)
        - month (1-12)
        - day of week (0-6, 0=Sunday)

    Examples:
        Cron("0 * * * *")       # Every hour
        Cron("0 0 * * *")       # Every day at midnight
        Cron("0 9 * * 1")       # Every Monday at 9am
        Cron("*/15 * * * *")    # Every 15 minutes
    """

    def __init__(self, expression: str):
        self.expression = expression

    def __repr__(self) -> str:
        return f"Cron({self.expression!r})"


class Period:
    """
    Simple periodic scheduling.

    Use this when you want a function to run at regular intervals
    without the complexity of cron syntax.

    Examples:
        Period(minutes=5)         # Every 5 minutes
        Period(hours=1)           # Every hour
        Period(seconds=30)        # Every 30 seconds
        Period(hours=2, minutes=30)  # Every 2.5 hours
    """

    def __init__(
        self,
        seconds: int = 0,
        minutes: int = 0,
        hours: int = 0,
    ):
        self.total_seconds = seconds + minutes * 60 + hours * 3600
        if self.total_seconds <= 0:
            raise ValueError("Period must be positive")

    def __repr__(self) -> str:
        if self.total_seconds >= 3600:
            return f"Period(hours={self.total_seconds // 3600})"
        elif self.total_seconds >= 60:
            return f"Period(minutes={self.total_seconds // 60})"
        else:
            return f"Period(seconds={self.total_seconds})"


class App:
    """
    A collection of serverless functions.

    Usage:
        app = App("my-app")

        @app.function()
        def hello(name: str) -> str:
            return f"Hello, {name}!"

        # Deploy all functions
        app.deploy()

        # Call remotely
        result = hello.remote("World")
    """

    def __init__(self, name: str):
        self.name = name
        self._functions: dict[str, Function] = {}
        self._web_endpoints: dict[str, WebEndpoint] = {}

    def function(
        self,
        python_version: str = "3.11",
        requirements: list[str] | None = None,
        schedule: Cron | Period | None = None,
        cpu: int = 1,
        memory: int = 512,
        image: "Image | None" = None,
        volumes: dict[str, str] | None = None,
        secrets: list[str] | None = None,
        timeout: int = 300,
        retries: int = 3,
    ) -> Callable[[Callable], Function]:
        """
        Decorator to register a function with this app.

        Args:
            python_version: Python version for the container
            requirements: List of pip packages to install
            schedule: Optional Cron or Period for scheduled execution
            cpu: Number of CPU cores required (default: 1)
            memory: Memory in MB required (default: 512)
            image: Optional custom Image configuration
            volumes: Dict mapping mount paths to volume names
            secrets: List of secret names to inject as env vars
            timeout: Task timeout in seconds (default: 300)
            retries: Number of retry attempts on failure (default: 3)

        Returns:
            A Function wrapper that supports .remote() calls
        """

        def decorator(func: Callable) -> Function:
            wrapped = Function(
                func=func,
                app_name=self.name,
                python_version=python_version,
                requirements=requirements,
                cpu=cpu,
                memory=memory,
                image=image,
                volumes=volumes,
                secrets=secrets,
                timeout=timeout,
                retries=retries,
            )
            # Store schedule on the function wrapper
            wrapped._schedule = schedule
            self._functions[func.__name__] = wrapped
            return wrapped

        return decorator

    def deploy(self) -> dict[str, int]:
        """
        Deploy all functions in this app.

        Returns:
            Dict mapping function name to function ID
        """
        from minimodal.sdk.client import get_client

        results = {}
        client = get_client()

        for name, func in self._functions.items():
            func_id = func.deploy()
            results[name] = func_id
            print(f"Deployed {name} → function_id={func_id}")

            # Register schedule if present
            schedule = getattr(func, "_schedule", None)
            if schedule:
                if isinstance(schedule, Cron):
                    client._request(
                        "POST",
                        "/schedules",
                        params={
                            "function_id": func_id,
                            "cron_expression": schedule.expression,
                        },
                    )
                    print(f"  Registered schedule: {schedule}")
                elif isinstance(schedule, Period):
                    client._request(
                        "POST",
                        "/schedules",
                        params={
                            "function_id": func_id,
                            "period_seconds": schedule.total_seconds,
                        },
                    )
                    print(f"  Registered schedule: {schedule}")

        # Register web endpoints
        for key, endpoint in self._web_endpoints.items():
            func_name = endpoint._func.__name__
            if func_name in results:
                func_id = results[func_name]
                try:
                    response = client._request(
                        "POST",
                        "/web-endpoints",
                        params={
                            "path": endpoint.path,
                            "method": endpoint.method,
                            "function_id": func_id,
                        },
                    )
                    response.raise_for_status()
                    response.json()
                    print(
                        f"  Registered web endpoint: {endpoint.method} {endpoint.path}"
                    )
                except Exception as e:
                    print(
                        f"  Warning: Failed to register web endpoint {endpoint.method} {endpoint.path}: {e}"
                    )

        return results

    def web_endpoint(
        self,
        path: str,
        method: str = "GET",
        python_version: str = "3.11",
        requirements: list[str] | None = None,
    ) -> Callable[[Callable], WebEndpoint]:
        """
        Decorator to register a web endpoint.

        The decorated function will be called when HTTP requests match
        the specified path and method.

        Args:
            path: URL path (can include path parameters like /users/{id})
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            python_version: Python version for the container
            requirements: List of pip packages to install

        Returns:
            A WebEndpoint wrapper

        Example:
            @app.web_endpoint(path="/api/users/{user_id}", method="GET")
            def get_user(user_id: str):
                return {"user_id": user_id}
        """

        def decorator(func: Callable) -> WebEndpoint:
            endpoint = WebEndpoint(
                func=func,
                path=path,
                method=method,
                app=self,
            )

            # Also create a Function wrapper for the underlying function
            wrapped_func = Function(
                func=func,
                app_name=self.name,
                python_version=python_version,
                requirements=requirements,
            )
            endpoint._function = wrapped_func
            self._functions[func.__name__] = wrapped_func
            self._web_endpoints[f"{method}:{path}"] = endpoint

            return endpoint

        return decorator

    @property
    def functions(self) -> dict[str, Function]:
        return self._functions.copy()

    @property
    def web_endpoints(self) -> dict[str, WebEndpoint]:
        return self._web_endpoints.copy()
