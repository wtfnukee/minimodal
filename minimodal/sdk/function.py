"""Function wrapper that enables local and remote execution."""

from typing import TYPE_CHECKING, Any, Callable, Iterable, Optional

from minimodal.sdk.client import DeploymentError, get_client
from minimodal.sdk.future import FunctionCall

if TYPE_CHECKING:
    from minimodal.sdk.image import Image


class Function:
    """
    Wrapper around a user function that enables remote execution.

    Usage:
        @app.function()
        def my_func(x):
            return x * 2

        # Local execution
        result = my_func(5)  # returns 10

        # Remote execution (blocking)
        result = my_func.remote(5)  # runs on worker, returns 10

        # Async execution
        future = my_func.spawn(5)  # returns immediately
        result = future.result()    # blocks when you need the result

        # Parallel map
        futures = my_func.map([1, 2, 3, 4, 5])
        results = [f.result() for f in futures]
    """

    def __init__(
        self,
        func: Callable,
        app_name: str,
        python_version: str = "3.11",
        requirements: list[str] | None = None,
        cpu: int = 1,
        memory: int = 512,
        image: "Image | None" = None,
        volumes: dict[str, str] | None = None,
        secrets: list[str] | None = None,
        timeout: int = 300,
        retries: int = 3,
    ):
        self._func = func
        self._app_name = app_name
        self._python_version = python_version
        self._requirements = requirements or []
        self._cpu = cpu
        self._memory = memory
        self._image = image
        self._volumes = volumes or {}  # {"/mount/path": "volume-name"}
        self._secrets = secrets or []  # ["SECRET_NAME_1", ...]
        self._timeout = timeout
        self._retries = retries

        # Set after deployment
        self._function_id: Optional[int] = None
        self._deployed = False

        # Preserve function metadata
        self.__name__ = func.__name__
        self.__doc__ = func.__doc__

    def __call__(self, *args, **kwargs) -> Any:
        """Local execution."""
        return self._func(*args, **kwargs)

    def _check_deployed(self):
        if not self._deployed:
            raise DeploymentError(
                f"Function {self.__name__} not deployed. Call app.deploy() first."
            )

    def spawn(self, *args, **kwargs) -> FunctionCall:
        """
        Submit a remote invocation and return FunctionCall (future) immediately.

        Args:
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            FunctionCall: Handle to the pending invocation

        Raises:
            DeploymentError: If function is not deployed
            InvocationError: If invocation fails to submit

        Example:
            future = my_func.spawn(10)
            # Do other work...
            result = future.result()  # Block when you need the result
        """
        self._check_deployed()

        client = get_client()
        invocation_id = client.invoke(self._function_id, args, kwargs)
        return FunctionCall(invocation_id, client)

    def remote(self, *args, **kwargs) -> Any:
        """
        Blocking remote execution.

        Args:
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            The result of the remote function execution

        Raises:
            DeploymentError: If function is not deployed
            InvocationError: If invocation fails
            RemoteError: If remote function raises an exception
            TimeoutError: If result retrieval times out
        """
        return self.spawn(*args, **kwargs).result()

    def map(
        self,
        inputs: Iterable[Any],
        kwargs: dict | None = None,
    ) -> list[FunctionCall]:
        """
        Submit multiple invocations in parallel using batch invoke.

        Each element in inputs becomes the first positional argument
        to a separate invocation. All invocations share the same kwargs.

        Args:
            inputs: Iterable of inputs, each becomes first arg to a call
            kwargs: Optional keyword arguments shared by all calls

        Returns:
            List of FunctionCall handles for each invocation

        Raises:
            DeploymentError: If function is not deployed
            InvocationError: If any invocation fails to submit

        Example:
            # Process items in parallel
            futures = process.map([item1, item2, item3])
            results = [f.result() for f in futures]

            # Or use gather for convenience
            from minimodal import gather
            results = gather(*process.map([item1, item2, item3]))
        """
        self._check_deployed()

        kwargs = kwargs or {}
        inputs_list = list(inputs)

        if not inputs_list:
            return []

        # Build args list for batch invoke
        args_list = [((item,), kwargs) for item in inputs_list]

        # Submit all invocations at once
        client = get_client()
        invocation_ids = client.batch_invoke(self._function_id, args_list)

        # Create FunctionCall handles for each invocation
        return [FunctionCall(inv_id, client) for inv_id in invocation_ids]

    def starmap(
        self,
        inputs: Iterable[tuple],
    ) -> list[FunctionCall]:
        """
        Submit multiple invocations with tuple unpacking using batch invoke.

        Each tuple in inputs is unpacked as positional arguments
        to a separate invocation.

        Args:
            inputs: Iterable of tuples, each is unpacked as *args

        Returns:
            List of FunctionCall handles for each invocation

        Raises:
            DeploymentError: If function is not deployed
            InvocationError: If any invocation fails to submit

        Example:
            # Call add(a, b) with multiple pairs
            futures = add.starmap([(1, 2), (3, 4), (5, 6)])
            results = [f.result() for f in futures]  # [3, 7, 11]
        """
        self._check_deployed()

        inputs_list = list(inputs)

        if not inputs_list:
            return []

        # Build args list for batch invoke - each tuple becomes args, empty kwargs
        args_list = [(tuple(args), {}) for args in inputs_list]

        # Submit all invocations at once
        client = get_client()
        invocation_ids = client.batch_invoke(self._function_id, args_list)

        # Create FunctionCall handles for each invocation
        return [FunctionCall(inv_id, client) for inv_id in invocation_ids]

    def deploy(self) -> int:
        """Deploy this function to the control plane."""
        client = get_client()

        # Serialize image config if present
        image_config = None
        if self._image:
            image_config = self._image.to_dict()

        response = client.deploy(
            app_name=self._app_name,
            function_name=self.__name__,
            func=self._func,
            python_version=self._python_version,
            requirements=self._requirements,
            cpu=self._cpu,
            memory=self._memory,
            image_config=image_config,
            volume_mounts=self._volumes if self._volumes else None,
            secrets=self._secrets if self._secrets else None,
            timeout=self._timeout,
            max_retries=self._retries,
        )
        self._function_id = response.function_id
        self._deployed = True
        return self._function_id

    @property
    def function_id(self) -> Optional[int]:
        return self._function_id

    @property
    def is_deployed(self) -> bool:
        return self._deployed
