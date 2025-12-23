"""Tests for function execution: remote(), spawn(), map()."""

import pytest


class TestFunctionRemote:
    """Test blocking remote() execution."""

    def test_remote_returns_correct_result(
        self, server_process, worker_process, client
    ):
        """Test basic remote call returns correct result."""
        from minimodal import App

        app = App("test-remote")

        @app.function()
        def add(a, b):
            return a + b

        app.deploy()

        result = add.remote(5, 3)
        assert result == 8

    def test_remote_with_kwargs(self, server_process, worker_process, client):
        """Test remote call with keyword arguments."""
        from minimodal import App

        app = App("test-kwargs")

        @app.function()
        def greet(name, greeting="Hello"):
            return f"{greeting}, {name}!"

        app.deploy()

        result = greet.remote("World", greeting="Hi")
        assert result == "Hi, World!"

    def test_remote_with_complex_return(self, server_process, worker_process, client):
        """Test remote call with complex return types."""
        from minimodal import App

        app = App("test-complex-return")

        @app.function()
        def complex_data():
            return {
                "list": [1, 2, 3],
                "nested": {"a": {"b": "c"}},
                "tuple": (1, 2),
            }

        app.deploy()

        result = complex_data.remote()
        assert result["list"] == [1, 2, 3]
        assert result["nested"]["a"]["b"] == "c"

    def test_remote_with_no_args(self, server_process, worker_process, client):
        """Test remote call with no arguments."""
        from minimodal import App

        app = App("test-no-args")

        @app.function()
        def no_args():
            return "no args"

        app.deploy()

        result = no_args.remote()
        assert result == "no args"

    def test_remote_with_only_kwargs(self, server_process, worker_process, client):
        """Test remote call with only keyword arguments."""
        from minimodal import App

        app = App("test-only-kwargs")

        @app.function()
        def only_kwargs(x=1, y=2):
            return x + y

        app.deploy()

        result = only_kwargs.remote(x=10, y=20)
        assert result == 30


class TestFunctionSpawn:
    """Test async spawn() execution."""

    def test_spawn_returns_future(self, server_process, worker_process, client):
        """Test spawn returns a FunctionCall (future)."""
        from minimodal import App

        app = App("test-spawn")

        @app.function()
        def slow_task():
            import time

            time.sleep(0.5)
            return "done"

        app.deploy()

        future = slow_task.spawn()
        # Future should exist immediately
        assert future is not None
        assert hasattr(future, "result")

        result = future.result()
        assert result == "done"

    def test_spawn_multiple_concurrent(self, server_process, worker_process, client):
        """Test multiple spawn calls run concurrently."""
        from minimodal import App, gather

        app = App("test-spawn-concurrent")

        @app.function()
        def square(x):
            return x * x

        app.deploy()

        futures = [square.spawn(i) for i in range(5)]
        results = gather(*futures)

        assert results == [0, 1, 4, 9, 16]

    def test_spawn_result_caches(self, server_process, worker_process, client):
        """Test that result() caches the result."""
        from minimodal import App

        app = App("test-spawn-cache")

        @app.function()
        def compute():
            return 42

        app.deploy()

        future = compute.spawn()
        result1 = future.result()
        result2 = future.result()

        assert result1 == result2 == 42


class TestFunctionMap:
    """Test parallel map() execution."""

    def test_map_over_list(self, server_process, worker_process, client):
        """Test map over a list of inputs."""
        from minimodal import App

        app = App("test-map")

        @app.function()
        def double(x):
            return x * 2

        app.deploy()

        futures = double.map([1, 2, 3, 4, 5])
        results = [f.result() for f in futures]

        assert results == [2, 4, 6, 8, 10]

    def test_starmap_with_tuples(self, server_process, worker_process, client):
        """Test starmap with tuple arguments."""
        from minimodal import App

        app = App("test-starmap")

        @app.function()
        def add(a, b):
            return a + b

        app.deploy()

        futures = add.starmap([(1, 2), (3, 4), (5, 6)])
        results = [f.result() for f in futures]

        assert results == [3, 7, 11]

    def test_map_empty_list(self, server_process, worker_process, client):
        """Test map with empty list."""
        from minimodal import App

        app = App("test-map-empty")

        @app.function()
        def identity(x):
            return x

        app.deploy()

        futures = identity.map([])
        results = [f.result() for f in futures]

        assert results == []

    def test_map_preserves_order(self, server_process, worker_process, client):
        """Test map preserves order of results."""
        from minimodal import App

        app = App("test-map-order")

        @app.function()
        def identity(x):
            return x

        app.deploy()

        inputs = list(range(10))
        futures = identity.map(inputs)
        results = [f.result() for f in futures]

        assert results == inputs

    def test_map_with_kwargs(self, server_process, worker_process, client):
        """Test map with shared kwargs."""
        from minimodal import App

        app = App("test-map-kwargs")

        @app.function()
        def multiply(x, factor=1):
            return x * factor

        app.deploy()

        futures = multiply.map([1, 2, 3], kwargs={"factor": 10})
        results = [f.result() for f in futures]

        assert results == [10, 20, 30]

    def test_map_large_batch(self, server_process, worker_process, client):
        """Test map with a larger batch of items."""
        from minimodal import App

        app = App("test-map-large")

        @app.function()
        def square(x):
            return x * x

        app.deploy()

        # Test with 20 items to verify batch handling
        inputs = list(range(20))
        futures = square.map(inputs)
        results = [f.result() for f in futures]

        assert results == [x * x for x in inputs]


class TestFunctionErrors:
    """Test error handling in remote functions."""

    def test_remote_exception_is_raised(self, server_process, worker_process, client):
        """Test that remote exceptions are properly raised."""
        from minimodal import App
        from minimodal.sdk.client import RemoteError

        app = App("test-error")

        @app.function()
        def failing_function():
            raise ValueError("intentional error")

        app.deploy()

        with pytest.raises(RemoteError) as exc_info:
            failing_function.remote()

        assert "ValueError" in str(exc_info.value.remote_traceback)

    def test_remote_exception_preserves_type(
        self, server_process, worker_process, client
    ):
        """Test that exception type is preserved in traceback."""
        from minimodal import App
        from minimodal.sdk.client import RemoteError

        app = App("test-error-type")

        @app.function()
        def type_error():
            raise TypeError("wrong type")

        app.deploy()

        with pytest.raises(RemoteError) as exc_info:
            type_error.remote()

        assert "TypeError" in str(exc_info.value.remote_traceback)


class TestFunctionLocal:
    """Test local function execution."""

    def test_local_call(self, server_process, worker_process, client):
        """Test that functions can be called locally."""
        from minimodal import App

        app = App("test-local")

        @app.function()
        def local_func(x):
            return x * 2

        # Note: local() requires deployment first
        app.deploy()

        # Local call directly on the decorated function
        result = local_func(5)
        assert result == 10
