"""Tests for streaming/generator functions."""

import pytest


class TestStreamingFunctions:
    """Test generator functions with streaming results."""

    def test_generator_returns_all_values(
        self, server_process, worker_process, client, server_url
    ):
        """Test that a generator function yields all values."""
        from minimodal import App

        app = App("test-generator")

        @app.function()
        def generate_numbers(n):
            for i in range(n):
                yield i

        app.deploy()

        # Use stream() to get results
        future = generate_numbers.spawn(5)
        results = list(future.stream())

        assert results == [0, 1, 2, 3, 4]

    def test_generator_with_complex_values(
        self, server_process, worker_process, client, server_url
    ):
        """Test generator with complex return values."""
        from minimodal import App

        app = App("test-generator-complex")

        @app.function()
        def generate_dicts():
            for i in range(3):
                yield {"index": i, "value": i * 10}

        app.deploy()

        future = generate_dicts.spawn()
        results = list(future.stream())

        assert results == [
            {"index": 0, "value": 0},
            {"index": 1, "value": 10},
            {"index": 2, "value": 20},
        ]

    def test_generator_empty(
        self, server_process, worker_process, client, server_url
    ):
        """Test generator that yields nothing."""
        from minimodal import App

        app = App("test-generator-empty")

        @app.function()
        def generate_nothing():
            return
            yield  # Makes it a generator

        app.deploy()

        future = generate_nothing.spawn()
        results = list(future.stream())

        assert results == []

    def test_generator_single_value(
        self, server_process, worker_process, client, server_url
    ):
        """Test generator that yields a single value."""
        from minimodal import App

        app = App("test-generator-single")

        @app.function()
        def generate_one():
            yield "only value"

        app.deploy()

        future = generate_one.spawn()
        results = list(future.stream())

        assert results == ["only value"]

    def test_is_generator_property(
        self, server_process, worker_process, client, server_url
    ):
        """Test that is_generator property correctly identifies generators."""
        from minimodal import App
        import time

        app = App("test-is-generator")

        @app.function()
        def gen_func():
            yield 1

        @app.function()
        def regular_func():
            return 1

        app.deploy()

        # Submit both
        gen_future = gen_func.spawn()
        reg_future = regular_func.spawn()

        # Wait a moment for execution
        time.sleep(1)

        # Check is_generator property
        assert gen_future.is_generator is True
        assert reg_future.is_generator is False


class TestWebSocketStreaming:
    """Test WebSocket-based streaming specifically."""

    def test_websocket_streaming(
        self, server_process, worker_process, client, server_url
    ):
        """Test that WebSocket streaming works correctly."""
        from minimodal import App

        app = App("test-websocket-streaming")

        @app.function()
        def generate_numbers(n):
            for i in range(n):
                yield i * 2

        app.deploy()

        # spawn and stream with explicit WebSocket
        future = generate_numbers.spawn(5)
        results = list(future.stream(use_websocket=True))

        assert results == [0, 2, 4, 6, 8]

    def test_http_polling_fallback(
        self, server_process, worker_process, client, server_url
    ):
        """Test that HTTP polling works when explicitly requested."""
        from minimodal import App

        app = App("test-http-polling")

        @app.function()
        def generate_letters():
            for c in "abc":
                yield c

        app.deploy()

        # Use HTTP polling explicitly
        future = generate_letters.spawn()
        results = list(future.stream(use_websocket=False))

        assert results == ["a", "b", "c"]


class TestStreamingWithRegularFunctions:
    """Test that regular functions still work with streaming API."""

    def test_regular_function_remote(
        self, server_process, worker_process, client, server_url
    ):
        """Test that regular functions still work with remote()."""
        from minimodal import App

        app = App("test-regular-with-streaming")

        @app.function()
        def add(a, b):
            return a + b

        app.deploy()

        result = add.remote(3, 4)
        assert result == 7

    def test_regular_function_spawn(
        self, server_process, worker_process, client, server_url
    ):
        """Test that regular functions still work with spawn()."""
        from minimodal import App

        app = App("test-regular-spawn")

        @app.function()
        def multiply(a, b):
            return a * b

        app.deploy()

        future = multiply.spawn(3, 4)
        result = future.result()
        assert result == 12
