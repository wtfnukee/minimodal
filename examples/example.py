"""
Basic Example: Core MiniModal Features

This example demonstrates the essential features of MiniModal:
- Custom Docker images with pip_install and apt_install (API shown)
- Multiple function types (regular, streaming/generator)
- Parallel execution with map() and spawn()
- Result collection with gather()
- Resource allocation (CPU, memory)

To run this example:

1. Start the server:
   uv run python -m minimodal.server.api

2. Start a worker (in another terminal):
   uv run python -m minimodal.worker.executor

3. Run this example:
   uv run python examples/above_basic_example.py
"""

# === Define a custom Docker image ===
#
# IMPORTANT: The container Python version must match the host!
# Cloudpickle serializes bytecode which is version-specific.
#
import sys

from minimodal import App, Image, gather

_python_version = f"{sys.version_info.major}.{sys.version_info.minor}"

data_science_image = (
    Image.debian_slim(python_version=_python_version)
    .apt_install("jq", "curl")  # System packages
    .pip_install("numpy")  # Python packages
    .env(PYTHONUNBUFFERED="1")  # Environment variables
)

# Create the app
app = App("above-basic-demo")


# === Regular Functions ===


@app.function()
def add(a: int, b: int) -> int:
    """Simple function - adds two numbers."""
    return a + b


@app.function()
def factorial(n: int) -> int:
    """Compute factorial recursively."""
    if n <= 1:
        return 1
    return n * factorial(n - 1)


# === Functions with Custom Image ===


@app.function(image=data_science_image, cpu=1, memory=512)
def compute_statistics(data: list[float]) -> dict:
    """
    Compute statistics using numpy.

    Runs in a Docker container with numpy installed.
    Resource limits (cpu=2, memory=1024MB) are enforced by Docker.
    """
    import numpy as np

    arr = np.array(data)
    return {
        "mean": float(np.mean(arr)),
        "std": float(np.std(arr)),
        "min": float(np.min(arr)),
        "max": float(np.max(arr)),
        "median": float(np.median(arr)),
        "sum": float(np.sum(arr)),
    }


@app.function(image=data_science_image)
def matrix_multiply(size: int) -> dict:
    """
    Create and multiply random matrices using numpy.

    Demonstrates CPU-intensive work with numpy in Docker containers.
    """
    import numpy as np

    a = np.random.rand(size, size)
    b = np.random.rand(size, size)
    result = np.dot(a, b)

    return {
        "size": size,
        "result_shape": list(result.shape),
        "result_sum": float(np.sum(result)),
    }


# === Streaming/Generator Function ===


@app.function()
def generate_fibonacci(n: int):
    """
    Generate Fibonacci sequence as a stream.

    This is a generator function - results are streamed
    as they're computed rather than returned all at once.
    """
    a, b = 0, 1
    for i in range(n):
        yield {"index": i, "value": a}
        a, b = b, a + b


@app.function()
def countdown(start: int):
    """
    Countdown from a number, streaming each value.
    """
    import time

    for i in range(start, 0, -1):
        yield i
        time.sleep(0.1)  # Simulate work
    yield "Liftoff!"


@app.function(image=data_science_image)
def stream_random_numbers(count: int, delay: float = 0.1):
    """
    Stream random numbers from a normal distribution using numpy.

    Demonstrates streaming from a function in a Docker container.
    """
    import time

    import numpy as np

    for i in range(count):
        value = float(np.random.randn())
        yield {"index": i, "value": round(value, 4)}
        time.sleep(delay)


# === Function for batch processing ===


@app.function()
def process_item(item: str) -> dict:
    """
    Process a single item.

    This function is designed to be used with map()
    for batch processing multiple items in parallel.
    """
    import hashlib

    return {
        "original": item,
        "upper": item.upper(),
        "length": len(item),
        "hash": hashlib.md5(item.encode()).hexdigest()[:8],
    }


def main():
    """Demonstrate all features."""
    print("=" * 60)
    print("MiniModal: Above Basic Example")
    print("=" * 60)

    # Deploy all functions
    print("\n[1] Deploying functions...")
    app.deploy()
    print("    Done!")

    # === Regular remote execution ===
    print("\n[2] Regular remote execution...")

    result = add.remote(10, 20)
    print(f"    add(10, 20) = {result}")

    result = factorial.remote(6)
    print(f"    factorial(6) = {result}")

    # === Custom image function ===
    print("\n[3] Function with custom Docker image (numpy)...")

    data = [1.5, 2.3, 4.7, 3.2, 5.8, 2.1, 4.4]
    stats = compute_statistics.remote(data)
    print(f"    Data: {data}")
    print(f"    Statistics (computed with numpy in Docker):")
    for key, value in stats.items():
        print(f"      {key}: {value:.2f}")

    # === Parallel execution with spawn() ===
    print("\n[4] Parallel execution with spawn()...")

    # Spawn multiple matrix multiplications in parallel
    sizes = [50, 100, 150]
    futures = [matrix_multiply.spawn(size) for size in sizes]
    print(f"    Spawned {len(futures)} jobs for sizes: {sizes}")

    # Collect results with gather()
    results = gather(*futures)
    for r in results:
        print(f"    {r['size']}x{r['size']} matrix: sum = {r['result_sum']:.2f}")

    # === Batch processing with map() ===
    print("\n[5] Batch processing with map()...")

    items = ["apple", "banana", "cherry", "date", "elderberry"]
    print(f"    Processing {len(items)} items in parallel...")

    futures = process_item.map(items)
    results = [f.result() for f in futures]

    for r in results:
        print(f"    {r['original']:12} → {r['upper']:12} (hash: {r['hash']})")

    # === Streaming with generators ===
    print("\n[6] Streaming with generators...")

    print("\n    a) Fibonacci sequence (streaming):")
    fib_future = generate_fibonacci.spawn(8)
    print("       ", end="")
    for item in fib_future.stream():
        print(f"{item['value']}", end=" ")
    print()

    print("\n    b) Countdown (streaming):")
    countdown_future = countdown.spawn(5)
    print("       ", end="")
    for item in countdown_future.stream():
        print(f"{item}", end=" ")
    print()

    print("\n    c) Random numbers with numpy (streaming):")
    random_future = stream_random_numbers.spawn(5, delay=0.05)
    print("       ", end="")
    for item in random_future.stream():
        print(f"{item['value']:+.3f}", end=" ")
    print()

    # === Combining patterns ===
    print("\n[7] Combining patterns: spawn + stream + gather...")

    # Spawn multiple streaming jobs
    stream_futures = [
        generate_fibonacci.spawn(3),
        generate_fibonacci.spawn(4),
        generate_fibonacci.spawn(5),
    ]

    # Collect all streaming results
    all_results = []
    for i, future in enumerate(stream_futures):
        values = [item["value"] for item in future.stream()]
        all_results.append(values)
        print(f"    Stream {i + 1}: {values}")

    # === Summary ===
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print("""
Features demonstrated:
  - Custom Docker image with pip_install() and apt_install()
  - Regular functions with .remote() calls
  - CPU/memory resource allocation
  - Parallel execution with .spawn()
  - Result collection with gather()
  - Batch processing with .map()
  - Streaming/generator functions with .stream()
  - WebSocket-based real-time streaming

All functions ran remotely on workers!
    """)


if __name__ == "__main__":
    main()
