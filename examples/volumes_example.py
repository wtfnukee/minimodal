"""
Example: Using Volumes for Persistent Storage

Volumes allow you to persist data across function invocations.
Data stored in volumes survives container restarts.

To run this example:

1. Start the server:
   uv run python -m minimodal.server.api

2. Start a worker (in another terminal):
   uv run python -m minimodal.worker.executor

3. Run this example:
   uv run python examples/volumes_example.py
"""

import json
import tempfile
from pathlib import Path

from minimodal import App, Volume

# Create an app
app = App("volumes-example")

# Create a persistent volume for storing results
results_vol = Volume.persisted("computation-results")


@app.function()
def process_and_store(data: list, filename: str) -> dict:
    """
    Process data and store results to the volume.

    In a real scenario, this function would run in a container
    with the volume mounted at /data.
    """
    # Process the data
    result = {
        "sum": sum(data),
        "count": len(data),
        "average": sum(data) / len(data) if data else 0,
        "min": min(data) if data else None,
        "max": max(data) if data else None,
    }

    return result


@app.function()
def aggregate_results() -> dict:
    """
    Read all results from the volume and aggregate them.
    """
    # In real usage, this would read from the mounted volume
    return {"status": "would read from /data volume"}


def demonstrate_volume_api():
    """Demonstrate the Volume SDK API without running remote functions."""
    print("=" * 60)
    print("Volume API Demonstration")
    print("=" * 60)

    # Create or reference a volume
    vol = Volume.persisted("demo-volume")
    print(f"\n1. Created volume reference: {vol}")

    # For demonstration, we'll use the client directly
    # In real usage, volumes are mounted to functions
    from minimodal import get_client

    client = get_client()

    # Create the volume on the server
    print("\n2. Creating volume on server...")
    try:
        vol_id = client.create_volume("demo-volume")
        print(f"   Volume created with ID: {vol_id}")
    except Exception as e:
        print(f"   Note: {e}")

    # Upload a file
    print("\n3. Uploading files to volume...")
    test_data = {"message": "Hello from volume!", "numbers": [1, 2, 3]}
    content = json.dumps(test_data, indent=2).encode("utf-8")

    try:
        client.upload_volume_file("demo-volume", "/data/test.json", content)
        print("   Uploaded: /data/test.json")

        # Upload another file
        client.upload_volume_file(
            "demo-volume",
            "/data/nested/config.txt",
            b"key=value\nmode=production",
        )
        print("   Uploaded: /data/nested/config.txt")
    except Exception as e:
        print(f"   Upload error: {e}")

    # List files
    print("\n4. Listing files in volume...")
    try:
        files = client.list_volume_files("demo-volume", "/")
        print(f"   Root directory: {files}")

        files = client.list_volume_files("demo-volume", "/data")
        print(f"   /data directory: {files}")
    except Exception as e:
        print(f"   List error: {e}")

    # Download a file
    print("\n5. Downloading file from volume...")
    try:
        downloaded = client.download_volume_file("demo-volume", "/data/test.json")
        data = json.loads(downloaded.decode("utf-8"))
        print(f"   Downloaded content: {data}")
    except Exception as e:
        print(f"   Download error: {e}")

    # Clean up
    print("\n6. Cleaning up...")
    try:
        client.delete_volume_file("demo-volume", "/data/test.json")
        print("   Deleted: /data/test.json")
    except Exception as e:
        print(f"   Cleanup note: {e}")

    print("\n" + "=" * 60)


def main():
    """Run the volumes example."""
    print("Volumes Example")
    print("-" * 40)

    # First demonstrate the Volume API
    demonstrate_volume_api()

    print("\n" + "=" * 60)
    print("Function Deployment with Volumes")
    print("=" * 60)

    # Deploy functions
    print("\nDeploying functions...")
    app.deploy()

    # In a full implementation, you would use volumes like this:
    #
    # @app.function(volumes={"/data": results_vol})
    # def my_function():
    #     # Read from /data
    #     with open("/data/input.json") as f:
    #         data = json.load(f)
    #
    #     # Write results to /data
    #     with open("/data/output.json", "w") as f:
    #         json.dump(results, f)

    print("\nNote: Full volume mounting requires Docker container support.")
    print("The Volume API demonstrated above shows how to interact with volumes.")


if __name__ == "__main__":
    main()
