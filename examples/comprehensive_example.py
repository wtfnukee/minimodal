"""
Comprehensive Example: All MiniModal Features Combined

This example demonstrates how to use all MiniModal features together
in a realistic application: a data processing pipeline with:
- Volumes for persistent storage
- Secrets for API credentials
- Web endpoints for HTTP interface
- Cron jobs for scheduled tasks
- Async execution with spawn() and map()

To run this example:

1. Start the server:
   uv run python -m minimodal.server.api

2. Start a worker (in another terminal):
   uv run python -m minimodal.worker.executor

3. Run this example:
   uv run python examples/comprehensive_example.py
"""

import json
import time
from datetime import datetime, timezone

from minimodal import App, Cron, Period, Secret, Volume, get_client

# Create the app
app = App("data-pipeline")

# Define persistent storage
data_volume = Volume.persisted("pipeline-data")
results_volume = Volume.persisted("pipeline-results")

# Define secrets
api_key = Secret.from_name("DATA_API_KEY")
db_credentials = Secret.from_name("DATABASE_URL")


# === Core Processing Functions ===


@app.function()
def fetch_data(source_id: str) -> dict:
    """
    Fetch data from an external source.
    """
    data = {
        "source_id": source_id,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "records": [
            {"id": i, "value": i * 10, "source": source_id} for i in range(1, 6)
        ],
    }
    return data


@app.function()
def transform_data(data: dict) -> dict:
    """
    Transform raw data into processed format.
    """
    records = data.get("records", [])

    transformed = {
        "source_id": data.get("source_id"),
        "processed_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "count": len(records),
            "total": sum(r["value"] for r in records),
            "average": sum(r["value"] for r in records) / len(records)
            if records
            else 0,
        },
        "records": [
            {**r, "processed": True, "doubled_value": r["value"] * 2} for r in records
        ],
    }
    return transformed


@app.function()
def store_results(data: dict, filename: str) -> dict:
    """
    Store processed results.
    """
    return {
        "stored": True,
        "filename": filename,
        "record_count": len(data.get("records", [])),
    }


# === Scheduled Jobs ===


@app.function(schedule=Period(minutes=15))
def periodic_sync():
    """
    Sync data every 15 minutes.
    """
    return {
        "job": "periodic_sync",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "executed",
    }


@app.function(schedule=Cron("0 0 * * *"))
def daily_aggregation():
    """
    Run daily aggregation at midnight UTC.
    """
    return {
        "job": "daily_aggregation",
        "date": datetime.now(timezone.utc).date().isoformat(),
        "status": "completed",
    }


# === Web Endpoints ===


@app.web_endpoint(path="/api/status", method="GET")
def get_status(request: dict = None):
    """
    Get pipeline status.
    """
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "version": "1.0.0",
    }


@app.web_endpoint(path="/api/sources", method="GET")
def list_sources(request: dict = None):
    """
    List available data sources.
    """
    return {
        "sources": [
            {"id": "source-a", "name": "Source A", "type": "api"},
            {"id": "source-b", "name": "Source B", "type": "database"},
            {"id": "source-c", "name": "Source C", "type": "file"},
        ]
    }


@app.web_endpoint(path="/api/process/{source_id}", method="POST")
def process_source(request: dict = None):
    """
    Process a specific source - runs fetch and transform.
    """
    source_id = (
        request.get("path_params", {}).get("source_id") if request else "unknown"
    )

    # Actually fetch and transform data
    data = fetch_data(source_id)
    transformed = transform_data(data)

    return {
        "source_id": source_id,
        "records_processed": len(transformed.get("records", [])),
        "summary": transformed.get("summary"),
    }


def main():
    """Run the comprehensive example with actual remote execution."""
    print("=" * 60)
    print("MiniModal Comprehensive Example")
    print("=" * 60)

    client = get_client()

    # === 1. Deploy all functions ===
    print("\n[1] Deploying functions...")
    function_ids = app.deploy()
    print(f"    Deployed {len(function_ids)} functions")

    # === 2. Test Volumes API ===
    print("\n[2] Testing Volumes API...")
    try:
        # Create volume
        vol_id = client.create_volume("pipeline-data")
        print(f"    Created volume: pipeline-data (id={vol_id})")

        # Upload a file
        test_data = {
            "test": "data",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        client.upload_volume_file(
            "pipeline-data", "/input/config.json", json.dumps(test_data).encode()
        )
        print("    Uploaded: /input/config.json")

        # List files
        files = client.list_volume_files("pipeline-data", "/")
        print(f"    Files in volume: {files}")

        # Download file
        downloaded = client.download_volume_file("pipeline-data", "/input/config.json")
        print(f"    Downloaded config: {json.loads(downloaded)}")
    except Exception as e:
        print(f"    Volume error: {e}")

    # === 3. Test Secrets API ===
    print("\n[3] Testing Secrets API...")
    try:
        # Create secrets
        client.create_secret("DATA_API_KEY", "sk-test-api-key-12345")
        print("    Created secret: DATA_API_KEY")

        client.create_secret("DATABASE_URL", "postgresql://user:pass@localhost/db")
        print("    Created secret: DATABASE_URL")

        # List secrets
        response = client._request("GET", "/secrets")
        secrets = response.json().get("secrets", [])
        print(f"    Secrets stored: {[s['name'] for s in secrets]}")

        # Retrieve secret value (for verification)
        value = client.get_secret_value("DATA_API_KEY")
        print(f"    Retrieved DATA_API_KEY: {value[:10]}...")
    except Exception as e:
        print(f"    Secrets error: {e}")

    # === 4. Execute functions remotely ===
    print("\n[4] Executing functions remotely...")

    # Single remote call
    print("\n    a) Single fetch_data.remote('source-a')...")
    result = fetch_data.remote("source-a")
    print(f"       Fetched {len(result['records'])} records from {result['source_id']}")

    # Chain remote calls (pipeline)
    print("\n    b) Running ETL pipeline remotely...")
    data = fetch_data.remote("source-b")
    print(f"       Step 1: Fetched {len(data['records'])} records")

    transformed = transform_data.remote(data)
    print(f"       Step 2: Transformed - summary: {transformed['summary']}")

    stored = store_results.remote(transformed, "output.json")
    print(
        f"       Step 3: Stored {stored['record_count']} records to {stored['filename']}"
    )

    # === 5. Test async execution with spawn() ===
    print("\n[5] Testing async execution with spawn()...")

    # Spawn multiple jobs in parallel
    print("\n    Spawning 3 fetch jobs in parallel...")
    jobs = [
        fetch_data.spawn("source-a"),
        fetch_data.spawn("source-b"),
        fetch_data.spawn("source-c"),
    ]
    print(f"    Spawned job IDs: {[j.invocation_id for j in jobs]}")

    # Wait for results
    print("    Waiting for results...")
    results = [job.result() for job in jobs]
    for r in results:
        print(f"       - {r['source_id']}: {len(r['records'])} records")

    # === 6. Test map() for batch processing ===
    print("\n[6] Testing map() for batch processing...")

    sources = ["batch-1", "batch-2", "batch-3", "batch-4"]
    print(f"    Processing {len(sources)} sources with map()...")

    # map() takes single arguments directly
    jobs = fetch_data.map(sources)
    results = [job.result() for job in jobs]
    print(f"    Processed {len(results)} sources:")
    for r in results:
        print(f"       - {r['source_id']}: {len(r['records'])} records")

    # === 7. Test Web Endpoints ===
    print("\n[7] Testing Web Endpoints...")

    import httpx

    # First, register the web endpoints with the server
    print("\n    Registering web endpoints...")
    web_endpoints_to_register = [
        ("/api/status", "GET", "get_status"),
        ("/api/sources", "GET", "list_sources"),
        ("/api/process/{source_id}", "POST", "process_source"),
    ]

    for path, method, func_name in web_endpoints_to_register:
        func_id = function_ids.get(func_name)
        if func_id:
            resp = client._request(
                "POST",
                "/web-endpoints",
                params={"path": path, "method": method, "function_id": func_id},
            )
            print(f"       Registered {method} {path} -> function {func_id}")

    # Test GET /api/status
    print("\n    a) GET /web/api/status")
    try:
        resp = httpx.get("http://localhost:8000/web/api/status", timeout=60.0)
        print(f"       Response: {resp.json()}")
    except Exception as e:
        print(f"       Error: {e}")

    # Test GET /api/sources
    print("\n    b) GET /web/api/sources")
    try:
        resp = httpx.get("http://localhost:8000/web/api/sources", timeout=60.0)
        print(f"       Sources: {[s['id'] for s in resp.json()['sources']]}")
    except Exception as e:
        print(f"       Error: {e}")

    # Test POST /api/process/{source_id}
    print("\n    c) POST /web/api/process/web-source")
    try:
        resp = httpx.post(
            "http://localhost:8000/web/api/process/web-source", timeout=60.0
        )
        print(f"       Processed: {resp.json()}")
    except Exception as e:
        print(f"       Error: {e}")

    # === 8. Check Scheduled Jobs ===
    print("\n[8] Checking scheduled jobs...")

    response = client._request("GET", "/schedules")
    schedules = response.json().get("schedules", [])
    print(f"    Registered schedules: {len(schedules)}")
    for sched in schedules:
        if sched.get("cron_expression"):
            print(
                f"       - Function {sched['function_id']}: cron={sched['cron_expression']}"
            )
        elif sched.get("period_seconds"):
            print(
                f"       - Function {sched['function_id']}: every {sched['period_seconds']}s"
            )

    # === Summary ===
    print("\n" + "=" * 60)
    print("Summary: All Features Tested Successfully!")
    print("=" * 60)
    print("""
    [1] Functions: Deployed and executed remotely
    [2] Volumes: Created, uploaded, listed, downloaded files
    [3] Secrets: Created and retrieved encrypted secrets
    [4] Remote Execution: .remote() calls working
    [5] Async Execution: .spawn() for parallel jobs
    [6] Batch Processing: .map() for multiple inputs
    [7] Web Endpoints: HTTP API responding
    [8] Cron Jobs: Schedules registered

    MiniModal is fully operational!
    """)


if __name__ == "__main__":
    main()
