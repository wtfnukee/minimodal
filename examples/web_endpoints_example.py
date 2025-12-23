"""
Example: Using Web Endpoints to Create HTTP APIs

Web Endpoints allow you to expose your functions as HTTP APIs.
Incoming requests are routed to your functions, which process
the request and return a response.

To run this example:

1. Start the server:
   uv run python -m minimodal.server.api

2. Start a worker (in another terminal):
   uv run python -m minimodal.worker.executor

3. Run this example:
   uv run python examples/web_endpoints_example.py

4. Test the endpoints (once deployed):
   curl http://localhost:8000/web/api/hello
   curl http://localhost:8000/web/api/users/123
   curl -X POST http://localhost:8000/web/api/data -d '{"key": "value"}'
"""

from minimodal import App, WebRequest, WebResponse

# Create an app
app = App("web-api-example")


# Simple GET endpoint
@app.web_endpoint(path="/api/hello", method="GET")
def hello_handler():
    """
    Simple hello endpoint.

    Returns a JSON greeting.
    """
    return {"message": "Hello from MiniModal!", "status": "ok"}


# GET endpoint with path parameter
@app.web_endpoint(path="/api/users/{user_id}", method="GET")
def get_user(user_id: str = None, request: dict = None):
    """
    Get user by ID.

    Path parameters are extracted and passed to the function.
    """
    # In real usage, you'd query a database
    users = {
        "1": {"id": "1", "name": "Alice", "email": "alice@example.com"},
        "2": {"id": "2", "name": "Bob", "email": "bob@example.com"},
        "3": {"id": "3", "name": "Charlie", "email": "charlie@example.com"},
    }

    # Extract user_id from request if passed that way
    if request and "path_params" in request:
        user_id = request.get("path_params", {}).get("user_id", user_id)

    if user_id in users:
        return users[user_id]
    else:
        return {"error": "User not found"}, 404


# POST endpoint with request body
@app.web_endpoint(path="/api/data", method="POST")
def create_data(request: dict = None):
    """
    Create new data.

    Request body is parsed as JSON and passed to the function.
    """
    body = request.get("body", {}) if request else {}

    # Process the data
    result = {
        "received": body,
        "processed": True,
        "id": "new-item-123",
    }

    return result, 201  # Return with status code


# Endpoint demonstrating query parameters
@app.web_endpoint(path="/api/search", method="GET")
def search(request: dict = None):
    """
    Search endpoint with query parameters.

    Query params like ?q=term&limit=10 are passed in request.
    """
    query_params = request.get("query_params", {}) if request else {}

    q = query_params.get("q", "")
    limit = int(query_params.get("limit", "10"))

    # Simulate search results
    results = [
        {"id": i, "title": f"Result {i} for '{q}'"}
        for i in range(1, min(limit + 1, 6))
    ]

    return {
        "query": q,
        "limit": limit,
        "results": results,
        "total": len(results),
    }


# DELETE endpoint
@app.web_endpoint(path="/api/items/{item_id}", method="DELETE")
def delete_item(request: dict = None):
    """
    Delete an item by ID.
    """
    item_id = request.get("path_params", {}).get("item_id") if request else None

    # In real usage, you'd delete from database
    return {
        "deleted": True,
        "item_id": item_id,
    }


def demonstrate_web_classes():
    """Demonstrate WebRequest and WebResponse classes."""
    print("=" * 60)
    print("WebRequest and WebResponse Classes")
    print("=" * 60)

    # WebRequest example
    print("\n1. Creating a WebRequest...")
    request = WebRequest(
        method="POST",
        path="/api/users",
        path_params={"user_id": "123"},
        query_params={"include": "profile", "format": "json"},
        headers={"Authorization": "Bearer token123", "Content-Type": "application/json"},
        body={"name": "New User", "email": "new@example.com"},
    )

    print(f"   Method: {request.method}")
    print(f"   Path: {request.path}")
    print(f"   Path Params: {request.path_params}")
    print(f"   Query Params: {request.query_params}")
    print(f"   Body: {request.body}")

    # Convert to dict (for serialization)
    print("\n2. Converting request to dict...")
    request_dict = request.to_dict()
    print(f"   Keys: {list(request_dict.keys())}")

    # Create from dict
    print("\n3. Creating request from dict...")
    request2 = WebRequest.from_dict(request_dict)
    print(f"   Reconstructed method: {request2.method}")

    # WebResponse example
    print("\n4. Creating a WebResponse...")
    response = WebResponse(
        body={"status": "success", "data": [1, 2, 3]},
        status_code=200,
        content_type="application/json",
        headers={"X-Request-ID": "abc123"},
    )

    print(f"   Status: {response.status_code}")
    print(f"   Content-Type: {response.content_type}")
    print(f"   Body: {response.body}")
    print(f"   Headers: {response.headers}")

    print("\n" + "=" * 60)


def demonstrate_endpoint_registration():
    """Show how endpoints are registered."""
    print("\n" + "=" * 60)
    print("Endpoint Registration")
    print("=" * 60)

    print("\n1. Registered endpoints in app:")
    for key, endpoint in app.web_endpoints.items():
        print(f"   {endpoint}")

    print("\n2. Endpoint details:")
    for key, endpoint in app.web_endpoints.items():
        print(f"   - Path: {endpoint.path}")
        print(f"     Method: {endpoint.method}")
        print(f"     Handler: {endpoint.__name__}")
        print()

    print("=" * 60)


def test_local_execution():
    """Test endpoints locally (without server)."""
    print("\n" + "=" * 60)
    print("Local Endpoint Testing")
    print("=" * 60)

    # Test hello endpoint
    print("\n1. Testing hello_handler locally...")
    result = hello_handler()
    print(f"   Result: {result}")

    # Test get_user endpoint
    print("\n2. Testing get_user locally...")
    result = get_user(user_id="1")
    print(f"   Result: {result}")

    # Test with request dict (as it would be called from web router)
    print("\n3. Testing with request dict...")
    request = {
        "method": "GET",
        "path": "/api/search",
        "query_params": {"q": "minimodal", "limit": "3"},
    }
    result = search(request=request)
    print(f"   Search result: {result}")

    print("\n" + "=" * 60)


def main():
    """Run the web endpoints example."""
    print("Web Endpoints Example")
    print("-" * 40)

    # Demonstrate helper classes
    demonstrate_web_classes()

    # Show registered endpoints
    demonstrate_endpoint_registration()

    # Test locally
    test_local_execution()

    print("\n" + "=" * 60)
    print("Deploying Functions")
    print("=" * 60)

    # Deploy functions
    print("\nDeploying functions...")
    app.deploy()

    print("\nEndpoints available at:")
    print("  GET  http://localhost:8000/web/api/hello")
    print("  GET  http://localhost:8000/web/api/users/{user_id}")
    print("  POST http://localhost:8000/web/api/data")
    print("  GET  http://localhost:8000/web/api/search?q=term&limit=5")
    print("  DELETE http://localhost:8000/web/api/items/{item_id}")

    print("\nTo register endpoints with the server, use the /web-endpoints API.")
    print("Full web routing requires function IDs from deployment.")


if __name__ == "__main__":
    main()
