"""
Example: Using Secrets for Secure Configuration

Secrets allow you to securely store sensitive values like API keys,
database credentials, and other configuration that shouldn't be
hardcoded or stored in plain text.

To run this example:

1. Start the server:
   uv run python -m minimodal.server.api

2. Start a worker (in another terminal):
   uv run python -m minimodal.worker.executor

3. Run this example:
   uv run python examples/secrets_example.py
"""

import os

from minimodal import App, Secret, secret_from_env

# Create an app
app = App("secrets-example")

# Create secret references
# These will be injected as environment variables when functions run
api_key = Secret.from_name("API_KEY")
db_url = Secret.from_name("DATABASE_URL")


@app.function()
def call_external_api(endpoint: str) -> dict:
    """
    Call an external API using a secret API key.

    In real usage, the API_KEY secret would be injected as an
    environment variable before this function runs.
    """
    # In a real function, you would do:
    # api_key = os.environ.get("API_KEY")
    # response = requests.get(endpoint, headers={"Authorization": f"Bearer {api_key}"})

    return {
        "endpoint": endpoint,
        "status": "would use API_KEY from environment",
    }


@app.function()
def query_database(query: str) -> dict:
    """
    Query a database using secret credentials.
    """
    # In real usage:
    # db_url = os.environ.get("DATABASE_URL")
    # conn = psycopg2.connect(db_url)

    return {
        "query": query,
        "status": "would use DATABASE_URL from environment",
    }


def demonstrate_secrets_api():
    """Demonstrate the Secrets SDK API."""
    print("=" * 60)
    print("Secrets API Demonstration")
    print("=" * 60)

    from minimodal import get_client

    client = get_client()

    # Create secrets
    print("\n1. Creating secrets...")

    try:
        # Create an API key secret
        secret_id = client.create_secret("DEMO_API_KEY", "sk-demo-12345-secret-key")
        print(f"   Created DEMO_API_KEY (ID: {secret_id})")

        # Create a database URL secret
        secret_id = client.create_secret(
            "DEMO_DB_URL",
            "postgresql://user:password@localhost:5432/mydb",
        )
        print(f"   Created DEMO_DB_URL (ID: {secret_id})")

        # Create multiple secrets at once using from_dict
        secrets = Secret.from_dict(
            {
                "AWS_ACCESS_KEY": "AKIA...",
                "AWS_SECRET_KEY": "secret...",
            }
        )
        print(f"   Created {len(secrets)} secrets via from_dict")

    except Exception as e:
        print(f"   Error creating secrets: {e}")

    # List secrets (values are never shown)
    print("\n2. Listing secrets (values hidden)...")
    try:
        response = client._request("GET", "/secrets")
        data = response.json()
        for secret in data.get("secrets", []):
            print(f"   - {secret['name']} (created: {secret['created_at'][:10]})")
    except Exception as e:
        print(f"   Error listing secrets: {e}")

    # Update a secret
    print("\n3. Updating a secret...")
    try:
        client.create_secret("DEMO_API_KEY", "sk-new-updated-key-67890")
        print("   Updated DEMO_API_KEY with new value")
    except Exception as e:
        print(f"   Error updating secret: {e}")

    # Retrieve secret value (internal endpoint - for workers only)
    print("\n4. Retrieving secret value (internal API)...")
    try:
        value = client.get_secret_value("DEMO_API_KEY")
        # Show only first/last few chars for security
        masked = value[:5] + "..." + value[-5:] if len(value) > 10 else "***"
        print(f"   DEMO_API_KEY value: {masked}")
    except Exception as e:
        print(f"   Error retrieving secret: {e}")

    # Clean up
    print("\n5. Cleaning up demo secrets...")
    try:
        client._request("DELETE", "/secrets/DEMO_API_KEY")
        print("   Deleted DEMO_API_KEY")
        client._request("DELETE", "/secrets/DEMO_DB_URL")
        print("   Deleted DEMO_DB_URL")
    except Exception as e:
        print(f"   Cleanup note: {e}")

    print("\n" + "=" * 60)


def demonstrate_secret_from_env():
    """Show how to create secrets from environment variables."""
    print("\n" + "=" * 60)
    print("Creating Secrets from Environment Variables")
    print("=" * 60)

    # Set a demo environment variable
    os.environ["MY_LOCAL_SECRET"] = "local-secret-value"

    try:
        # Create secret from environment variable
        secret = secret_from_env("MY_LOCAL_SECRET")
        print(f"\n1. Created secret from env: {secret}")
        print(f"   Name: {secret.name}")
        print(f"   Has local value: {secret._local_value is not None}")

    except KeyError as e:
        print(f"\n   Environment variable not found: {e}")

    # Try to get a non-existent env var
    print("\n2. Trying non-existent env var...")
    try:
        secret = secret_from_env("DEFINITELY_NOT_SET")
    except KeyError:
        print("   Correctly raised KeyError for missing env var")

    print("\n" + "=" * 60)


def main():
    """Run the secrets example."""
    print("Secrets Example")
    print("-" * 40)

    # Demonstrate the Secrets API
    demonstrate_secrets_api()

    # Show secret_from_env usage
    demonstrate_secret_from_env()

    print("\n" + "=" * 60)
    print("Function Deployment with Secrets")
    print("=" * 60)

    # Deploy functions
    print("\nDeploying functions...")
    app.deploy()

    # In a full implementation, you would use secrets like this:
    #
    # api_secret = Secret.from_name("API_KEY")
    # api_secret.set("sk-your-actual-key")
    #
    # @app.function(secrets=[api_secret])
    # def my_function():
    #     # Secret is injected as environment variable
    #     api_key = os.environ["API_KEY"]
    #     # Use the key...

    print("\nNote: Full secret injection requires worker support.")
    print("Secrets are encrypted at rest and only decrypted when needed.")


if __name__ == "__main__":
    main()
