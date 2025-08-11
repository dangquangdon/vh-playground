import httpx
import os

# zurAmgTnsyhV9hGQ4w2QoxEi3CD2VyG5tDdGYPPl

def get_api_client() -> httpx.Client:
    """
    Get an HTTP client that is authenticated with the Valohai API token,
    and configured to use the base URL from the environment.
    """
    client = httpx.Client(
        base_url=os.environ.get("VH_API_BASE_URL", "http://localhost:8000/"),
        timeout=5,
    )

    valohai_api_token = os.environ["VH_API_TOKEN"]
    client.headers["Authorization"] = f"Token {valohai_api_token}"
    return client