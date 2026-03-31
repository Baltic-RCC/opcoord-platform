import requests
import time
import json
import tarfile
from pathlib import Path
from typing import Optional
from pydantic import SecretStr
from config.integrations import OperatorFabricSettings
from loguru import logger


conf = OperatorFabricSettings()


class TokenManager:
    def __init__(
        self,
        base_url: str = conf.host,
        username: str = conf.username,
        password: str = conf.password,
        port: int = 2002,
        timeout: int = 10,
    ):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password.get_secret_value() if isinstance(password, SecretStr) else password
        self.port = port
        self.timeout = timeout

        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.expires_at: float = 0  # epoch timestamp

    @property
    def token_url(self):
        return f"{self.base_url}:{self.port}/auth/token"

    def _request_token(self, data: dict) -> dict:
        response = requests.post(
            self.token_url,
            data=data,
            timeout=self.timeout,
        )

        response.raise_for_status()
        return response.json()

    def login(self):
        """Initial login using password grant"""
        payload = {
            "username": self.username,
            "password": self.password,
            "grant_type": "password",
        }

        data = self._request_token(payload)
        self._store_tokens(data)

    def refresh(self):
        """Refresh using refresh_token"""
        if not self.refresh_token:
            raise RuntimeError("No refresh token available")

        payload = {
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token",
        }

        data = self._request_token(payload)
        self._store_tokens(data)

    def _store_tokens(self, data: dict):
        self.access_token = data.get("access_token")
        self.refresh_token = data.get("refresh_token")

        expires_in = data.get("expires_in", 3600)

        # subtract small buffer (30s) to avoid edge expiry
        self.expires_at = time.time() + expires_in - 30

    def is_expired(self) -> bool:
        return time.time() >= self.expires_at

    def get_valid_token(self) -> str:
        """
        Returns valid access token.
        Automatically refreshes or re-logins if needed.
        """
        if not self.access_token:
            self.login()

        elif self.is_expired():
            try:
                self.refresh()
            except Exception:
                # If refresh fails, try full login
                self.login()

        return self.access_token


class AuthenticatedSession:
    def __init__(self, base_url: str = conf.host):
        self.base_url = base_url.rstrip("/")
        self.tm = TokenManager(base_url=base_url)
        self.session = requests.Session()

    def request(self, method, url, **kwargs):
        token = self.tm.get_valid_token()

        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {token}"
        kwargs["headers"] = headers

        # Execute the request
        response = self.session.request(method, url, **kwargs)

        # If token expired or invalid → refresh and retry once
        if response.status_code == 401:
            self.tm.refresh()
            headers["Authorization"] = f"Bearer {self.tm.access_token}"
            kwargs["headers"] = headers
            response = self.session.request(method, url, **kwargs)

        # Inspect response for common shapes (requests-like, dict, or custom)
        status = None
        try:
            if hasattr(response, "status_code"):
                status = int(getattr(response, "status_code"))
            elif isinstance(response, dict):
                status = int(response.get("status") or response.get("status_code") or response.get("code") or 0)
            elif hasattr(response, "get"):
                status = int(response.get("status") or response.get("status_code") or response.get("code") or 0)
        except Exception:
            logger.debug("Unable to parse status code from OperatorFabric response", response=response)

        # Log success or failure based on status when available
        if status:
            if 200 <= status < 300:
                logger.success(f"Card published to OperatorFabric successfully (status={status})")
            else:
                logger.error(f"OperatorFabric returned error status={status}; response={response}")
        else:
            # No status available: log the raw response for debugging
            logger.warning(f"OperatorFabric returned an unexpected response shape; response={response}")

        response.raise_for_status()
        return response

    def post_card(self, card_json=None, **kwargs):
        endpoint_url = f"{self.base_url}:2102/cards"
        return self.request("POST", url=endpoint_url, json=card_json, **kwargs)

    def get_card(self, card_id: str, **kwargs):
        endpoint_url = f"{self.base_url}:2002/cards-consultation/cards/{card_id}"
        return self.request("GET", url=endpoint_url, **kwargs)

    def post_process_bundle(self, bundle_folder_name: str, **kwargs):
        # Package the bundle directory into a tar.gz archive
        bundle_dir = Path(bundle_folder_name)
        output_tar = Path(f"bundle.tar.gz")
        with tarfile.open(output_tar, "w:gz") as tar:
            for item in bundle_dir.iterdir():
                tar.add(item, arcname=item.name)

        print("Archive created:", output_tar)

        # Send the tar.gz file to the API
        endpoint_url = f"{self.base_url}:2100/businessconfig/processes"
        headers = {"accept": "application/json"}
        with open("bundle.tar.gz", "rb") as f:
            files = {
                "file": ("bundle.tar.gz", f, "application/gzip")
            }
            response = self.request("POST", url=endpoint_url, headers=headers, files=files, **kwargs)

        print(response.content)

        return response

    def create_perimeter(self, perimeter_json=None, **kwargs):
        endpoint_url = f"{self.base_url}:2103/perimeters"
        return self.request("POST", url=endpoint_url, json=perimeter_json, **kwargs)

    def add_perimeter_to_group(self, perimeter_id: str, group_name: str, **kwargs):
        endpoint_url = f"{self.base_url}:2103/perimeters/{perimeter_id}/groups"
        payload = [group_name]
        return self.request("PUT", url=endpoint_url, json=payload, **kwargs)

    def post_process_groups(self, json_file_path: str, **kwargs):
        endpoint_url = f"{self.base_url}:2100/businessconfig/processgroups"
        headers = {"accept": "application/json"}
        with open(json_file_path, "rb") as f:
            files = {
                "file": (json_file_path.split("/")[-1], f, "application/json")
            }
            response = self.request("POST", url=endpoint_url, headers=headers, files=files, **kwargs)

        return response


if __name__ == "__main__":
    # Example usage of TokenManager and AuthenticatedSession
    tm = TokenManager(
        base_url="http://localhost",
        username="admin",
        password="test"
    )
    api_token = tm.get_valid_token()

    # Client with automatic token handling
    client = AuthenticatedSession(base_url="http://localhost")

    # Send example card data
    with open("card.json", "r") as f:
        payload = json.load(f)
    payload["startDate"] = int(time.time() * 1000)
    response = client.post_card(card_json=payload)

    # Get card by ID
    card_id = "defaultProcess.hello-world-1"
    response = client.get_card(card_id=card_id)
    print(json.dumps(response.json(), indent=4, ensure_ascii=False))

    # Upload process bundle
    response = client.post_process_bundle(bundle_folder_name="bundle")

    # Setup perimeter
    with open("perimeter.json", "r") as f:
        payload = json.load(f)
    response = client.create_perimeter(perimeter_json=payload)
    client.add_perimeter_to_group(perimeter_id="rcc-perimeter", group_name="Dispatcher")

    # Upload process group config
    response = client.post_process_groups(json_file_path="process_group.json")
