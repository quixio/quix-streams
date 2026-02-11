"""
REST Catalog Client

Simple HTTP client for REST Catalog API interactions.
"""

from typing import Optional

import requests


class CatalogClient:
    """Simple HTTP client for REST Catalog API interactions."""

    def __init__(self, base_url: str, auth_token: Optional[str] = None):
        """
        Initialize the catalog client.

        :param base_url: Base URL of the REST Catalog API
        :param auth_token: Optional authentication token for API requests
        """
        self.base_url = base_url.rstrip("/")
        self.auth_token = auth_token
        self._session = requests.Session()

        # Set up authentication header if token is provided
        if self.auth_token:
            self._session.headers["Authorization"] = f"Bearer {self.auth_token}"

    def get(self, path: str, timeout: int = 30) -> requests.Response:
        """
        Make a GET request to the catalog API.

        :param path: API endpoint path (will be appended to base_url)
        :param timeout: Request timeout in seconds
        :returns: Response object from requests library
        """
        url = f"{self.base_url}{path}"
        return self._session.get(url, timeout=timeout)

    def post(
        self, path: str, json: Optional[dict] = None, timeout: int = 30
    ) -> requests.Response:
        """
        Make a POST request to the catalog API.

        :param path: API endpoint path (will be appended to base_url)
        :param json: JSON payload to send in request body
        :param timeout: Request timeout in seconds
        :returns: Response object from requests library
        """
        url = f"{self.base_url}{path}"
        return self._session.post(url, json=json, timeout=timeout)

    def put(
        self, path: str, json: Optional[dict] = None, timeout: int = 30
    ) -> requests.Response:
        """
        Make a PUT request to the catalog API.

        :param path: API endpoint path (will be appended to base_url)
        :param json: JSON payload to send in request body
        :param timeout: Request timeout in seconds
        :returns: Response object from requests library
        """
        url = f"{self.base_url}{path}"
        return self._session.put(url, json=json, timeout=timeout)

    def __str__(self):
        """String representation showing the base URL."""
        return self.base_url
