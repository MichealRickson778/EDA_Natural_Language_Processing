
import threading
import time
from collections.abc import Iterator
from typing import Any

import requests

from connectors.base_connector import BaseConnector, ConnectorError
from core.exceptions import RateLimitError


class APIConnectorError(ConnectorError):
    """Raise when we have issue on API connecting"""


class APIConnector(BaseConnector):
    #define the name and timout and needed parameters
    def __init__(self, name: str,
                timeout_s: int,
                host: str,
                port: int,
                rate_limit_translate: int,
                rate_limit_submit_job: int,
                max_retries: int,
                api_url: str,
                api_key: str,
                chunk_size: int,
                ssl_verify: bool | str):
        super().__init__(name, timeout_s)
        self.api_url = api_url
        self.api_key = api_key
        self.max_retries = max_retries
        self.host = host
        self.port = port
        self.rate_limit_translate = rate_limit_translate
        self.rate_limit_submit_job = rate_limit_submit_job
        self.chunk_size = chunk_size
        self._rps = rate_limit_translate/3600
        self._tokens = self._rps
        self._last_refill = time.time()
        self._lock = threading.Lock()
        self.ssl_verify = ssl_verify
        self._connected = False
        self._session = None
        self._base_url = None


    def connect(self) -> None:
        """Connect to the API get the response"""
        # guard against double connection
        if self._connected is True or self._session is not None:
            return
        if not self.api_key:
            raise APIConnectorError(f"API Key is empty :{self.api_key}")
        try:
            self._session = requests.Session()
            self._session.headers["Authorization"] = f"Bearer {self.api_key}"

            # validate the url its an external or internal
            if self.host and self.port:
                self._base_url = f"http://{self.host}:{self.port}"
            else:
                self._base_url = self.api_url

            response = self._session.get(self._base_url, verify=self.ssl_verify)

            if not response.status_code == 200:
                raise APIConnectorError(f"API connection faliure status code : {response.status_code}" )
            self._connected = True

        except APIConnectorError:
            raise
        except Exception as exc:
            raise APIConnectorError("API Connection faliure ",
                                    details={ "API URL":self._base_url}) from exc

    def disconnect(self) -> None:
        """Disconnect from the API or close the connection"""
        if self._connected is True or self._session is not None:
            self._session.close()
            self._connected = False
            self._session = None

    def health_check(self) -> bool:
        """ check the api health check and do basic checks"""
        # check the connection
        if self._connected is False or self._session is None:
            return False
        try:
            # validate the url its an external or internal
            self._acquire_token()
            response = self._session.get(self._base_url, verify=self.ssl_verify)

            if response.status_code == 200:
                return True
            return False
        except Exception:
            return False

    def exists(self, source, **kwargs) -> bool:
        """ Check for the end points are exists or not"""
        if self._connected is False or self._session is None:
            return False
        try:
            # validate the url its an external or internal
            exists_url = f"{self._base_url}/{source}"
            self._acquire_token()
            response = self._session.get(exists_url, verify=self.ssl_verify)

            if response.status_code == 200:
                return True
            return False
        except Exception:
            return False

    def read(self, source: str, **kwargs) -> Any:

        # API keys to validate response
        expected_keys = kwargs.get("expected_keys", None)

        if not self._connected or self._session is None:
            raise APIConnectorError("API not connected")

        _url = self._base_url if not source else f"{self._base_url}/{source}"

        for attempt in range(self.max_retries):
            try:
                # Added timeout to prevent hanging
                self._acquire_token()
                response = self._session.get(_url, timeout=self.timeout_s, verify=self.ssl_verify)

                if response.status_code == 200:
                    if expected_keys:
                        body = response.json()
                        missing = [key for key in expected_keys if key not in body]
                        if missing:
                            raise APIConnectorError(
                                "Unexpected response structure",
                                details={"missing_keys":missing, "response_body":body, "url": _url}
                            )
                        return body
                    return response.json()

                if response.status_code == 429:
                    raise RateLimitError(f"Rate limited: {response.status_code}")

                if 400 <= response.status_code < 500:
                    raise APIConnectorError(f"Client error: {response.status_code}")

                # 5xx Errors: Logic falls through to the sleep/retry below
                if response.status_code >= 500:
                    # Wait before retrying (1s, 2s, 4s...)
                    time.sleep(2 ** attempt)
                    pass

            except (requests.exceptions.RequestException, Exception) as exc:
                # If it's a specific API/Rate error we raised, re-raise it immediately
                if isinstance(exc, (RateLimitError, APIConnectorError)):
                    raise
                # Otherwise, treat network jitters as a reason to retry
                if attempt == self.max_retries - 1:
                    raise APIConnectorError(f"Request failed after {self.max_retries} attempts") from exc

        raise APIConnectorError(f"Max retries exhausted for URL: {_url}")

    def write(self, destination: str, data: dict, **kwargs) -> Any:
        """Write data using post method"""

        # API keys to validate response
        expected_keys = kwargs.get("expected_keys", None)

        if not self._connected or self._session is None:
            raise APIConnectorError("API not connected")

        _url = self._base_url if not destination else f"{self._base_url}/{destination}"

        for attempt in range(self.max_retries):
            try:
                # Added timeout to prevent hanging
                self._acquire_token()
                response = self._session.post(_url, json=data, timeout=self.timeout_s, verify=self.ssl_verify)

                if response.status_code == 200:
                    if expected_keys:
                        body = response.json()
                        missing = [key for key in expected_keys if key not in body]
                        if missing:
                            raise APIConnectorError(
                                "Unexpected response structure",
                                details={"missing_keys":missing, "response_body":body, "url": _url}
                            )
                        return body
                    return response.json()

                if response.status_code == 429:
                    raise RateLimitError(f"Rate limited: {response.status_code}")

                if 400 <= response.status_code < 500:
                    raise APIConnectorError(f"Client error: {response.status_code}")

                # 5xx Errors: Logic falls through to the sleep/retry below
                if response.status_code >= 500:
                    # Wait before retrying (1s, 2s, 4s...)
                    time.sleep(2 ** attempt)
                    pass

            except (requests.exceptions.RequestException, Exception) as exc:
                # If it's a specific API/Rate error we raised, re-raise it immediately
                if isinstance(exc, (RateLimitError, APIConnectorError)):
                    raise
                # Otherwise, treat network jitters as a reason to retry
                if attempt == self.max_retries - 1:
                    raise APIConnectorError(f"Request failed after {self.max_retries} attempts") from exc

        raise APIConnectorError(f"Max retries exhausted for URL: {_url}")

    def post_stream(self, destination: str, data: dict, **kwargs) -> Iterator:
        """Stream the data as little chunks"""
        if not self._connected or self._session is None:
            raise APIConnectorError("API not connected")

        _url = self._base_url if not destination else f"{self._base_url}/{destination}"

        for attempt in range(self.max_retries):
            try:
                # Added timeout to prevent hanging
                self._acquire_token()
                response = self._session.post(_url, json=data, stream=True, timeout=self.timeout_s, verify=self.ssl_verify)
                if response.status_code == 200:
                    for chunk in response.iter_content(chunk_size=self.chunk_size):
                        if chunk:
                            yield chunk
                    return

                if response.status_code == 429:
                    raise RateLimitError(f"Rate limited: {response.status_code}")

                if 400 <= response.status_code < 500:
                    raise APIConnectorError(f"Client error: {response.status_code}")

                # 5xx Errors: Logic falls through to the sleep/retry below
                if response.status_code >= 500:
                    # Wait before retrying (1s, 2s, 4s...)
                    time.sleep(2 ** attempt)
                    pass

            except (requests.exceptions.RequestException, Exception) as exc:
                # If it's a specific API/Rate error we raised, re-raise it immediately
                if isinstance(exc, (RateLimitError, APIConnectorError)):
                    raise
                # Otherwise, treat network jitters as a reason to retry
                if attempt == self.max_retries - 1:
                    raise APIConnectorError(f"Request failed after {self.max_retries} attempts") from exc

        raise APIConnectorError(f"Max retries exhausted for URL: {_url}")

    def set_rate_limit(self, rate_limit_translate: int) -> None:
        """
        Configures the client-side rate limiter.
        rps = requests per second.
        Uses token bucket algorithm to throttle outgoing calls.
        """
        with self._lock:
            self._rps = rate_limit_translate / 3600
            self._tokens = self._rps
            self._last_refill = time.time()

    def _acquire_token(self):
        while True:
            with self._lock:
                now = time.time()
                elapsed_time = now - self._last_refill
                refill_amount = elapsed_time * self._rps

                self._tokens = min(self._tokens + refill_amount, self._rps)
                self._last_refill = now

                if self._tokens >= 1:
                    self._tokens -= 1
                    return

                wait_time = 1 / self._rps

            time.sleep(wait_time)
