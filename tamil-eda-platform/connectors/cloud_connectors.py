import os


from connectors.base_connector import BaseConnector, ConnectorError

class CloudConnectorError(ConnectorError):
    """Raise when the error accore on cloud connector"""

class CloudConnector(BaseConnector):
    """Connecting to the cloud"""
    def __init__(self, name: str,
                timeout_s: int,
                service_provider: str,
                bucket: str,
                credentials: str,
                region:str):
        super().__init__(name, timeout_s)
        self.service_provider = service_provider
        self.bucket = bucket
        self.credentials = credentials
        self.region = region
        self._client = None
        self._connected = False
