import os
from pathlib import Path
from typing import Any

import chardet

from connectors.base_connector import BaseConnector, ConnectorError


class FileConnectorError(ConnectorError):
    """raise when we can't able to connect the file"""


class FileConnector(BaseConnector):
    def __init__(self, name: str, timeout_s: int, base_path: Path):
        super().__init__(name, timeout_s)
        self.base_path = base_path

    def exists(self, source, **kwargs) -> bool:
        """Check the source path is exists or not"""
        path = Path(source)
        if not path.exists():
            return False
        if path.is_dir():
            return os.access(path, os.R_OK)
        if path.is_file():
            return os.access(path, os.R_OK)
        return False

    def connect(self) -> None:
        """
        Verify base_path exists, is a directory, and is readable
        sets_connected to true on success.
        Raises FileConnectionError if any check fails.

        """
        if not self.exists(str(self.base_path)):
            raise FileConnectorError(f"base path not exists: {self.base_path}",
                                     details={"path": str(self.base_path)})
        self._connected = True

    def disconnect(self) -> None:
        """
        Once it is called _connected is to set the False flag
        """
        self._connected = False

    def health_check(self) -> bool:
        """
        Check the connection and files are available in silently

        """
        if not self._connected:
            return False
        if not self.exist_s(str(self.base_path)):
            return False
        return True

    def read(self, source, **kwargs) -> Any:
        """Read the file
        - Check the file exists or not
        - detect the encoding and confidence with minimum byte file
        - if low confidence fall back to utf-8
        - if encoding detection fails -> fall back to utf-8"""
        path = Path(source)

        if not self.exists(str(path)):
            raise FileConnectorError(f"base path not exists: {path}",
                                     details={"path": str(path)})

        try:
            with open(path, 'rb') as f:
               raw = f.read(1024)
            result = chardet.detect(raw)
            encoding = result.get("encoding","utf-8") or "utf-8"
            confidence = result.get("confidence", 0.0)

            if confidence < 0.8:
                encoding = "utf-8"

            with open(path, encoding=encoding) as f:
                content = f.read()
        except Exception as exc:
            raise FileConnectorError(f"cannot read file:{path}",
                                     details={"path":str(path)}) from exc
        return content


    def write(self, destination:str, data:Any, **kwargs) -> None:
        """Write the data into destination directory
        - check destination directory exists
        - to prevent atomic write use temp directory
        - once safely write in the destination delete temp
        """

        path = Path(destination)

        if not self.exists(str(path.parent)):
            raise FileConnectorError(f"destination directory or file not found: {destination}",
                                     details={"path":str(path)})
        try:
            temp_path = Path(str(destination) + ".tmp")

            with open(temp_path, "w", encoding="utf-8") as f:
                f.write(data)

            os.replace(temp_path, path)

        except Exception as exc:
            temp_path.unlink(missing_ok = True)
            raise FileConnectorError(f"Error occurred during the write operation: {destination}",
                                     details={"destination":str(destination),
                                              "temporary_path":str(temp_path)}) from exc


    def list_files(self,source: str, **kwargs) -> list[Path]:
        """List the files matches with pattern """

        path = Path(source)
        pattern = kwargs.get("pattern", "*")

        if not self.exists(path):
            raise FileConnectorError(f"source directory or file not found: {source}",
                                     details={"path":str(path)})
        try:
            files = [f for f in path.glob(pattern) if f.is_file()]
            return files
        except Exception as exc:
            raise FileConnectorError(f"Error occurred when list the files: {source}",
                                     details={"path":str(path),"pattern":pattern}) from exc

