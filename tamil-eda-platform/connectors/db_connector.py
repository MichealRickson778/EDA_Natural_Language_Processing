from contextlib import contextmanager
from typing import Any

from sqlalchemy import create_engine, inspect, text

from connectors.base_connector import BaseConnector, ConnectorError


class DBConnectorError(ConnectorError):
    """Raise when we face the issue in db connector"""

class DBConnector(BaseConnector):
    def __init__(self,name: str, timeout_s: int,
                 db_url: str,
                 pool_size: int,
                 max_overflow: int,
                 echo: bool
                 ):
        super().__init__(name, timeout_s)
        self.db_url = db_url
        self.pool_size = pool_size
        self.echo = echo
        self.max_overflow = max_overflow
        self._connected = False
        self._engine = None
        self._transaction_conn = None

    def connect(self) -> None:
        """
        connect to the database coz we need active connection for all operations
        sets_connected to true on success.
        Raises DBConnectionError if connection fails.

        """
        try:

            if self.db_url.startswith("sqlite"):
               self._engine = create_engine(self.db_url, echo=self.echo)
            else:
                self._engine = create_engine(self.db_url,
                               pool_size=self.pool_size,
                               max_overflow=self.max_overflow,
                               echo=self.echo,
                               pool_timeout=self.timeout_s,
                               pool_pre_ping=True)
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self._connected = True

        except TimeoutError as exc:
            raise DBConnectorError("Connection pool exhausted",
                           details={"pool_size": self.pool_size}) from exc
        except Exception as exc:
            raise DBConnectorError(
                f"Failed to connect to database: {self.db_url}",
                details={"db_url": self.db_url}
            ) from exc

    def disconnect(self) -> None:
        """disconnect the database or close the connection"""
        if self._connected is True or self._engine is not None:
            self._engine.dispose()
            self._engine = None
        self._connected = False

    def health_check(self) -> bool:
        """ check the database connection silently """
        if not self._connected or self._engine is None:
            return False
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def exists(self, source, **kwargs) -> bool:
        """check for the existance of table"""
        if not self._connected or self._engine is None:
            return False
        try:
            inspector = inspect(self._engine)
            table_name = inspector.get_table_names()
            if source in table_name:
                with self._engine.connect() as conn:
                    conn.execute(text(f"SELECT 1 FROM {source} LIMIT 1"))
                    return True
            return False
        except Exception:
            return False

    def read(self, source, **kwargs) -> Any:
        """Read the data from db using query"""

        # validation to read and check the connection is active
        if not self._connected or self._engine is None:
            raise DBConnectorError(f"db connection is closed and engine is not started yet connected :{self._connected}",
                                   details={"connected":self._connected, "engine":self._engine})

        # getting the query and params from kwrgs
        query = kwargs.get("query")
        params = kwargs.get("params")

        # validate the query
        if query is None:
            raise DBConnectorError("query required to read the data from db")

        try:
            with self._engine.connect() as conn:
                result = conn.execute(text(query),params)
                list_of_dicts = result.mappings().all()
            return list_of_dicts
        except TimeoutError as exc:
            raise DBConnectorError("Connection pool exhausted",
                           details={"pool_size": self.pool_size}) from exc
        except Exception as exc:
            raise DBConnectorError(f"while read the data from db got an error query:{query}",
                                   details={"query":query,"params":params}) from exc

    def write(self, destination, data, **kwargs) -> None:
        """excute INSERT/UPDATE/DELETE"""
        if not self._connected or self._engine is None:
            raise DBConnectorError(f"db connection is closed and engine is not started yet connected :{self._connected}",
                                   details={"connected":self._connected, "engine":self._engine})

        # getting the query and params from kwrgs
        query = kwargs.get("query")
        params = kwargs.get("params")

        # validate the query
        if query is None:
            raise DBConnectorError("query required to INSERT/UPDATE/DELETE the data from db")

        try:
            """# engine.begin can cannect and commit automatically and rollback if error occured """
            conn = self._transaction_conn
            if conn:
                conn.execute(text(query), params)  # use it
            else:
                with self._engine.begin() as conn:
                    conn.execute(text(query),params)
        except TimeoutError as exc:
            raise DBConnectorError("Connection pool exhausted",
                           details={"pool_size": self.pool_size}) from exc
        except Exception as exc:
            raise DBConnectorError(f"while write the data from db got an error query:{query}",
                                   details={"query":query,"params":params}) from exc

    @contextmanager
    def transaction(self):

        try:
            with self._engine.begin() as conn:
                self._transaction_conn = conn
                yield
        except Exception:
            raise
        finally:
            self._transaction_conn = None
















