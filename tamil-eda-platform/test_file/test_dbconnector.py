from unittest.mock import patch

import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from connectors.db_connector import DBConnector, DBConnectorError

 # Replace your_module with actual filename

# --- Configuration for Integration Tests ---
SQLITE_URL = "sqlite:///:memory:"

@pytest.fixture
def connector():
    """Provides a fresh DBConnector instance for each test."""
    return DBConnector(
        name="test_db",
        timeout_s=10,
        db_url=SQLITE_URL,
        pool_size=5,
        max_overflow=10,
        echo=False
    )

## --- Integration Tests (Real SQLite in-memory) ---

def test_connect_success(connector):
    connector.connect()
    assert connector._connected is True
    assert connector._engine is not None
    connector.disconnect()

def test_health_check(connector):
    # Should be false before connecting
    assert connector.health_check() is False

    connector.connect()
    assert connector.health_check() is True
    connector.disconnect()

def test_write_and_read_integration(connector):
    connector.connect()

    # Create a table
    with connector._engine.begin() as conn:
        conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"))

    # Test Write
    connector.write(destination="users", data=None,
                    query="INSERT INTO users (name) VALUES (:name)",
                    params={"name": "Alice"})

    # Test Read
    results = connector.read(source="users",
                             query="SELECT * FROM users WHERE name = :name",
                             params={"name": "Alice"})

    assert len(results) == 1
    assert results[0]["name"] == "Alice"
    connector.disconnect()

def test_exists_integration(connector):
    connector.connect()
    with connector._engine.begin() as conn:
        conn.execute(text("CREATE TABLE items (id INTEGER)"))

    assert connector.exists("items") is True
    assert connector.exists("non_existent_table") is False
    connector.disconnect()

## --- Unit Tests (Mocks & Error Handling) ---

def test_connect_failure():
    # Provide a garbage URL to trigger an exception
    bad_connector = DBConnector("bad", 5, "postgresql://invalid_user:pass@localhost/fake", 5, 5, False)
    with pytest.raises(DBConnectorError):
        bad_connector.connect()

def test_read_without_connection(connector):
    with pytest.raises(DBConnectorError) as excinfo:
        connector.read(source="any", query="SELECT 1")
    assert "db connection is closed" in str(excinfo.value)

def test_read_missing_query(connector):
    connector.connect()
    with pytest.raises(DBConnectorError) as excinfo:
        connector.read(source="any")  # Missing query kwarg
    assert "query required" in str(excinfo.value)

@patch("sqlalchemy.engine.base.Connection.execute")
def test_read_exception_handling(mock_execute, connector):
    """Tests if the connector properly wraps SQLAlchemy exceptions."""
    connector.connect()
    mock_execute.side_effect = SQLAlchemyError("DB Down")

    with pytest.raises(DBConnectorError):
        connector.read(source="any", query="SELECT * FROM table")

def test_disconnect(connector):
    connector.connect()
    connector.disconnect()
    assert connector._connected is False
    assert connector._engine is None

print("All assertions passed")
