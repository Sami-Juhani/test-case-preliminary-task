from sqlalchemy import create_engine
from unittest.mock import Mock
import pytest

from pipeline.models import Base
from tests.data import mock_data_json

DB_FOLDER = "db"
DB_PATH = f"{DB_FOLDER}/reagle_test.db"


@pytest.fixture
def mock_response():
    mock_resp = Mock()
    mock_resp.json.return_value = mock_data_json
    mock_resp.raise_for_status.return_value = None
    return mock_resp


@pytest.fixture
def test_db_engine():
    """Create a test database engine and tables"""
    engine = create_engine(f"sqlite:///{DB_PATH}")
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)


@pytest.fixture
def test_db_connection():
    return "sqlite:///{DB_PATH}"

    
