from unittest import mock
from sqlalchemy.orm import sessionmaker

from pipeline import run_etl
from pipeline.models import VantaaOpenApplications
from tests.data import mock_data_df


@mock.patch('pipeline.etl.create_engine')
@mock.patch('pipeline.etl.SimpleExtractor.__call__', return_value=mock_data_df)
def test_etl_end_to_end(_, mock_create_engine, test_db_engine, test_db_connection):
    # Mock create_engine to return the test database engine
    mock_create_engine.return_value = test_db_engine

    # Run the ETL process
    run_etl(conn_str=test_db_connection) 
    
    # Verify the data was loaded into the database
    SessionFactory = sessionmaker(bind=test_db_engine)
    session = SessionFactory()
    result = session.query(VantaaOpenApplications).all()
    assert len(result) == len(mock_data_df), f"Expected {len(mock_data_df)} rows, got {len(result)}"

    