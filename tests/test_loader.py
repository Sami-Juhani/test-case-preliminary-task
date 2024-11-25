import pytest
from unittest.mock import patch
from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import IntegrityError
import pandas as pd

from pipeline.etl import SimpleLoader
from pipeline.models import VantaaOpenApplications
from pipeline.const import TABLE_NAME
from tests.data import mock_data_formatted_dates


class TestSimpleLoader:
    def setup_method(self):
        """General setup for tests"""
        self.engine = None
        self.session = None

    def teardown_method(self):
        """Teardown method that runs after each test"""
        if self.session:
            self.session.remove()
        if self.engine:
            self.engine.dispose()

    def _setup_loader(self, test_db_engine, test_db_connection):
        """Setup SimpleLoader with mocked engine, has to be done
           here since Pytest does not inject fixture arguments into 
           setup_method"""
        self.engine = test_db_engine

        # Mock the engine creation inside SimpleLoader
        with patch('pipeline.etl.create_engine') as mock_create_engine:
            mock_create_engine.return_value = self.engine
            self.loader = SimpleLoader(test_db_connection)

        self.inspector = inspect(self.engine)
        self.SessionFactory = sessionmaker(bind=self.engine)
        self.session = scoped_session(self.SessionFactory)


    def test_loader_creates_table(self, test_db_engine, test_db_connection):
        self._setup_loader(test_db_engine, test_db_connection)
        assert TABLE_NAME in self.inspector.get_table_names()


    def test_table_schema(self, test_db_engine, test_db_connection):
        self._setup_loader(test_db_engine, test_db_connection)
        columns = {col['name']: col for col in self.inspector.get_columns(TABLE_NAME)}
        assert columns['id']['primary_key']
        assert not columns['field']['nullable']
        assert not columns['job_title']['nullable']
        assert not columns['job_key']['nullable']
        assert not columns['address']['nullable']
        assert not columns['longitude_wgs84']['nullable']
        assert not columns['latitude_wgs84']['nullable']
        assert columns['application_end_date']['nullable']
        assert not columns['link']['nullable']


    def test_db_loading(self, test_db_engine, test_db_connection):
        self._setup_loader(test_db_engine, test_db_connection)
        # Load mock data
        self.loader.load(mock_data_formatted_dates)
        
        # Verify the data is loaded into the table
        result = self.session.query(VantaaOpenApplications).all()
        assert len(result) == len(mock_data_formatted_dates)

    
    def test_handles_duplicate_entries(self, test_db_engine, test_db_connection):
        self._setup_loader(test_db_engine, test_db_connection)
        self.loader.load(mock_data_formatted_dates)

        try:
            # Attempt to load duplicate entries
            self.loader.load(mock_data_formatted_dates)
        except IntegrityError:
            assert False, "IntegrityError was raised when loading duplicate entries"


    def test_empty_dataframe(self, test_db_engine, test_db_connection):
        self._setup_loader(test_db_engine, test_db_connection)
        # Attempt to load an empty DataFrame
        empty_df = pd.DataFrame()
        with pytest.raises(ValueError):
            self.loader.load(empty_df)
