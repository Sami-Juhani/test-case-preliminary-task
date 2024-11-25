import pytest
from unittest.mock import patch
import pandas as pd
from requests.exceptions import HTTPError

from pipeline.etl import SimpleExtractor
from tests.data import mock_data_json


class TestSimpleExtractor:
    def setup_method(self):
        """Setup method that runs before each test"""
        self.extractor = SimpleExtractor()
        self.mock_get_patcher = patch('requests.get')
        self.mock_get = self.mock_get_patcher.start()
    
    def teardown_method(self):
        """Teardown method that runs after each test"""
        self.mock_get_patcher.stop()


    def test_fetch_data(self, mock_response):
        self.mock_get.return_value = mock_response
        response = self.extractor.fetch_data()
        
        self.mock_get.assert_called_once()
        assert response.json() == mock_data_json


    def test_extract_returns_dataframe(self, mock_response):
        self.mock_get.return_value = mock_response
        df = self.extractor()
        
        assert isinstance(df, pd.DataFrame)

    
    def test_extractor_handles_api_errors(self):
        self.mock_get.side_effect = HTTPError()
        
        with pytest.raises(HTTPError):
            self.extractor()
