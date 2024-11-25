import pytest
import pandas as pd
from datetime import date

from pipeline.etl import SimpleTransformer
from tests.data import mock_data_df, mock_data_renamed_cols_df, mock_invalid_data


class TestSimpleTransformer:
    def setup_method(self):
        """Setup method that runs before each test"""
        self.transformer = SimpleTransformer()


    def test_rename_columns(self):
        df = self.transformer._rename_columns(mock_data_df)
        expected_columns = list(self.transformer.rename_schema.values())
        assert list(df.columns) == expected_columns, f"Expected columns {expected_columns}, but got {list(df.columns)}"


    def test_transformer_handles_invalid_dates(self):
        mock_data_copy = mock_data_renamed_cols_df
        mock_data_copy.loc[0, 'application_end_date'] = '05-12-2024'
        df = self.transformer._transform_dates(mock_data_copy)
        assert pd.isnull(df["application_end_date"].iloc[0]), "Invalid dates should be converted to None"


    def test_transform_dates_handles_null(self):
        input_df = pd.DataFrame([{
            'application_end_date': None
        }])
        df = self.transformer._transform_dates(input_df)

        assert pd.isna(df['application_end_date'].iloc[0])


    @pytest.mark.parametrize("date_str", [
        '2024-12-05',    # Standard format
        '2024-12-5',     # Single digit day
        '2024-1-05',     # Single digit month
        '2024-01-01',    # All double digits
    ])
    def test_transform_dates_various_formats(self, date_str):
        input_df = pd.DataFrame([{
            'application_end_date': date_str
        }])
        df = self.transformer._transform_dates(input_df)

        assert isinstance(df['application_end_date'].iloc[0], date)
    

    def test_data_validation(self):
        df = self.transformer._validate_data(mock_invalid_data)

        # Define schema for validation
        schema = {
            "id": {"type_check": pd.api.types.is_integer_dtype, "nullable": False},
            "field": {"type_check": pd.api.types.is_string_dtype, "nullable": False},
            "job_title": {"type_check": pd.api.types.is_string_dtype, "nullable": False},
            "job_key": {"type_check": pd.api.types.is_string_dtype, "nullable": False},
            "address": {"type_check": pd.api.types.is_string_dtype, "nullable": False},
            "longitude_wgs84": {"type_check": pd.api.types.is_float_dtype, "nullable": False},
            "latitude_wgs84": {"type_check": pd.api.types.is_float_dtype, "nullable": False},
            "application_end_date": {"type_check": pd.api.types.is_string_dtype, "nullable": True},
            "link": {"type_check": pd.api.types.is_string_dtype, "nullable": False},
        }

        # Iterate through the schema and validate each column
        for column, props in schema.items():
            # Check if column exists in the DataFrame
            assert column in df.columns, f"Column '{column}' is missing from DataFrame"

            # Validate column type
            assert props["type_check"](df[column]), f"Column '{column}' does not match expected type"

            # Validate nullability
            if not props["nullable"]:
                assert not df[column].isnull().any(), f"Column '{column}' contains null values"
