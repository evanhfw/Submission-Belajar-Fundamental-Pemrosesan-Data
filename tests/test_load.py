"""
Unit tests for the load module that handles data loading to various destinations.

This module contains test cases for CSV, Google Sheets, and PostgreSQL loading functions,
covering normal operations and edge cases.
"""

import unittest
import os
import tempfile
from unittest.mock import patch, MagicMock, ANY
import datetime

import pandas as pd
import numpy as np
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
from sqlalchemy.exc import OperationalError, ProgrammingError
from requests.exceptions import ConnectionError as RequestsConnectionError
from utils.load import (
    load_to_csv,
    load_to_google_sheets,
    load_to_postgres,
    load,
    SERVICE_ACCOUNT_FILE,
)


class TestLoadToCSV(unittest.TestCase):
    """Test cases for the load_to_csv function."""

    def setUp(self):
        """Set up test data."""
        # Create sample data
        self.data = pd.DataFrame(
            {
                "Title": ["Product 1", "Product 2"],
                "Price": ["$10.99", "$20.99"],
                "Rating": ["4.5", "3.5"],
                "Colors": ["Red, Blue", "Green, Yellow"],
                "Size": ["S, M, L", "M, L, XL"],
                "Gender": ["Women", "Men"],
                "Timestamp": [datetime.datetime.now(), datetime.datetime.now()],
            }
        )

        # Create temporary directory for file operations
        self.temp_dir = tempfile.TemporaryDirectory()
        self.csv_path = os.path.join(self.temp_dir.name, "test_products.csv")

    def tearDown(self):
        """Clean up after tests."""
        self.temp_dir.cleanup()

    def test_successful_csv_save(self):
        """Test successful CSV file creation."""
        result = load_to_csv(self.data, self.csv_path)

        self.assertTrue(result)
        self.assertTrue(os.path.exists(self.csv_path))

        # Verify content
        loaded_data = pd.read_csv(self.csv_path)
        self.assertEqual(len(loaded_data), 2)
        self.assertEqual(loaded_data["Title"][0], "Product 1")

    @patch("pandas.DataFrame.to_csv")
    def test_csv_write_permission_error(self, mock_to_csv):
        """Test handling of permission error when writing CSV."""
        mock_to_csv.side_effect = PermissionError("Permission denied")

        result = load_to_csv(self.data, "/root/unauthorized/file.csv")

        self.assertFalse(result)
        mock_to_csv.assert_called_once()

    @patch("pandas.DataFrame.to_csv")
    def test_csv_disk_full_error(self, mock_to_csv):
        """Test handling of disk full error."""
        mock_to_csv.side_effect = OSError("No space left on device")

        result = load_to_csv(self.data, self.csv_path)

        self.assertFalse(result)
        mock_to_csv.assert_called_once()

    def test_empty_dataframe(self):
        """Test saving an empty DataFrame."""
        # Create empty DataFrame with columns to avoid EmptyDataError
        empty_df = pd.DataFrame(columns=["Title", "Price", "Rating", "Gender"])

        result = load_to_csv(empty_df, self.csv_path)

        self.assertTrue(result)
        self.assertTrue(os.path.exists(self.csv_path))

        # Verify that file exists with headers but no data
        loaded_data = pd.read_csv(self.csv_path)
        self.assertEqual(len(loaded_data), 0)

    def test_dataframe_with_special_characters(self):
        """Test saving DataFrame with special characters."""
        special_df = pd.DataFrame(
            {
                "Title": ['Product With "Quotes"', "Product With, Comma"],
                "Description": ["Line 1\nLine 2", "Tab\tCharacter"],
                "Unicode": ["❤️ Emoji", "中文 Chinese"],
            }
        )

        result = load_to_csv(special_df, self.csv_path)

        self.assertTrue(result)

        # Verify content is preserved
        loaded_data = pd.read_csv(self.csv_path)
        self.assertEqual(loaded_data["Title"][0], 'Product With "Quotes"')
        self.assertEqual(loaded_data["Unicode"][1], "中文 Chinese")

    def test_large_dataframe(self):
        """Test saving a large DataFrame."""
        # Create dataframe with 1000 rows and 10 columns
        large_df = pd.DataFrame(
            np.random.rand(1000, 10), columns=[f"Column_{i}" for i in range(10)]
        )

        result = load_to_csv(large_df, self.csv_path)

        self.assertTrue(result)
        self.assertTrue(os.path.exists(self.csv_path))

        # Verify row count
        loaded_data = pd.read_csv(self.csv_path)
        self.assertEqual(len(loaded_data), 1000)


class TestLoadToGoogleSheets(unittest.TestCase):
    """Test cases for the load_to_google_sheets function."""

    def setUp(self):
        """Set up test data."""
        self.data = pd.DataFrame(
            {
                "Title": ["Product 1", "Product 2"],
                "Price": ["$10.99", "$20.99"],
                "Rating": ["4.5", "3.5"],
                "Colors": ["Red, Blue", "Green, Yellow"],
                "Size": ["S, M, L", "M, L, XL"],
                "Gender": ["Women", "Men"],
                "Timestamp": [datetime.datetime.now(), datetime.datetime.now()],
            }
        )

        self.mock_creds = MagicMock(spec=Credentials)
        self.mock_service = MagicMock()
        self.mock_sheets = MagicMock()
        self.mock_values = MagicMock()
        self.mock_update = MagicMock()
        self.mock_execute = MagicMock()

        self.mock_service.spreadsheets.return_value = self.mock_sheets
        self.mock_sheets.values.return_value = self.mock_values
        self.mock_values.update.return_value = self.mock_update
        self.mock_update.execute.return_value = {"updatedCells": 14}

    @patch("utils.load.Credentials.from_service_account_file")
    @patch("utils.load.build")
    def test_successful_upload(self, mock_build, mock_creds):
        """Test successful upload to Google Sheets."""
        mock_creds.return_value = self.mock_creds
        mock_build.return_value = self.mock_service

        result = load_to_google_sheets(self.data)

        self.assertIsNotNone(result)
        self.assertEqual(result["updatedCells"], 14)
        mock_creds.assert_called_once_with(SERVICE_ACCOUNT_FILE, scopes=ANY)
        mock_build.assert_called_once_with("sheets", "v4", credentials=self.mock_creds)

    @patch("utils.load.Credentials.from_service_account_file")
    def test_missing_service_account_file(self, mock_creds):
        """Test handling of missing service account file."""
        mock_creds.side_effect = FileNotFoundError("Service account file not found")

        with self.assertRaises(FileNotFoundError):
            load_to_google_sheets(self.data)

        mock_creds.assert_called_once()

    @patch("utils.load.Credentials.from_service_account_file")
    @patch("utils.load.build")
    def test_http_error(self, mock_build, mock_creds):
        """Test handling of HTTP error."""
        mock_creds.return_value = self.mock_creds
        mock_build.return_value = self.mock_service

        # Configure mock to raise HttpError
        http_error = HttpError(
            resp=MagicMock(), content=b'{"error": "API rate limit exceeded"}'
        )
        self.mock_update.execute.side_effect = http_error

        result = load_to_google_sheets(self.data)

        self.assertIsNone(result)
        mock_creds.assert_called_once()
        mock_build.assert_called_once()

    @patch("utils.load.Credentials.from_service_account_file")
    @patch("utils.load.build")
    def test_connection_error(self, mock_build, mock_creds):
        """Test handling of connection error."""
        mock_creds.return_value = self.mock_creds
        mock_build.return_value = self.mock_service

        # Configure mock to raise connection error
        self.mock_update.execute.side_effect = RequestsConnectionError(
            "Connection timed out"
        )

        result = load_to_google_sheets(self.data)

        self.assertIsNone(result)
        mock_creds.assert_called_once()
        mock_build.assert_called_once()

    @patch("utils.load.Credentials.from_service_account_file")
    @patch("utils.load.build")
    def test_data_with_non_serializable_types(self, mock_build, mock_creds):
        """Test handling of data with non-serializable types."""
        complex_data = pd.DataFrame(
            {
                "Title": ["Product 1", "Product 2"],
                "Price": [10.99, 20.99],  # Numbers instead of strings
                "InStock": [True, False],  # Boolean values
                "LastUpdated": [
                    datetime.datetime.now(),
                    datetime.datetime.now(),
                ],  # Datetime objects
                "Inventory": [
                    {"Red": 5, "Blue": 10},
                    {"Green": 7},
                ],  # Dictionary values
            }
        )

        mock_creds.return_value = self.mock_creds
        mock_build.return_value = self.mock_service

        result = load_to_google_sheets(complex_data)

        self.assertIsNotNone(result)
        # Verify all values were converted to strings
        values_passed = self.mock_values.update.call_args[1]["body"]["values"]
        for row in values_passed:
            for cell in row:
                self.assertIsInstance(cell, str)

    @patch("utils.load.Credentials.from_service_account_file")
    @patch("utils.load.build")
    def test_empty_dataframe(self, mock_build, mock_creds):
        """Test upload of empty DataFrame."""
        empty_df = pd.DataFrame()

        mock_creds.return_value = self.mock_creds
        mock_build.return_value = self.mock_service

        result = load_to_google_sheets(empty_df)

        self.assertIsNotNone(result)
        # Verify empty values list was passed
        self.mock_values.update.assert_called_once()
        body = self.mock_values.update.call_args[1]["body"]
        self.assertEqual(body["values"], [])


class TestLoadToPostgres(unittest.TestCase):
    """Test cases for the load_to_postgres function."""

    def setUp(self):
        """Set up test data."""
        self.data = pd.DataFrame(
            {
                "Title": ["Product 1", "Product 2"],
                "Price": ["$10.99", "$20.99"],
                "Rating": ["4.5", "3.5"],
                "Colors": ["Red, Blue", "Green, Yellow"],
                "Size": ["S, M, L", "M, L, XL"],
                "Gender": ["Women", "Men"],
                "Timestamp": [datetime.datetime.now(), datetime.datetime.now()],
            }
        )

        self.db_url = "postgresql://user:password@localhost:5432/testdb"

        # Set up mock objects
        self.mock_engine = MagicMock()
        self.mock_conn = MagicMock()
        self.mock_engine.connect.return_value.__enter__.return_value = self.mock_conn

    @patch("utils.load.create_engine")
    @patch("pandas.DataFrame.to_sql")
    def test_successful_load(self, mock_to_sql, mock_create_engine):
        """Test successful load to PostgreSQL."""
        mock_create_engine.return_value = self.mock_engine

        result = load_to_postgres(self.data, self.db_url)

        self.assertTrue(result)
        mock_create_engine.assert_called_once_with(self.db_url)
        mock_to_sql.assert_called_once_with(
            "fashion_products", con=self.mock_conn, if_exists="append", index=False
        )

    @patch("utils.load.create_engine")
    def test_connection_error(self, mock_create_engine):
        """Test handling of database connection error."""
        mock_create_engine.side_effect = OperationalError(
            "connection", "Connection refused", None
        )

        result = load_to_postgres(self.data, self.db_url)

        self.assertFalse(result)
        mock_create_engine.assert_called_once()

    @patch("utils.load.create_engine")
    @patch("pandas.DataFrame.to_sql")
    def test_sql_syntax_error(self, mock_to_sql, mock_create_engine):
        """Test handling of SQL syntax error."""
        mock_create_engine.return_value = self.mock_engine
        mock_to_sql.side_effect = ProgrammingError(
            "syntax", "Syntax error in SQL statement", None
        )

        result = load_to_postgres(self.data, self.db_url)

        self.assertFalse(result)
        mock_create_engine.assert_called_once()

    @patch("utils.load.create_engine")
    @patch("pandas.DataFrame.to_sql")
    def test_table_does_not_exist(self, mock_to_sql, mock_create_engine):
        """Test handling of table not existing."""
        mock_create_engine.return_value = self.mock_engine
        mock_to_sql.side_effect = ProgrammingError(
            "table", "Table 'fashion_products' does not exist", None
        )

        result = load_to_postgres(self.data, self.db_url)

        self.assertFalse(result)
        mock_create_engine.assert_called_once()

    @patch("utils.load.create_engine")
    @patch("pandas.DataFrame.to_sql")
    def test_data_with_null_values(self, mock_to_sql, mock_create_engine):
        """Test handling of data with NULL values."""
        data_with_nulls = pd.DataFrame(
            {
                "Title": ["Product 1", None],
                "Price": ["$10.99", "$20.99"],
                "Rating": [None, "3.5"],
                "Gender": ["Women", "Men"],
            }
        )

        mock_create_engine.return_value = self.mock_engine

        result = load_to_postgres(data_with_nulls, self.db_url)

        self.assertTrue(result)
        mock_create_engine.assert_called_once()
        mock_to_sql.assert_called_once()

    @patch("utils.load.create_engine")
    @patch("pandas.DataFrame.to_sql")
    def test_data_with_sql_injection(self, mock_to_sql, mock_create_engine):
        """Test handling of potential SQL injection attempts."""
        data_with_injection = pd.DataFrame(
            {
                "Title": ["Product 1; DROP TABLE fashion_products;--", "Product 2"],
                "Price": ["$10.99", "$20.99"],
            }
        )

        mock_create_engine.return_value = self.mock_engine

        result = load_to_postgres(data_with_injection, self.db_url)

        self.assertTrue(result)
        mock_create_engine.assert_called_once()
        mock_to_sql.assert_called_once()
        # SQL injection should be prevented by SQLAlchemy's parameterization


class TestLoad(unittest.TestCase):
    """Test cases for the main load function."""

    def setUp(self):
        """Set up test data."""
        self.data = pd.DataFrame(
            {
                "Title": ["Product 1", "Product 2"],
                "Price": ["$10.99", "$20.99"],
                "Rating": ["4.5", "3.5"],
                "Gender": ["Women", "Men"],
            }
        )

        self.db_url = "postgresql://user:password@localhost:5432/testdb"
        self.csv_path = "test_products.csv"

    @patch("utils.load.load_to_csv")
    @patch("utils.load.load_to_google_sheets")
    @patch("utils.load.load_to_postgres")
    def test_all_loads_successful(self, mock_postgres, mock_sheets, mock_csv):
        """Test when all loading operations succeed."""
        mock_csv.return_value = True
        mock_sheets.return_value = {"updatedCells": 8}
        mock_postgres.return_value = True

        results = load(self.data, self.db_url, self.csv_path)

        self.assertEqual(
            results, {"csv": True, "google_sheets": True, "postgresql": True}
        )
        mock_csv.assert_called_once_with(self.data, self.csv_path)
        mock_sheets.assert_called_once_with(self.data)
        mock_postgres.assert_called_once_with(self.data, self.db_url)

    @patch("utils.load.load_to_csv")
    @patch("utils.load.load_to_google_sheets")
    @patch("utils.load.load_to_postgres")
    def test_mixed_success_and_failure(self, mock_postgres, mock_sheets, mock_csv):
        """Test when some loading operations succeed and others fail."""
        mock_csv.return_value = True
        mock_sheets.return_value = None  # Sheets upload fails
        mock_postgres.return_value = False  # Postgres load fails

        results = load(self.data, self.db_url, self.csv_path)

        self.assertEqual(
            results, {"csv": True, "google_sheets": False, "postgresql": False}
        )
        mock_csv.assert_called_once()
        mock_sheets.assert_called_once()
        mock_postgres.assert_called_once()

    @patch("utils.load.load_to_csv")
    @patch("utils.load.load_to_google_sheets")
    @patch("utils.load.load_to_postgres")
    def test_all_loads_fail(self, mock_postgres, mock_sheets, mock_csv):
        """Test when all loading operations fail."""
        mock_csv.return_value = False
        mock_sheets.return_value = None
        mock_postgres.return_value = False

        results = load(self.data, self.db_url, self.csv_path)

        self.assertEqual(
            results, {"csv": False, "google_sheets": False, "postgresql": False}
        )
        mock_csv.assert_called_once()
        mock_sheets.assert_called_once()
        mock_postgres.assert_called_once()

    @patch("utils.load.load_to_csv")
    @patch("utils.load.load_to_google_sheets")
    @patch("utils.load.load_to_postgres")
    def test_continue_after_one_failure(self, mock_postgres, mock_sheets, mock_csv):
        """Test that the function continues after one load operation fails."""
        # Set load_to_csv to return False instead of raising exception
        # since the actual implementation catches exceptions and returns False
        mock_csv.return_value = False
        mock_sheets.return_value = {"updatedCells": 8}
        mock_postgres.return_value = True

        results = load(self.data, self.db_url, self.csv_path)

        # CSV should fail but other operations continue
        self.assertEqual(results["csv"], False)
        self.assertEqual(results["google_sheets"], True)
        self.assertEqual(results["postgresql"], True)

        mock_csv.assert_called_once()
        mock_sheets.assert_called_once()
        mock_postgres.assert_called_once()

    @patch("utils.load.load_to_csv")
    @patch("utils.load.load_to_google_sheets")
    @patch("utils.load.load_to_postgres")
    def test_with_empty_dataframe(self, mock_postgres, mock_sheets, mock_csv):
        """Test loading an empty DataFrame."""
        empty_df = pd.DataFrame()

        mock_csv.return_value = True
        mock_sheets.return_value = {"updatedCells": 0}
        mock_postgres.return_value = True

        results = load(empty_df, self.db_url, self.csv_path)

        self.assertEqual(
            results, {"csv": True, "google_sheets": True, "postgresql": True}
        )

        # Verify empty DataFrame was passed to all functions
        mock_csv.assert_called_once_with(empty_df, self.csv_path)
        mock_sheets.assert_called_once_with(empty_df)
        mock_postgres.assert_called_once_with(empty_df, self.db_url)


if __name__ == "__main__":
    unittest.main()
