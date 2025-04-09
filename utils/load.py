"""
Data loading utilities for storing processed data in various destinations.

This module provides functions to load cleaned data into different storage targets:
- CSV files
- Google Sheets
- PostgreSQL database
"""

from typing import Dict, Optional
import logging

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# Google Sheets API constants
SERVICE_ACCOUNT_FILE = "google-sheets-api.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "1OSXrSugb5Rsc5vmesFjlQ0MXAf-2Fv5Trjm1yBV8LNk"
RANGE_NAME = "Sheet1!A2:G9999"


def load_to_csv(cleaned_data: pd.DataFrame, csv_name: str = "products.csv") -> bool:
    """
    Save cleaned data to a CSV file.

    Args:
        cleaned_data: DataFrame containing processed data
        csv_name: Destination CSV filename

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        cleaned_data.to_csv(csv_name, index=False)
        return True
    except Exception as e:
        logging.error("Error saving to CSV: %s", e)
        return False


def load_to_google_sheets(cleaned_data: pd.DataFrame) -> Optional[Dict]:
    """
    Upload cleaned data to Google Sheets.

    Args:
        cleaned_data: DataFrame containing processed data

    Returns:
        Dict containing API response if successful, None otherwise

    Raises:
        FileNotFoundError: If service account file is not found
    """
    try:
        credential = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )

        # Build the service
        service = build("sheets", "v4", credentials=credential)

        # Access the Sheets API
        # Type annotation to fix linter error
        sheet = service.spreadsheets()  # type: ignore

        # Create a copy of the DataFrame to avoid modifying the original
        df_copy = cleaned_data.copy()

        # Convert DataFrame to string values to ensure JSON serialization
        # This handles Timestamp objects and other non-serializable types
        for col in df_copy.columns:
            df_copy[col] = df_copy[col].astype(str)

        # Convert DataFrame to a list of lists (values for Google Sheets)
        # Don't include the header row since it's already in Google Sheets
        values = df_copy.values.tolist()

        body = {"values": values}

        result = (
            sheet.values()
            .update(
                spreadsheetId=SPREADSHEET_ID,
                range=RANGE_NAME,
                valueInputOption="RAW",
                body=body,
            )
            .execute()
        )

        return result
    except FileNotFoundError as e:
        logging.error("Service account file not found: %s", e)
        raise
    except HttpError as e:
        logging.error("Google Sheets API error: %s", e)
        return None
    except Exception as e:
        logging.error("Error uploading to Google Sheets: %s", e)
        return None


def load_to_postgres(cleaned_data: pd.DataFrame, db_url: str) -> bool:
    """
    Load cleaned data to a PostgreSQL database.

    Args:
        cleaned_data: DataFrame containing processed data
        db_url: Database connection URL

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        engine = create_engine(db_url)

        with engine.connect() as con:
            cleaned_data.to_sql(
                "fashion_products", con=con, if_exists="append", index=False
            )
        return True
    except SQLAlchemyError as e:
        logging.error("Database error: %s", e)
        return False
    except Exception as e:
        logging.error("Error loading to PostgreSQL: %s", e)
        return False


def load(
    cleaned_data: pd.DataFrame, db_url: str, csv_path: str = "products.csv"
) -> Dict[str, bool]:
    """
    Load cleaned data to all available destinations.

    This function attempts to load the data to CSV, Google Sheets, and PostgreSQL.
    It returns the status of each operation.

    Args:
        cleaned_data: DataFrame containing processed data
        db_url: Database connection URL
        csv_path: Path to save the CSV file

    Returns:
        Dict with status of each loading operation
    """
    results = {}

    # Load to CSV
    results["csv"] = load_to_csv(cleaned_data, csv_path)

    # Load to Google Sheets
    sheets_result = load_to_google_sheets(cleaned_data)
    results["google_sheets"] = sheets_result is not None

    # Load to PostgreSQL
    results["postgresql"] = load_to_postgres(cleaned_data, db_url)

    return results
