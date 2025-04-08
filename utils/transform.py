"""
Data transformation utilities for cleaning and preparing datasets.

This module provides functions to transform raw dataframes into clean, properly
formatted data ready for analysis or further processing.
"""

import pandas as pd


def transform(raw_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Transform raw data by cleaning and converting columns to appropriate formats.

    This function performs the following operations:
    - Cleans text in Title, Rating, and Price columns
    - Converts Rating to float
    - Converts Price from USD to IDR (multiplying by 16000)
    - Extracts first value from Colors, Size, and Gender columns
    - Converts Timestamp to datetime format

    Args:
        raw_df: Input DataFrame to transform. If None, an empty DataFrame is returned.

    Returns:
        Transformed DataFrame with cleaned data

    Raises:
        ValueError: If input is not a DataFrame
        KeyError: If required columns are missing
    """
    # Input validation
    if raw_df is None:
        return pd.DataFrame()

    if not isinstance(raw_df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")

    required_columns = [
        "Title",
        "Rating",
        "Price",
        "Colors",
        "Size",
        "Gender",
        "Timestamp",
    ]
    missing_columns = [col for col in required_columns if col not in raw_df.columns]
    if missing_columns:
        raise KeyError(f"Missing required columns: {', '.join(missing_columns)}")

    # Create a copy to avoid modifying the original DataFrame
    try:
        df = raw_df.copy()

        # Clean Title Features
        df["Title"] = df["Title"].str.lower()
        df.loc[df["Title"].str.contains("unknown", na=False), "Title"] = pd.NA

        # Clean Rating Features
        df["Rating"] = df["Rating"].str.lower()
        df.loc[df["Rating"].str.contains("invalid", na=False), "Rating"] = pd.NA
        df.loc[df["Rating"].str.contains("not", na=False), "Rating"] = pd.NA

        # Clean Price Features
        df["Price"] = df["Price"].str.lower()
        df.loc[df["Price"].str.contains("unavailable", na=False), "Price"] = pd.NA

        # Drop rows with missing values
        df = df.dropna()

        # Transform columns to appropriate data types
        try:
            df["Rating"] = df["Rating"].str.split(expand=True)[2].astype(float)
        except (IndexError, ValueError) as e:
            raise ValueError(f"Failed to process Rating column: {e}") from e

        try:
            # Convert price from USD to IDR
            df["Price"] = df["Price"].str.replace("$", "").astype(float) * 16000
        except ValueError as e:
            raise ValueError(f"Failed to process Price column: {e}") from e

        # Extract first value from categorical columns
        df["Colors"] = df["Colors"].str.split(expand=True)[0]
        df["Size"] = df["Size"].str.split(expand=True)[1]
        df["Gender"] = df["Gender"].str.split(expand=True)[1]

        # Convert timestamp to datetime
        try:
            df["Timestamp"] = pd.to_datetime(df["Timestamp"])
        except ValueError as e:
            raise ValueError(f"Failed to convert Timestamp to datetime: {e}") from e

        return df

    except Exception as e:
        raise Exception(f"An error occurred during data transformation: {e}") from e
