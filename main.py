"""
Main application module for Fashion Studio data pipeline.

This module orchestrates the ETL (Extract, Transform, Load) process for
Fashion Studio product data, handling the extraction from web, data transformation,
and loading to various storage destinations.
"""

import logging
import sys
from typing import Dict

from utils import extract, load, transform

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Database configuration
DB_URL = "postgresql+psycopg2://developer:developer@localhost:5432/productsdb"


def process_results(result: Dict[str, bool]) -> None:
    """
    Process and display the results of data loading operations.

    Args:
        result: Dictionary containing the status of each loading operation
    """
    print("\nHasil Loading Data:")
    print("-" * 50)

    status_symbols = {True: "✅", False: "❌"}

    for destination, status in result.items():
        symbol = status_symbols[status]
        destination_name = destination.replace("_", " ").title()

        if status:
            print(f"{symbol} {destination_name:15}: Data berhasil disimpan")
        else:
            print(f"{symbol} {destination_name:15}: Gagal menyimpan data")

    # Display summary
    success_count = sum(result.values())
    total_count = len(result)
    success_rate = (success_count / total_count) * 100

    print("\nRingkasan:")
    print(f"Total operasi    : {total_count}")
    print(f"Berhasil        : {success_count}")
    print(f"Gagal           : {total_count - success_count}")
    print(f"Tingkat sukses  : {success_rate:.1f}%")


def main() -> None:
    """
    Main function to execute the ETL pipeline.

    This function orchestrates the following steps:
    1. Extract data from the Fashion Studio website
    2. Transform and clean the extracted data
    3. Load the cleaned data to various destinations
    4. Display the results of the operations
    """
    try:
        logging.info("Starting data extraction...")
        raw_data = extract()

        if raw_data.empty:
            logging.error("No data was extracted. Exiting...")
            sys.exit(1)

        logging.info("Transforming data...")
        cleaned_data = transform(raw_data)

        if cleaned_data.empty:
            logging.error("Data transformation resulted in empty dataset. Exiting...")
            sys.exit(1)

        logging.info("Loading data to destinations...")
        result = load(cleaned_data, db_url=DB_URL)

        process_results(result)

    except Exception as e:
        logging.error("An error occurred during execution: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
