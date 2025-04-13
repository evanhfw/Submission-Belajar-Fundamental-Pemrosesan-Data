# Fashion Studio Data Pipeline

## Overview

This project implements an ETL (Extract, Transform, Load) pipeline for Fashion Studio product data. It extracts product information from web sources, transforms and cleans the data, and loads it into various destinations including CSV files, JSON files, PostgreSQL database, and Google Sheets.

## Features

- **Web Scraping**: Extracts fashion product data from online sources
- **Data Transformation**: Cleans and processes raw data into a structured format
- **Multi-destination Loading**: Stores processed data in:
  - CSV files
  - JSON files
  - PostgreSQL database
  - Google Sheets
- **Logging**: Comprehensive logging of pipeline operations
- **Automated Testing**: Unit tests with coverage reporting

## Project Structure

```
.
├── .env/                # Virtual environment (not tracked by git)
├── .git/                # Git repository metadata
├── logs/                # Log files (created on execution)
├── tests/               # Unit tests
├── utils/               # Utility modules
│   ├── __init__.py      # Package initialization
│   ├── extract.py       # Data extraction module
│   ├── transform.py     # Data transformation module
│   └── load.py          # Data loading module
├── .gitignore           # Git ignore configuration
├── google-sheets-api.json # Google API credentials
├── main.py              # Main application entry point
├── output.json          # Sample output data (JSON format)
├── products.csv         # Sample output data (CSV format)
├── README.md            # Project documentation
├── requirements.txt     # Project dependencies
├── scrapped_data.csv    # Raw scraped data
└── submission.txt       # Instructions for running the project
```

## Prerequisites

- Python 3.6 or higher
- PostgreSQL database server
- Google API credentials

## Installation

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd fashion-studio-pipeline
   ```

2. Create and activate a virtual environment:

   ```bash
   python -m venv .env
   source .env/bin/activate  # On Windows, use: .env\Scripts\activate
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

4. Configure your PostgreSQL database (default configuration expects a database named `productsdb` with username/password as `developer/developer` running on localhost port 5432).

5. Place your Google API credentials in the `google-sheets-api.json` file.

## Usage

### Running the Pipeline

Execute the main ETL pipeline script:

```bash
python main.py
```

### Running Tests

Run the unit tests:

```bash
python -m pytest tests
```

Run test coverage reports:

```bash
coverage run -m pytest tests
coverage report
```

## Pipeline Process

1. **Extraction**: Data is scraped from fashion product sources
2. **Transformation**: Raw data is cleaned, structured, and validated
3. **Loading**: Processed data is loaded to multiple destinations:
   - CSV file
   - JSON file
   - PostgreSQL database
   - Google Sheets

## Log Files

Log files are automatically created in the `logs/` directory with timestamps in the format `pipeline_YYYYMMDD_HHMMSS.log`.

## Development

To contribute to this project:

1. Create a new branch for your feature
2. Implement your changes
3. Add tests for your functionality
4. Submit a pull request
