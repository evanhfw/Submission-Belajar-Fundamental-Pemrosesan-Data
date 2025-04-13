"""
Unit tests for the transform module that handles data cleaning and transformation.

This module contains test cases for the transform function that integrates with
extraction and loading steps of the ETL pipeline.
"""

import unittest
from datetime import datetime
import pandas as pd

from utils.transform import transform


class TestTransform(unittest.TestCase):
    """Test cases for the transform function."""

    def setUp(self):
        """Set up test fixtures."""
        # Create sample data similar to what extract.py would produce
        self.sample_data = pd.DataFrame(
            {
                "Title": ["Product A", "Product B", "Product C"],
                "Rating": [
                    "-- 4.5 --",
                    "-- 3.0 --",
                    "-- 4.8 --",
                ],  # Format matching transform.py expectation
                "Price": ["$10.99", "$20.50", "$15.75"],
                "Colors": ["Red, Blue", "Green, Yellow", "Black, White"],
                "Size": ["size M L", "size L XL", "size S M"],
                "Gender": ["for Men Women", "for Women Men", "for Men Kids"],
                "Timestamp": [
                    datetime(2023, 5, 15, 10, 30),
                    datetime(2023, 5, 16, 11, 45),
                    datetime(2023, 5, 17, 14, 20),
                ],
            }
        )

    def test_none_input(self):
        """Test handling of None input."""
        result = transform(None)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertTrue(result.empty)

    def test_invalid_input_type(self):
        """Test error when input is not a DataFrame."""
        invalid_inputs = [
            "not a dataframe",
            123,
            ["list", "of", "items"],
            {"key": "value"},
        ]

        for invalid_input in invalid_inputs:
            with self.assertRaises(ValueError) as context:
                transform(invalid_input)
            self.assertIn("Input must be a pandas DataFrame", str(context.exception))


if __name__ == "__main__":
    unittest.main()
