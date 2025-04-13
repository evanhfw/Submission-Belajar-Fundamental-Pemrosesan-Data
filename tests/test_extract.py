"""
Unit tests for the extract module that handles scraping data from Fashion Studio website.

This module contains test cases for the FashionStudioDicodingSpider class and extract function,
covering normal operations and various error handling scenarios.
"""

import unittest
from unittest.mock import patch, MagicMock
import datetime

import pandas as pd

from utils.extract import FashionStudioDicodingSpider, extract


class TestFashionStudioDicodingSpider(unittest.TestCase):
    """Test cases for the FashionStudioDicodingSpider class."""

    def setUp(self):
        """Set up test fixtures."""
        self.spider = FashionStudioDicodingSpider()
        # Don't try to set logger directly as it's a property with no setter
        self.logger_patcher = patch("scrapy.Spider.logger", new_callable=MagicMock)
        self.mock_logger = self.logger_patcher.start()
        # Make the mock logger accessible to all test methods
        self.spider_logger_patcher = patch.object(
            self.spider.__class__, "logger", self.mock_logger
        )
        self.spider_logger_patcher.start()

    def tearDown(self):
        """Clean up after tests."""
        self.logger_patcher.stop()
        self.spider_logger_patcher.stop()

    def test_start_requests(self):
        """Test the start_requests method generates the expected request."""
        requests = list(self.spider.start_requests())
        self.assertEqual(len(requests), 1)
        self.assertEqual(requests[0].url, "https://fashion-studio.dicoding.dev/")
        self.assertEqual(requests[0].callback, self.spider.parse)

    @patch("scrapy.Request")
    def test_start_requests_error(self, mock_request):
        """Test error handling in start_requests method."""
        mock_request.side_effect = Exception("Network error")

        with self.assertRaises(Exception):
            list(self.spider.start_requests())
        self.mock_logger.error.assert_called_once()

    def test_parse_valid_response(self):
        """Test parsing a valid response with product data."""
        # Create mock for a single product card
        collection_card = MagicMock()

        # Set up the mock CSS selectors for each element within the card
        title_selector = MagicMock()
        title_selector.get.return_value = "Test Product"

        price_selector = MagicMock()
        price_selector.get.return_value = "$99.99"

        rating_selector = MagicMock()
        rating_selector.get.return_value = "4.5 stars"

        colors_selector = MagicMock()
        colors_selector.get.return_value = "Red, Blue, Green"

        size_selector = MagicMock()
        size_selector.get.return_value = "S, M, L"

        gender_selector = MagicMock()
        gender_selector.get.return_value = "Men"

        # Set up the collection_card.css() method to return the appropriate selectors
        collection_card.css.side_effect = lambda selector: {
            "h3.product-title::text": title_selector,
            "span.price::text": price_selector,
            "div.product-details > p:nth-child(3)::text": rating_selector,
            "div.product-details > p:nth-child(4)::text": colors_selector,
            "div.product-details > p:nth-child(5)::text": size_selector,
            "div.product-details > p:nth-child(6)::text": gender_selector,
        }[selector]

        # Create the next page selector
        next_page_selector = MagicMock()
        next_page_selector.get.return_value = "/page/2"

        # Set up the response with the collection cards and next page link
        mock_response = MagicMock()
        mock_response.css.side_effect = lambda selector: {
            "div.collection-card": [collection_card],
            "li.page-item.next > a.page-link::attr(href)": next_page_selector,
        }[selector]

        mock_response.follow.return_value = "followed request"

        # Call the parse method
        results = list(self.spider.parse(mock_response))

        # Verify parsed product data
        self.assertEqual(len(results), 2)  # One product item and one follow request
        self.assertEqual(results[0]["Title"], "Test Product")
        self.assertEqual(results[0]["Price"], "$99.99")
        self.assertEqual(results[0]["Rating"], "4.5 stars")
        self.assertEqual(results[0]["Colors"], "Red, Blue, Green")
        self.assertEqual(results[0]["Size"], "S, M, L")
        self.assertEqual(results[0]["Gender"], "Men")
        self.assertIsInstance(results[0]["Timestamp"], datetime.datetime)

        # Verify next page is followed
        self.assertEqual(results[1], "followed request")
        mock_response.follow.assert_called_once_with(
            "/page/2", callback=self.spider.parse
        )

    def test_parse_empty_response(self):
        """Test parsing an empty response with no products."""
        mock_response = MagicMock()
        mock_response.css.side_effect = lambda selector: {
            "div.collection-card": [],
            "li.page-item.next > a.page-link::attr(href)": MagicMock(get=lambda: None),
        }[selector]

        results = list(self.spider.parse(mock_response))

        self.assertEqual(len(results), 0)  # No products, no next page

    def test_parse_partial_data(self):
        """Test parsing product with missing fields."""
        mock_response = MagicMock()
        collection_card_mock = MagicMock()
        collection_card_mock.css.side_effect = lambda selector: {
            "h3.product-title::text": MagicMock(get=lambda: "Test Product"),
            "span.price::text": MagicMock(get=lambda: "$99.99"),
            "div.product-details > p:nth-child(3)::text": MagicMock(
                get=lambda: None
            ),  # Missing rating
            "div.product-details > p:nth-child(4)::text": MagicMock(get=lambda: "Red"),
            "div.product-details > p:nth-child(5)::text": MagicMock(get=lambda: "M"),
            "div.product-details > p:nth-child(6)::text": MagicMock(
                get=lambda: None
            ),  # Missing gender
        }[selector]

        mock_response.css.side_effect = lambda selector: {
            "div.collection-card": [collection_card_mock],
            "li.page-item.next > a.page-link::attr(href)": MagicMock(get=lambda: None),
        }[selector]

        results = list(self.spider.parse(mock_response))

        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["Title"], "Test Product")
        self.assertEqual(results[0]["Price"], "$99.99")
        self.assertIsNone(results[0]["Rating"])
        self.assertEqual(results[0]["Colors"], "Red")
        self.assertEqual(results[0]["Size"], "M")
        self.assertIsNone(results[0]["Gender"])

    def test_parse_product_error(self):
        """Test error handling when processing a product card fails."""
        mock_response = MagicMock()
        collection_card_mock = MagicMock()
        collection_card_mock.css.side_effect = Exception("Parsing error")

        mock_response.css.side_effect = lambda selector: {
            "div.collection-card": [collection_card_mock],
            "li.page-item.next > a.page-link::attr(href)": MagicMock(get=lambda: None),
        }[selector]

        results = list(self.spider.parse(mock_response))

        self.assertEqual(len(results), 0)  # No results due to error
        self.mock_logger.error.assert_called_once()

    def test_parse_general_error(self):
        """Test error handling when the entire parse function fails."""
        mock_response = MagicMock()
        mock_response.css.side_effect = Exception("Critical error")

        with self.assertRaises(Exception):
            list(self.spider.parse(mock_response))

        self.mock_logger.error.assert_called_once()


class TestExtractFunction(unittest.TestCase):
    """Test cases for the extract function."""

    @patch("utils.extract.CrawlerProcess")
    @patch("utils.extract.pd.read_json")
    def test_extract_success(self, mock_read_json, mock_crawler_process):
        """Test successful data extraction."""
        # Prepare mocks
        mock_process = MagicMock()
        mock_crawler_process.return_value = mock_process

        expected_data = pd.DataFrame(
            {
                "Title": ["Product 1", "Product 2"],
                "Price": ["$10.99", "$20.99"],
                "Rating": ["4.5 stars", "3.5 stars"],
                "Colors": ["Red, Blue", "Green, Yellow"],
                "Size": ["S, M, L", "M, L, XL"],
                "Gender": ["Women", "Men"],
                "Timestamp": [datetime.datetime.now(), datetime.datetime.now()],
            }
        )
        mock_read_json.return_value = expected_data

        # Call function
        result = extract()

        # Verify results
        mock_process.crawl.assert_called_once_with(FashionStudioDicodingSpider)
        mock_process.start.assert_called_once()
        mock_read_json.assert_called_once_with("output.json")
        pd.testing.assert_frame_equal(result, expected_data)

    @patch("utils.extract.CrawlerProcess")
    @patch("utils.extract.pd.read_json")
    @patch("utils.extract.logging")
    def test_extract_file_not_found(
        self, mock_logging, mock_read_json, mock_crawler_process
    ):
        """Test handling of FileNotFoundError."""
        # Prepare mocks
        mock_process = MagicMock()
        mock_crawler_process.return_value = mock_process
        mock_read_json.side_effect = FileNotFoundError("File not found")

        # Call function
        result = extract()

        # Verify results
        mock_process.crawl.assert_called_once_with(FashionStudioDicodingSpider)
        mock_process.start.assert_called_once()
        mock_read_json.assert_called_once_with("output.json")
        mock_logging.error.assert_called_once()
        self.assertTrue(result.empty)

    @patch("utils.extract.CrawlerProcess")
    @patch("utils.extract.pd.read_json")
    @patch("utils.extract.logging")
    def test_extract_read_error(
        self, mock_logging, mock_read_json, mock_crawler_process
    ):
        """Test handling of general error when reading data."""
        # Prepare mocks
        mock_process = MagicMock()
        mock_crawler_process.return_value = mock_process
        mock_read_json.side_effect = Exception("Invalid JSON")

        # Call function
        result = extract()

        # Verify results
        mock_process.crawl.assert_called_once_with(FashionStudioDicodingSpider)
        mock_process.start.assert_called_once()
        mock_read_json.assert_called_once_with("output.json")
        mock_logging.error.assert_called_once()
        self.assertTrue(result.empty)

    @patch("utils.extract.CrawlerProcess")
    @patch("utils.extract.logging")
    def test_extract_crawler_error(self, mock_logging, mock_crawler_process):
        """Test handling of error in crawler process."""
        # Prepare mocks
        mock_crawler_process.side_effect = Exception("Crawler error")

        # Call function
        result = extract()

        # Verify results
        mock_logging.critical.assert_called_once()
        self.assertTrue(result.empty)

    @patch("utils.extract.CrawlerProcess")
    @patch("utils.extract.pd.read_json")
    def test_extract_empty_data(self, mock_read_json, mock_crawler_process):
        """Test handling of empty data."""
        # Prepare mocks
        mock_process = MagicMock()
        mock_crawler_process.return_value = mock_process
        mock_read_json.return_value = pd.DataFrame()  # Empty dataframe

        # Call function
        result = extract()

        # Verify results
        mock_process.crawl.assert_called_once_with(FashionStudioDicodingSpider)
        mock_process.start.assert_called_once()
        mock_read_json.assert_called_once_with("output.json")
        self.assertTrue(result.empty)


if __name__ == "__main__":
    unittest.main()
