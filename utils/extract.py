"""
Data extraction module for Fashion Studio Dicoding website using Scrapy.

This module provides a Scrapy spider and utility functions to scrape product data
from the Fashion Studio Dicoding website and convert it to a pandas DataFrame.
"""

import datetime
import logging
import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess


class FashionStudioDicodingSpider(scrapy.Spider):
    """
    Scrapy spider for extracting product data from Fashion Studio Dicoding website.

    This spider crawls through product pages and extracts information including:
    - Product title
    - Price
    - Rating
    - Available colors
    - Available sizes
    - Target gender
    - Timestamp of data collection
    """

    name = "fashion_studio_spider"

    custom_settings = {
        "FEEDS": {"output.json": {"format": "json", "overwrite": True}},
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
    }

    def start_requests(self):
        """
        Initiate the crawling process by sending the first request.

        Returns:
            A scrapy.Request object for the target website

        Raises:
            Exception: If the initial request fails
        """
        try:
            yield scrapy.Request(
                url="https://fashion-studio.dicoding.dev/", callback=self.parse
            )
        except Exception as e:
            self.logger.error("Error in start_requests: %s", str(e))
            raise e from e

    def parse(self, response, **kwargs):
        """
        Parse the response and extract product information.

        Args:
            response: HTTP response from scrapy
            **kwargs: Additional keyword arguments

        Yields:
            Dictionary containing product information

        Raises:
            Exception: If parsing fails
        """
        try:
            timestamp = datetime.datetime.now()
            collection_cards = response.css("div.collection-card")

            for collection_card in collection_cards:
                try:
                    title = collection_card.css("h3.product-title::text").get()
                    price = collection_card.css("span.price::text").get()
                    rating = collection_card.css(
                        "div.product-details > p:nth-child(3)::text"
                    ).get()
                    colors = collection_card.css(
                        "div.product-details > p:nth-child(4)::text"
                    ).get()
                    size = collection_card.css(
                        "div.product-details > p:nth-child(5)::text"
                    ).get()
                    gender = collection_card.css(
                        "div.product-details > p:nth-child(6)::text"
                    ).get()

                    yield {
                        "Title": title,
                        "Price": price,
                        "Rating": rating,
                        "Colors": colors,
                        "Size": size,
                        "Gender": gender,
                        "Timestamp": timestamp,
                    }
                except Exception as e:
                    self.logger.error("Error processing product card: %s", e)
                    continue

            next_link = response.css(
                "li.page-item.next > a.page-link::attr(href)"
            ).get()
            if next_link:
                yield response.follow(next_link, callback=self.parse)
        except Exception as e:
            self.logger.error("Error in parse function: %s", e)
            raise e from e


def extract() -> pd.DataFrame:
    """
    Run Scrapy crawler and return collected data as DataFrame.

    This function executes the FashionStudioDicodingSpider to extract data
    and then loads the results from the output file into a pandas DataFrame.

    Returns:
        pd.DataFrame: DataFrame containing the scraped product data.
        Empty DataFrame if extraction or loading fails.
    """
    try:
        process = CrawlerProcess()
        process.crawl(FashionStudioDicodingSpider)
        process.start()

        try:
            data = pd.read_json("output.json")
            return data
        except FileNotFoundError:
            logging.error("Output file not found, crawling may have failed")
            return pd.DataFrame()
        except Exception as e:
            logging.error("Error loading data: %s", e)
            return pd.DataFrame()

    except Exception as e:
        logging.critical("Fatal error in crawler: %s", e)
        return pd.DataFrame()
