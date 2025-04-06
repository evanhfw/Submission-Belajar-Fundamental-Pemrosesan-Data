"""Scraping module for Fashion Studio Dicoding website using Scrapy"""

import datetime
import logging

import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess


class FashionStudioDicodingSpider(scrapy.Spider):
    """Scrapy spider for extracting product data from Fashion Studio Dicoding website"""

    name = "fashion_studio_spider"

    custom_settings = {
        "FEEDS": {"output.json": {"format": "json", "overwrite": True}},
        "USER_AGENT": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        ),
    }

    def start_requests(self):
        try:
            yield scrapy.Request(
                url="https://fashion-studio.dicoding.dev/", callback=self.parse
            )
        except Exception as e:
            self.logger.error(f"Error in start_requests: {str(e)}")
            raise

    def parse(self, response, **kwargs):
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
            raise


def extract():
    """Run Scrapy crawler and return collected data as DataFrame"""
    try:
        process = CrawlerProcess()
        process.crawl(FashionStudioDicodingSpider)
        process.start()

        try:
            data = pd.read_json("output.json")
            return data
        except FileNotFoundError:
            logging.error("Output file not found, crawling mungkin gagal")
            return pd.DataFrame()
        except Exception as e:
            logging.error("Error loading data: %s", e)
            return pd.DataFrame()

    except Exception as e:
        logging.critical("Fatal error in crawler: %s", e)
        return pd.DataFrame()
