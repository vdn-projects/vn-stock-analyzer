
import datetime
import logging


from tasks.etl_vndirect_price import VNDirectCrawlPrice
from tasks import config
from tasks.utils import Utils

if __name__ == "__main__":
    logger = Utils.get_logger()
    crawl_price = VNDirectCrawlPrice(config.conn_string, logger)
