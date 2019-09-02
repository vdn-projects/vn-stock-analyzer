import os
import time
import glob
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pytz import timezone
import traceback

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

import psycopg2
import pandas as pd


import vn_stock.tasks.utils as utils
import vn_stock.tasks.config as config


class VNDirectCrawlPrice:
    time_zone = "Asia/Saigon"
    date_format = "%d/%m/%Y"

    url_price = "https://www.vndirect.com.vn/portal/thong-ke-thi-truong-chung-khoan/lich-su-gia.shtml?request_locale=en"

    # Insert price query
    insert_price_query = """
    INSERT INTO historical_price(ticker_code, date, open, highest, lowest, close, average, adjusted, trading_volume, put_through_volume)
    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT ON CONSTRAINT historical_price_key 
    DO NOTHING
    """

    def __init__(self, driver, conn_str, logger, start_date="01/01/2010"):
        self.driver = driver
        self.conn_str = conn_str
        self.logger = logger
        self.start_date = start_date

    def init_driver(self):
        # Init chrome driver
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-notifications")
        options.add_argument('--no-sandbox')
        options.add_argument('--verbose')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-software-rasterizer')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--headless')

        driver = webdriver.Chrome(
            executable_path='/usr/local/bin/chromedriver', chrome_options=options)

        driver.get(VNDirectCrawlPrice.url_price)

        return driver

    def quit_driver(self, driver):
        driver.close()
        driver.quit()

    def get_tickers(self):
        with psycopg2.connect(self.conn_str) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                return pd.read_sql_query("SELECT ticker_code FROM ticker", conn)

    def last_update(self, ticker_code):
        """
        Get the last updated time of latest record
        """
        last_updated = self.start_date
        with psycopg2.connect(self.conn_str) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT MAX(date) FROM historical_price WHERE ticker_code = '{ticker_code}'")
                result = cur.fetchall()[0][0]
                if result:
                    last_updated = (result + timedelta(days=1)
                                    ).strftime(VNDirectCrawlPrice.date_format)
        return last_updated

    def execute_etl(self):

        to_date = (datetime.now(timezone(VNDirectCrawlPrice.time_zone)) +
                   timedelta(days=1)).strftime(VNDirectCrawlPrice.date_format)

        # Run selenium to download csv files
        self.logger.info("Read list of ticker from database")

        tickers = self.get_tickers()
        # tickers = pd.DataFrame({"ticker_code": ["AAA", "FPT"]})
        self.logger.info(f"There are {tickers.shape[0]}")

        # Crawl historical price
        for _, ticker in tickers.iterrows():
            try:
                from_date = self.last_update(ticker["ticker_code"])
                self.driver = self.init_driver()
                self.crawl_price(ticker["ticker_code"], from_date, to_date)
            except Exception as ex:
                self.logger.error(traceback.print_exc())

    def crawl_price(self, ticker_code, from_date, to_date):
        try:
            self.logger.info(
                f"Crawling {ticker_code} from {from_date} to {to_date}.")
            self.input_price_params(ticker_code, from_date, to_date)
            self.load_price(ticker_code)

            # Start checking if paging is available, then continue load more data
            page_no = 2
            while self.click_next_price(page_no):
                self.logger.info(f"Load price on page {page_no}.")
                self.load_price(ticker_code)
                page_no += 1

            self.logger.info(
                f"Complete crawling {ticker_code} from {from_date} to {to_date}.")
        except Exception as ex:
            self.logger.error(traceback.print_exc())

    def click_next_price(self, page_no, max_retries=15):
        retry = 0
        while(retry < max_retries):
            try:
                # Check if the paging is availale
                element = WebDriverWait(self.driver, 5, 1).until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, '#tab-1 > div.paging')))

                paging_content = element.text.strip()
                self.logger.info(element.text.strip())
                if(">" in paging_content):
                    # Click next page
                    self.driver.execute_script(
                        f"javascript:_goTo({page_no})")
                    time.sleep(2)
                    return True
                else:
                    return False

            except Exception as ex:  # Not found the next button
                if "javascript error:" in str(ex):
                    self.logger.error("Not found javascript goto next page")
                else:
                    self.logger.error(traceback.print_exc())

                # resources complete download
                # If the javascript is not found, driver will refresh number of time to have the web
                self.driver.refresh()
                time.sleep(2)
                retry += 1
                self.logger.info(
                    f"#{retry} try to click next to page#{page_no}.")
        return False

    def load_price(self, ticker_code):
        elem = self.driver.find_element_by_css_selector(
            "#tab-1 > div.box_content_tktt > ul")
        price_table = elem.get_attribute("innerHTML")

        data_dict = {}
        source = BeautifulSoup(price_table, "html.parser")

        # Parsing date
        days = [datetime.strptime(x.get_text().strip(), "%Y-%m-%d")
                for x in source.select("li div.row-time.noline")[1:]]
        data_dict["ticker_code"] = [ticker_code for x in range(len(days))]
        data_dict["date"] = days

        # Parsing prices
        prices = [(float(utils.replace_comma(x.get_text().strip())) if utils.is_number(utils.remove_comma(x.get_text().strip(
        ))) else x.get_text().strip()) for x in source.select("li div.row1")]
        data_dict["open"] = prices[6::6]
        data_dict["highest"] = prices[7::6]
        data_dict["lowest"] = prices[8::6]
        data_dict["close"] = prices[9::6]
        data_dict["average"] = prices[10::6]
        data_dict["adjusted"] = prices[11::6]

        # Parsing volume
        volumes = [(int(float((x.get_text().strip()))) if utils.is_number(
            (x.get_text().strip())) else None) for x in source.select("li div.row3")[2:]]
        data_dict["trading_volume"] = volumes[0::2]
        data_dict["put_through_volume"] = volumes[1::2]

        df = pd.DataFrame(data_dict)
        df = df.where(df.notna(), None)

        with psycopg2.connect(config.conn_string) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                try:
                    cur.executemany(
                        VNDirectCrawlPrice.insert_price_query, [tuple(x) for x in df.values.tolist()])
                except Exception as ex:
                    self.logger.error(traceback.print_exc())

    def input_price_params(self, ticker_code, from_date, to_date):
        try:
            # Refresh the page again to reload resources
            self.driver.refresh()

            # Input ticker code
            elem = self.driver.find_element_by_css_selector('#symbolID')
            elem.send_keys(ticker_code)

            # Input time from
            elem = self.driver.find_element_by_css_selector(
                '#fHistoricalPrice_FromDate')
            elem.send_keys(from_date)

            # Input time to
            elem = self.driver.find_element_by_css_selector(
                '#fHistoricalPrice_ToDate')
            elem.send_keys(to_date)

            # View historical price list
            elem = self.driver.find_element_by_css_selector(
                '#fHistoricalPrice_View')
            elem.click()

        except Exception as ex:
            self.logger.error(traceback.print_exc())
