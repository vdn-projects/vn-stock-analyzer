import os
import time
import glob
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pytz import timezone
import logging
import traceback

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


import psycopg2
import pandas as pd

from vn_stock.tasks.utils import Utils


class VNDirectCrawlTicker(Utils):
    url_ticker = "https://www.vndirect.com.vn/portal/thong-tin-co-phieu/nhap-ma-chung-khoan.shtml?request_locale=en_GB"

    # Insert ticker query
    insert_ticker_query = """
        INSERT INTO ticker(ticker_code, company_name, company_full_name, sector, exchange)
        VALUES(%s, %s, %s, %s, %s)
        ON CONFLICT ON CONSTRAINT ticker_tickercode_pkey
        DO NOTHING
        """

    def __init__(self, conn_str, start_date="01/01/2010"):
        self.conn_str = conn_str
        self.start_date = start_date

    def init_driver(self):
        # Init chrome driver
        options = webdriver.ChromeOptions()
        options.add_argument('--disable-notifications')
        options.add_argument('--no-sandbox')
        options.add_argument('--verbose')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-software-rasterizer')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--headless')

        driver = webdriver.Chrome(
            executable_path='/usr/local/bin/chromedriver', chrome_options=options)

        driver.get(VNDirectCrawlTicker.url_ticker)
        return driver

    def quit_driver(self, driver):
        if driver is not None:
            driver.close()
            driver.quit()

    def execute_etl(self):
        driver = None
        try:
            driver = self.init_driver()
            self.crawl_ticker(driver)
        except Exception as ex:
            logging.error(traceback.print_exc())
        self.quit_driver(driver)

    def refresh_ticker_page(self, driver, max_retries=10):
        """
        The web page is intemittely not load enough resources, 
        This method is to overcome this issue by redo page refresh til max_retries
        """
        retry = 0
        while(retry <= max_retries):
            try:
                driver.refresh()
                elem = driver.find_element_by_css_selector(
                    '#fSearchSymbol_btnSymbolSearch')
                elem.click()
                WebDriverWait(driver, 5, 1).until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, '#fSearchSymbol_result > table > tbody > tr:nth-child(1) > td:nth-child(1) > span')))
                return 1
            except Exception as ex:
                retry += 1
        raise Exception("Cannot load ticker table")

    def click_next_ticker(self, driver, page_no, max_retries=15):
        retry = 0
        while(retry < max_retries):
            try:
                # Check if the paging is availale
                element = WebDriverWait(driver, 5, 1).until(EC.presence_of_element_located(
                    (By.CSS_SELECTOR, '#fSearchSymbol_paging > div')))

                paging_content = element.text.strip()
                logging.info(element.text.strip())
                if(">" in paging_content):
                    # Click next page
                    driver.execute_script(
                        f"javascript:_goTo('fSearchSymbol_paging',{page_no})")
                    time.sleep(2)
                    return True
                else:
                    return False

            except Exception as ex:  # Not found the next button
                if "javascript error:" in str(ex):
                    logging.error("Not found javascript goto next page")
                else:
                    logging.error(traceback.print_exc())

                # resources complete download
                # If the javascript is not found, driver will refresh number of time to have the web
                driver.refresh()
                time.sleep(2)
                retry += 1
                logging.info(
                    f"#{retry} try to click next to page#{page_no}.")
        return False

    def crawl_ticker(self, driver):
        """
        Selenium task to crawl ticker code and its information
        """
        try:
            # Refresh page at first
            self.refresh_ticker_page(driver)

            # Load tickers shown in the first page
            self.load_ticker(driver)

            # Load tickers in the next pages
            page_no = 2
            while self.click_next_ticker(driver, page_no):
                logging.info(f"Loading tickers on page {page_no}.")
                self.load_ticker(driver)
                page_no += 1
            logging.info(
                f"Complete crawling all ticker codes.")
        except Exception as ex:
            logging.error(traceback.print_exc())

    def load_ticker(self, driver):
        # Get ticker table of current selected page
        elem = driver.find_element_by_css_selector(
            "#fSearchSymbol_result")
        ticker_table = elem.get_attribute("innerHTML")

        # with open(f"{config.download_path}/{page_no}.html", "w") as f:
        #     f.write(ticker_table)

        data_dict = {}
        source = BeautifulSoup(ticker_table, "html.parser")

        # Parsing ticker info
        tickers = [x.get_text().strip()
                   for x in source.select("tbody tr td span")]

        data_dict["ticker_code"] = tickers[0::5]
        data_dict["company_name"] = tickers[1::5]
        data_dict["company_full_name"] = tickers[2::5]
        data_dict["sector"] = tickers[3::5]
        data_dict["exchange"] = tickers[4::5]

        df = pd.DataFrame(data_dict)
        # df.to_csv(f"{config.download_path}/{page_no}.csv", index=None)

        with psycopg2.connect(self.conn_str) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    try:
                        cur.execute(
                            VNDirectCrawlTicker.insert_ticker_query, row.to_list())
                    except Exception as ex:
                        logging.error(traceback.format_exc())
