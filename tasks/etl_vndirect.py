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

import tasks.config as config
import tasks.sql_queries as sql_queries


def is_number(s):
    """ Returns True is string is a number. """
    return s.replace('.', '', 1).isdigit()


def get_logger(log_path, max_bytes=1000):
    pass


def delete_files(path, wildcard):
    for file in glob.glob(os.path.join(path, wildcard)):
        os.remove(file)


def initialize():
    # Init chrome driver
    # url = "https://www.vndirect.com.vn/portal/thong-ke-thi-truong-chung-khoan/lich-su-gia.shtml?request_locale=en"
    url = "https://www.vndirect.com.vn/portal/thong-tin-co-phieu/nhap-ma-chung-khoan.shtml?request_locale=en_GB"
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

    driver.get(url)

    return driver


def refresh_ticker_page(driver, max_retries=10):
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


def end_page(driver):
    try:
        WebDriverWait(driver, 5, 1).until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, '#fSearchSymbol_paging > div > span.next > a')))
        return False
    except Exception as ex:  # Not found the next button
        return True


def click_next_page(driver, page_no, logger):
    driver.execute_script(
        f"javascript:_goTo('fSearchSymbol_paging',{page_no})")
    time.sleep(2)


def crawl_ticker(driver, logger):
    """
    Selenium task to crawl ticker code and its information
    """
    count = 1
    refresh_ticker_page(driver)
    try:
        # First click
        while not end_page(driver):
            # Get ticker table of current selected page
            elem = driver.find_element_by_css_selector(
                "#fSearchSymbol_result")
            ticker_table = elem.get_attribute("innerHTML")

            # with open(f"{config.download_path}/{count}.html", "w") as f:
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
            df.to_csv(f"{config.download_path}/{count}.csv", index=None)

            with psycopg2.connect(config.conn_string) as conn:
                conn.set_session(autocommit=True)
                with conn.cursor() as cur:
                    for _, row in df.iterrows():
                        try:
                            cur.execute(
                                sql_queries.upsert_ticker_table, row.to_list())
                        except Exception as ex:
                            logger.error(row["ticker_code"] + " | " +
                                         traceback.format_exc())

            logger.info(f"Page {count} completed.")
            count += 1
            click_next_page(driver, count, logger)
    except Exception as ex:
        logger.error(f"Page{count} | " + traceback.format_exc())


def process(driver, ticker_code, from_date, to_date, logger):
    """
    Selenium task to down load the price list
    """

    # Input ticker code
    elem = driver.find_element_by_css_selector('#symbolID')
    elem.send_keys(ticker_code)

    # Input time from
    elem = driver.find_element_by_css_selector('#fHistoricalPrice_FromDate')
    elem.send_keys(from_date)

    # Input time to
    elem = driver.find_element_by_css_selector('#fHistoricalPrice_ToDate')
    elem.send_keys(to_date)

    try:
        # View historical price list
        elem = driver.find_element_by_css_selector('#fHistoricalPrice_View')
        elem.click()

        # Wait until the table appear, over 10 seconds it will dismiss this ticker code and iterate for other one
        WebDriverWait(driver, 10, 1).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '#tab-1 > div.box_content_tktt > ul > li:nth-child(2) > div.row2 > span')))

        elem = driver.find_element_by_css_selector(
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
        prices = [(float(x.get_text().strip()) if is_number(x.get_text().strip(
        )) else x.get_text().strip()) for x in source.select("li div.row1")]
        data_dict["open"] = prices[6::6]
        data_dict["highest"] = prices[7::6]
        data_dict["lowest"] = prices[8::6]
        data_dict["close"] = prices[9::6]
        data_dict["average"] = prices[10::6]
        data_dict["adjusted"] = prices[11::6]

        # Parsing volume
        volumes = [((float(x.get_text().strip())) if is_number(
            x.get_text().strip()) else None) for x in source.select("li div.row3")[2:]]
        data_dict["trading_volume"] = volumes[0::2]
        data_dict["put_through_volume"] = volumes[1::2]

        df = pd.DataFrame(data_dict)
        df.to_csv(f"{config.download_path}/{ticker_code}.csv", index=None)

        logger.info(f"Download complete for {ticker_code}.")
    except Exception as ex:
        logger.error(ticker_code + " | " + getattr(ex, 'message', repr(ex)))


def quit(driver):
    if driver:
        driver.close()
        driver.quit()


def get_tickers():
    with psycopg2.connect(config.conn_string) as conn:
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            return pd.read_sql_query(sql_queries.get_ticker_list, conn)


def load_historical_price(download_path, logger):
    file_path_list = []
    for root, dirs, files in os.walk(download_path):
        file_path_list = glob.glob(os.path.join(root, "*"))

    with psycopg2.connect(config.conn_string) as conn:
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            for file_path in file_path_list:
                prices = pd.read_csv(file_path)
                for i, price in prices.iterrows():
                    try:
                        date = datetime.strptime(
                            price.DATE.strip(), "%d/%m/%Y")
                        close = float(price.CLOSE)
                        ticker = price.TICKER.strip()
                        open = float(price.OPEN)
                        high = float(price.HIGH)
                        low = float(price.LOW)
                        volume = int(price.VOLUME)

                        cur.execute(
                            sql_queries.upsert_historical_price_table,
                            (date, close, ticker, open, high, low,
                             volume, close, open, high, low, volume)
                        )
                    except Exception as ex:
                        logger.error(price.TICKER + " | " +
                                     getattr(ex, "message", repr(ex)))


def main(n_days=4):
    """
    Download stock prices of last n days
    """
    # Init the logging instance
    logging.basicConfig(filename="./app.log",
                        format="%(asctime)s: %(levelname)s: %(message)s",
                        datefmt="%d/%m/%Y %I:%M:%S %p")
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    logging.info("Initialize the program")
    time_zone = "Asia/Saigon"
    date_format = "%d/%m/%Y"

    from_date = (datetime.now(timezone(time_zone)) +
                 timedelta(days=-n_days)).strftime(date_format)

    to_date = (datetime.now(timezone(time_zone)) +
               timedelta(days=1)).strftime(date_format)

    # Clean csv remaing if any before download
    delete_files(config.download_path, "*.*")

    # Run selenium to download csv files
    logging.info("Read list of ticker from database")

    tickers = get_tickers()
    logger.info(f"There are {tickers.shape[0]}")

    driver = initialize()
    crawl_ticker(driver, logger)

    # for i, ticker in tickers.iterrows():
    #     try:
    #         driver = initialize()
    #         process(driver, ticker.ticker_code, from_date, to_date, logger)
    #         quit(driver)
    #     except Exception as ex:
    #         quit(driver)
    #         logger.error(ticker.ticker_code + " | " +
    #                      getattr(ex, 'message', repr(ex)))

    # Update changes if any into historical price table
    # load_historical_price(config.download_path, logger)

    # Clean csv remaing if any after download
    # delete_files(config.download_path, "*.csv")


if __name__ == "__main__":
    logging.info("Program starts ...")
    main()
