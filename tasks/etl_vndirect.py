import os
import time
import glob
from datetime import datetime, timedelta
from pytz import timezone
import logging

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import psycopg2
import pandas as pd

import tasks.config as config
import tasks.sql_queries as sql_queries


def wait_download(ticker_code, interval=1, max_wait=10):
    time_elapsed = 0
    fileList = glob.glob(config.download_path +
                         f"/historical-price-{ticker_code}*")
    while len(fileList) < 1:
        time.sleep(interval)
        time_elapsed += interval
        if time_elapsed > max_wait:
            raise Exception(
                f"Timeout of waiting ticker {ticker_code} downloaded!")

        fileList = glob.glob(config.download_path +
                             f"/historical-price-{ticker_code}*")
    return True


def get_logger(log_path, max_bytes=1000):
    pass


def delete_files(path, wildcard):
    for file in glob.glob(os.path.join(path, wildcard)):
        os.remove(file)


def confirm_download(driver):
    """
    Repeatly check the downloading file completed or not to execute the close step
    """
    if not driver.current_url.startswith("chrome://downloads"):
        driver.get("chrome://downloads/")
    return driver.execute_script("""
        var items = downloads.Manager.get().items_;
        if (items.every(e => e.state === "COMPLETE"))
            return items.map(e => e.file_url);
        """)


def enable_download_headless(browser, download_dir):
    browser.command_executor._commands["send_command"] = (
        "POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {
        'behavior': 'allow', 'downloadPath': download_dir}}
    browser.execute("send_command", params)


def initialize():
    # Init chrome driver
    url = "https://www.vndirect.com.vn/portal/thong-ke-thi-truong-chung-khoan/lich-su-gia.shtml?request_locale=en"
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-notifications")
    options.add_argument('--no-sandbox')
    options.add_argument('--verbose')
    options.add_experimental_option("prefs", {
        "download.default_directory": config.download_path,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing_for_trusted_sources_enabled": False,
        "safebrowsing.enabled": False
    })
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--headless')
    # options.add_argument('--disable-gpu')
    _driver = webdriver.Chrome(
        executable_path='/usr/local/bin/chromedriver', chrome_options=options)

    _driver.command_executor._commands["send_command"] = (
        "POST", '/session/$sessionId/chromium/send_command')
    params = {'cmd': 'Page.setDownloadBehavior', 'params': {
        'behavior': 'allow', 'downloadPath': config.download_path}}
    _driver.execute("send_command", params)

    # send_command = ('POST', '/session/$sessionId/chromium/send_command')
    # _driver.command_executor._commands['SEND_COMMAND'] = send_command
    # _driver.execute('SEND_COMMAND', dict(
    #     cmd='Network.clearBrowserCache', params={}))

    _driver.get(url)

    return _driver


def process(driver, ticker_code, from_date, to_date, logger):
    """
    Selenium task to down load the price list
    """

    # driver.refresh()

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
        # driver.implicitly_wait(5)

        # Wait until the table appear, over 5 seconds it will dismiss this ticker code and iterate for other one
        WebDriverWait(driver, 10, 1).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, '#tab-1 > div.box_content_tktt > ul > li:nth-child(2) > div.row2 > span')))
        # driver.implicitly_wait(15)

        elem = driver.find_element_by_css_selector(
            "#tab-1 > div.box_content_tktt > ul")
        source_code = elem.get_attribute("innerHTML")

        f = open(f'./vn_stock/data/download/{ticker_code}.html', 'wb')
        f.write(source_code.encode('utf-8'))
        f.close()

        # elem = WebDriverWait(driver, 5, 1).until(
        #     EC.presence_of_element_located((By.ID, 'hoseIcon')))

        # Click download button
        # driver.implicitly_wait(15)
        # elem = driver.find_element_by_css_selector(
        #     '#tab-1 > div.box_content_tktt > div > div > a > span.text')
        # elem.click()

        # Wait until the file is downloaded successfully
        # WebDriverWait(driver, 10, 2).until(wait_download(ticker_code, 10),
        #                                    f"Download complete for {ticker_code}.")
        # wait_download(ticker_code, 2, 10)
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

    for i, ticker in tickers.iterrows():
        try:
            driver = initialize()
            process(driver, ticker.ticker_code, from_date, to_date, logger)
            quit(driver)
        except Exception as ex:
            quit(driver)
            logger.error(ticker.ticker_code + " | " +
                         getattr(ex, 'message', repr(ex)))

    # Update changes if any into historical price table
    load_historical_price(config.download_path, logger)

    # Clean csv remaing if any after download
    delete_files(config.download_path, "*.csv")


if __name__ == "__main__":
    logging.info("Program starts ...")
    main()
