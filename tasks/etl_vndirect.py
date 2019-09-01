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

import vn_stock.tasks.config as config
import vn_stock.tasks.sql_queries as sql_queries

url_price = "https://www.vndirect.com.vn/portal/thong-ke-thi-truong-chung-khoan/lich-su-gia.shtml?request_locale=en"
url_ticker = "https://www.vndirect.com.vn/portal/thong-tin-co-phieu/nhap-ma-chung-khoan.shtml?request_locale=en_GB"


def is_number(s):
    """ Returns True is string is a number. """
    return (s.replace('.', '', 1).isdigit())


def replace_comma(s):
    return s.replace(',', '.')


def remove_comma(s):
    return s.replace(',', '')


def get_logger(log_path, max_bytes=1000):
    pass


def delete_files(path, wildcard):
    for file in glob.glob(os.path.join(path, wildcard)):
        os.remove(file)


def initialize(url):
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
        element = WebDriverWait(driver, 5, 1).until(EC.presence_of_element_located(
            (By.CSS_SELECTOR, '#fSearchSymbol_paging > div')))
        print(element.text.strip())
        if(">" in element.text.strip()):
            return False
        else:
            return True
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
            # df.to_csv(f"{config.download_path}/{count}.csv", index=None)

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


def click_next_price(driver, page_no, logger, max_retries=15):
    retry = 0
    while(retry < max_retries):
        try:
            # Check if the paging is availale
            element = WebDriverWait(driver, 5, 1).until(EC.presence_of_element_located(
                (By.CSS_SELECTOR, '#tab-1 > div.paging')))

            paging_content = element.text.strip()
            print(element.text.strip())
            if(">" in paging_content):
                # Click next page
                driver.execute_script(
                    f"javascript:_goTo({page_no})")
                time.sleep(2)
                return True
            else:
                return False

        except Exception as ex:  # Not found the next button
            if "javascript error:" in str(ex):
                logger.error("Not found javascript goto next page")
            else:
                logger.error(traceback.print_exc())

            # resources complete download
            # If the javascript is not found, driver will refresh number of time to have the web
            driver.refresh()
            time.sleep(2)
            retry += 1
            logger.info(f"refresh the page at {retry} try")
    return False


def load_price(driver, ticker_code, logger, mode="first_load"):
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
    prices = [(float(replace_comma(x.get_text().strip())) if is_number(remove_comma(x.get_text().strip(
    ))) else x.get_text().strip()) for x in source.select("li div.row1")]
    data_dict["open"] = prices[6::6]
    data_dict["highest"] = prices[7::6]
    data_dict["lowest"] = prices[8::6]
    data_dict["close"] = prices[9::6]
    data_dict["average"] = prices[10::6]
    data_dict["adjusted"] = prices[11::6]

    # Parsing volume
    volumes = [(int(float((x.get_text().strip()))) if is_number(
        (x.get_text().strip())) else None) for x in source.select("li div.row3")[2:]]
    data_dict["trading_volume"] = volumes[0::2]
    data_dict["put_through_volume"] = volumes[1::2]

    df = pd.DataFrame(data_dict)
    df = df.where(df.notna(), None)

    # df.to_csv(f"{config.download_path}/{ticker_code}.csv", index=None)

    if mode == "incremental_load":
        with psycopg2.connect(config.conn_string) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    try:
                        cur.execute(
                            sql_queries.upsert_historical_price_table, row.to_list())
                    except Exception as ex:
                        logger.error(row["ticker_code"] + " | " +
                                     traceback.print_exc())
    elif mode == "first_load":
        with psycopg2.connect(config.conn_string) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                try:
                    cur.executemany(
                        sql_queries.insert_historical_price_table, [tuple(x) for x in df.values.tolist()])
                except Exception as ex:
                    logger.error(row["ticker_code"] + " | " +
                                 traceback.print_exc())
    else:
        raise Exception("The working mode is not defined!")


def input_price_params(driver, ticker_code, from_date, to_date, logger):
    try:
        # Refresh the page again to reload resources
        driver.refresh()

        # Input ticker code
        elem = driver.find_element_by_css_selector('#symbolID')
        elem.send_keys(ticker_code)

        # Input time from
        elem = driver.find_element_by_css_selector(
            '#fHistoricalPrice_FromDate')
        elem.send_keys(from_date)

        # Input time to
        elem = driver.find_element_by_css_selector('#fHistoricalPrice_ToDate')
        elem.send_keys(to_date)

        # View historical price list
        elem = driver.find_element_by_css_selector('#fHistoricalPrice_View')
        elem.click()

    except Exception as ex:
        logger.error(traceback.print_exc())


def crawl_price(driver, ticker_code, from_date, to_date, logger):
    try:
        logger.info(f"Crawling {ticker_code} from {from_date} to {to_date}.")
        input_price_params(driver, ticker_code, from_date, to_date, logger)
        load_price(driver, ticker_code, logger)

        # Start checking if paging is available, then continue load more data
        page_no = 2
        while click_next_price(driver, page_no, logger):
            logger.info(f"Load price on page {page_no}.")
            load_price(driver, ticker_code, logger)
            page_no += 1

        logger.info(
            f"Complete crawling {ticker_code} from {from_date} to {to_date}.")
    except Exception as ex:
        logger.error(ticker_code + " | " + traceback.print_exc())


def quit(driver):
    if driver:
        driver.close()
        driver.quit()


def get_tickers():
    with psycopg2.connect(config.conn_string) as conn:
        conn.set_session(autocommit=True)
        with conn.cursor() as cur:
            return pd.read_sql_query(sql_queries.get_ticker_list, conn)


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

    # from_date = (datetime.now(timezone(time_zone)) +
    #              timedelta(days=-n_days)).strftime(date_format)

    from_date = "01/01/2010"

    to_date = (datetime.now(timezone(time_zone)) +
               timedelta(days=1)).strftime(date_format)

    # Clean csv remaing if any before download
    delete_files(config.download_path, "*.*")

    # Run selenium to download csv files
    logging.info("Read list of ticker from database")

    tickers = get_tickers()
    # tickers = pd.DataFrame({"ticker_code": ["AAA", "FPT"]})
    logger.info(f"There are {tickers.shape[0]}")

    # Crawl ticker dictionary
    # try:
    #     driver = initialize(url_ticker)
    #     crawl_ticker(driver, logger)
    #     quit(driver)
    # except Exception as ex:
    #     quit(driver)
    #     logger.error(traceback.print_exc())

    # Crawl historical price
    for _, ticker in tickers.iterrows():
        try:
            driver = initialize(url_price)
            # process(driver, ticker.ticker_code, from_date, to_date, logger)
            crawl_price(driver, ticker["ticker_code"],
                        from_date, to_date, logger)
            quit(driver)
        except Exception as ex:
            quit(driver)
            logger.error(ticker["ticker_code"] + " | " + traceback.print_exc())

    # Clean files remaning
    delete_files(config.download_path, "*.*")


if __name__ == "__main__":
    logging.info("Program starts ...")
    main()
    logging.info("Program finished! ...")
