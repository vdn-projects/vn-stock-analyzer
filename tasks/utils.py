import os
import glob
from selenium import webdriver
import logging


def is_number(s):
    """ Returns True is string is a number. """
    return (s.replace('.', '', 1).isdigit())


def replace_comma(s):
    """Replace comma by dot"""
    return s.replace(',', '.')


def remove_comma(s):
    """Remove comma as part of cleaning process """
    return s.replace(',', '')


def delete_files(path, wildcard):
    for file in glob.glob(os.path.join(path, wildcard)):
        os.remove(file)


def get_logger():
    # Init the logging instance
    logging.basicConfig(filename="./app.log",
                        format="%(asctime)s: %(levelname)s: %(message)s",
                        datefmt="%d/%m/%Y %I:%M:%S %p")
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)


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


def quit(driver):
    if driver:
        driver.close()
        driver.quit()
