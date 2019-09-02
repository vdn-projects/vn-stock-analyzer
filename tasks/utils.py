import os
import glob
from selenium import webdriver
import logging
import psycopg2
import pandas as pd


class Utils:
    def __init__(self):
        pass

    @staticmethod
    def is_number(s):
        """ Returns True is string is a number. """
        return (s.replace('.', '', 1).isdigit())

    @staticmethod
    def replace_comma(s):
        """Replace comma by dot"""
        return s.replace(',', '.')

    @staticmethod
    def remove_comma(s):
        """Remove comma as part of cleaning process """
        return s.replace(',', '')

    @staticmethod
    def delete_files(path, wildcard):
        for file in glob.glob(os.path.join(path, wildcard)):
            os.remove(file)

    @staticmethod
    def get_logger(file_path):
        # Init the logging instance
        logging.basicConfig(filename=file_path,
                            format="%(asctime)s: %(levelname)s: %(message)s",
                            datefmt="%d/%m/%Y %I:%M:%S %p")
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)

        return logger

    @staticmethod
    def get_exchange(conn_str):
        with psycopg2.connect(conn_str) as conn:
            conn.set_session(autocommit=True)
            with conn.cursor() as cur:
                return pd.read_sql_query("SELECT DISTINCT exchange FROM ticker", conn)
