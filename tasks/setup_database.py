import os
import glob
from datetime import datetime
import psycopg2
import pandas as pd
from sql_queries import drop_table_queries, create_table_queries
import config as config


def create_database():
    """
    Create vietnam_stock database
    need to create superuserrole ahead
    """
    # Connect to default database with presetup user
    conn = psycopg2.connect(
        f"host=167.99.68.250 dbname=postgres user={config.db_username} password={config.db_psw}")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # Stop activities on target db
    cur.execute("""select * from pg_stat_activity where datname = 'vietnam_stock';
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = 'vietnam_stock';""")

    # Recreate database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS vietnam_stock")
    cur.execute(
        "CREATE DATABASE vietnam_stock WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    cur.close()

    # Connect to vietnam_stock database
    conn = psycopg2.connect(config.conn_string)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)


# def initial_load_ticker(file_path, cur, conn):
#     tickers = pd.read_csv(file_path)
#     for i, ticker in tickers.iterrows():
#         cur.execute(
#             upsert_ticker_table,
#             (
#                 ticker.ticker_code,
#                 ticker.company_name,
#                 None,
#                 ticker.stock_exchange,
#                 ticker.company_name,
#                 None,
#                 ticker.stock_exchange
#             )
#         )


# def initial_load_historical_price(file_path_list, cur, conn):
#     for file_path in file_path_list:
#         prices = pd.read_csv(file_path)
#         for i, price in prices.iterrows():
#             try:
#                 date = datetime.strptime(price.DATE.strip(), "%d/%m/%Y")
#                 close = float(price.CLOSE)
#                 ticker = price.TICKER.strip()
#                 open = float(price.OPEN)
#                 high = float(price.HIGH)
#                 low = float(price.LOW)
#                 volume = int(price.VOLUME)

#                 cur.execute(
#                     upsert_historical_price_table,
#                     (
#                         date,
#                         close,
#                         ticker,
#                         open,
#                         high,
#                         low,
#                         volume,
#                         close,
#                         open,
#                         high,
#                         low,
#                         volume
#                     )
#                 )
#             except Exception as ex:
#                 print(getattr(ex, "message", repr(ex)))


def main():
    # Create new database and tables
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    # # Inital load for ticker table
    # initial_load_ticker(
    #     "./data/initial_load/ticker/ticker_list.csv", cur, conn)

    # # Initial load for historical price table
    # file_path_list = []
    # for root, dirs, files in os.walk(config.initial_load_path):
    #     file_path_list = glob.glob(os.path.join(root, "*"))

    # initial_load_historical_price(file_path_list, cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
