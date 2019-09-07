import os
import glob
from datetime import datetime
import psycopg2
import pandas as pd
from sql_queries import drop_table_queries, create_table_queries

from vn_stock.tasks import config


def create_database():
    """
    Create vietnam_stock database
    need to create superuserrole ahead
    """
    # Connect to default database with presetup user
    conn = psycopg2.connect(config.conn_string)
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


def main():
    # Create new database and tables
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
