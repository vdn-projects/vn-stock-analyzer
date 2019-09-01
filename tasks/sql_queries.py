

# Drop table
ticker_table_drop = """
DROP TABLE IF EXISTS ticker
"""

historical_price_table_drop = """
DROP TABLE IF EXISTS historical_price
"""

# Create table
ticker_table_create = """
CREATE TABLE IF NOT EXISTS ticker(
    ticker_code varchar(20),
    company_name varchar(500) NULL,
    company_full_name varchar(1000) NULL,
    sector varchar(1000) NULL,
    exchange varchar(500) NULL,
    CONSTRAINT ticker_tickercode_pkey PRIMARY KEY(ticker_code)
)
"""

historical_price_table_create = """
CREATE TABLE IF NOT EXISTS historical_price(
    ticker_code varchar(20) NOT NULL,
    date date NOT NULL,
    open float NULL,
    highest float NULL,
    lowest float NULL,
    close float NULL,
    average float NULL,
    adjusted float NULL,
    trading_volume NUMERIC(12,0) NULL,
    put_through_volume NUMERIC(12,0) NULL,
    CONSTRAINT historical_ticker_code_fkey FOREIGN KEY(ticker_code) REFERENCES ticker(ticker_code),
    CONSTRAINT historical_price_key UNIQUE(date, ticker_code)
)
"""

# Upsert data
upsert_ticker_table = """
INSERT INTO ticker(ticker_code, company_name, company_full_name, sector, exchange)
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT ticker_tickercode_pkey
DO NOTHING
"""

# The price presented fixed by date, no update required
upsert_historical_price_table = """
INSERT INTO historical_price(ticker_code, date, open, highest, lowest, close, average, adjusted, trading_volume, put_through_volume)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT historical_price_key 
DO NOTHING
"""

# The price presented fixed by date, no update required
insert_historical_price_table = """
INSERT INTO historical_price(ticker_code, date, open, highest, lowest, close, average, adjusted, trading_volume, put_through_volume)
VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# Select statement
get_ticker_list = """
SELECT ticker_code FROM ticker
"""


create_table_queries = [ticker_table_create, historical_price_table_create]

drop_table_queries = [ticker_table_drop, historical_price_table_drop]

upsert_table_queries = [upsert_ticker_table, upsert_historical_price_table]
