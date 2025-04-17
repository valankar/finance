#!/usr/bin/env python3
"""Common functions."""

import os
import shutil
import tempfile
import typing
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import datetime
from functools import reduce
from pathlib import Path
from typing import Any, Generator, Mapping, Optional, Sequence

import duckdb
import pandas as pd
import walrus
import yahoofinancials
import yahooquery
import yfinance
from loguru import logger
from playwright.sync_api import sync_playwright
from pyngleton import singleton

CODE_DIR = f"{Path.home()}/code/accounts"
PUBLIC_HTML = f"{CODE_DIR}/web/"
PREFIX = PUBLIC_HTML
LOCK_TTL_SECONDS = 10 * 60
DUCKDB = f"{PREFIX}/db.duckdb"
DUCKDB_LOCK_NAME = "duckdb"
SCRIPT_LOCK_NAME = "script"
SELENIUM_REMOTE_URL = "http://selenium:4444"
LEDGER_BIN = "ledger"
LEDGER_DIR = f"{Path.home()}/code/ledger"
LEDGER_DAT = f"{LEDGER_DIR}/ledger.ledger"
LEDGER_PRICES_DB = f"{LEDGER_DIR}/prices.db"
LEDGER_PREFIX = f"{LEDGER_BIN} -f {LEDGER_DAT} --price-db {LEDGER_PRICES_DB} -X '$' -c --no-revalued"
GET_TICKER_TIMEOUT = 30
PLOTLY_THEME = "plotly_dark"
CURRENCIES_REGEX = r"^(\\$|CHF|EUR|GBP|SGD|SWVXX)$"
COMMODITIES_REGEX = "^(GLD|GLDM|SGOL|SIVR|COIN|BITX|MSTR)$"
OPTIONS_LOAN_REGEX = '^("SPX|"SMI) '
LEDGER_CURRENCIES_OPTIONS_CMD = f"{LEDGER_PREFIX} --limit 'commodity=~/{CURRENCIES_REGEX}/ or commodity=~/{OPTIONS_LOAN_REGEX}/'"
BROKERAGES = ("Interactive Brokers", "Charles Schwab Brokerage")
SUBPLOT_MARGIN = {"l": 0, "r": 50, "b": 0, "t": 50}


class GetTickerError(Exception):
    """Error getting ticker."""


@singleton
class WalrusDb:
    def __init__(self):
        self.db = walrus.Database(host=os.environ.get("REDIS_HOST", "localhost"))
        self.cache = self.db.cache()


@contextmanager
def pandas_options():
    """Set pandas output options."""
    with pd.option_context(
        "display.max_rows", None, "display.max_columns", None, "display.width", 1000
    ):
        yield


def get_tickers(tickers: Sequence[str]) -> Mapping:
    """Get prices for a list of tickers."""
    with ThreadPoolExecutor() as e:
        prices = list(e.map(get_ticker, tickers))
    ticker_dict = {}
    for ticker, price in zip(tickers, prices):
        ticker_dict[ticker] = price
    return ticker_dict


def cache_ticker_prices():
    tickers = []
    for table in ["schwab_etfs_prices", "schwab_ira_prices", "index_prices"]:
        tickers.extend(read_sql_last(table).columns)
    for col in get_latest_forex().index:
        tickers.append(f"{col}=X")
    get_tickers(tickers)


@WalrusDb().cache.cached(timeout=30 * 60)
def get_ticker(ticker: str) -> float:
    """Get ticker prices by trying various methods."""
    get_ticker_methods = (
        get_ticker_yahooquery,
        get_ticker_yahoofinancials,
        get_ticker_yfinance,
    )
    for method in get_ticker_methods:
        logger.info(f"Running {method.__name__}({ticker=})")
        with ThreadPoolExecutor(max_workers=1) as e:
            f = e.submit(method, ticker)
            try:
                return f.result(timeout=GET_TICKER_TIMEOUT)
            except Exception:
                logger.exception("Failed")
    raise GetTickerError("No more methods to get ticker price")


def get_ticker_yahoofinancials(ticker: str) -> float:
    """Get ticker price via yahoofinancials library."""
    return typing.cast(
        float, yahoofinancials.YahooFinancials(ticker).get_current_price()
    )


def get_ticker_yahooquery(ticker: str) -> float:
    """Get ticker price via yahooquery library."""
    return typing.cast(dict, yahooquery.Ticker(ticker).price)[ticker][
        "regularMarketPrice"
    ]


def get_ticker_yfinance(ticker: str) -> float:
    """Get ticker price via yfinance library."""
    return yfinance.Ticker(ticker).history(period="5d")["Close"].iloc[-1]


def get_latest_forex() -> pd.Series:
    return read_sql_last("forex").iloc[-1]


@contextmanager
def duckdb_lock(
    read_only: bool = False,
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    with WalrusDb().db.lock(DUCKDB_LOCK_NAME, ttl=LOCK_TTL_SECONDS * 1000):
        with duckdb.connect(DUCKDB, read_only=read_only) as con:
            yield con


def compact_db():
    with WalrusDb().db.lock(DUCKDB_LOCK_NAME, ttl=LOCK_TTL_SECONDS * 1000):
        with temporary_file_move(DUCKDB) as new_db:
            with duckdb.connect() as con:
                con.execute(f"ATTACH '{DUCKDB}' as old")
                os.unlink(new_db.name)
                con.execute(f"ATTACH '{new_db.name}' as new")
                con.execute("COPY FROM DATABASE old TO new")


def insert_sql(table: str, data: dict[str, Any], timestamp: Optional[datetime] = None):
    """Insert data into sql table."""
    cols = ["date"]
    values = [timestamp]
    if timestamp is None:
        values = [datetime.now()]
    prepared = ["?"]
    for col, value in data.items():
        cols.append(f'"{col}"')
        values.append(value)
        prepared.append("?")
    with duckdb_lock() as con:
        con.execute(
            f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(prepared)})",
            values,
        )


def read_sql_table(table, index_col="date") -> pd.DataFrame:
    with duckdb_lock(read_only=True) as con:
        rel = con.table(table)
        if table == "history":
            # Add some convenience columns.
            rel = rel.project(
                "*, "
                "total_liquid + total_retirement + total_investing as total_no_homes, "
                "total_no_homes + total_real_estate as total"
            )
        return rel.df().set_index(index_col)


def read_sql_query(query) -> pd.DataFrame:
    """Load table from sql query."""
    with duckdb_lock(read_only=True) as con:
        return con.sql(query).df().set_index("date")


def read_sql_last(table: str) -> pd.DataFrame:
    return read_sql_query(f"select * from {table} order by date desc limit 1")


def to_sql(dataframe, table, if_exists="append"):
    """Write dataframe to sql table."""
    dataframe = dataframe.reset_index()
    with duckdb_lock() as con:
        if if_exists == "replace":
            con.execute("BEGIN TRANSACTION")
            con.execute(f"DROP TABLE IF EXISTS {table}")
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM dataframe")
            con.execute("COMMIT")
        else:
            sql = f"INSERT INTO {table} BY NAME SELECT * FROM dataframe"
            con.execute(sql)


def write_ticker_sql(
    amounts_table: str,
    prices_table: str,
    ticker_aliases: Mapping | None = None,
    ticker_prices: Mapping | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Just get the latest row and use columns to figure out tickers.
    amounts_df = read_sql_last(amounts_table)
    if ticker_aliases:
        amounts_df = amounts_df.rename(columns=ticker_aliases)
    if not ticker_prices:
        ticker_prices = get_tickers(list(amounts_df.columns))
    prices_df = pd.DataFrame(
        ticker_prices, index=[pd.Timestamp.now()], columns=sorted(ticker_prices.keys())
    ).rename_axis("date")
    if ticker_aliases:
        prices_df = prices_df.rename(columns={v: k for k, v in ticker_aliases.items()})
    to_sql(prices_df, prices_table)
    return amounts_df, prices_df


def write_ticker_csv(
    amounts_table: str,
    prices_table: str,
    csv_output_path: str,
    ticker_col_name: str = "ticker",
    ticker_amt_col: str = "shares",
    ticker_aliases: Mapping | None = None,
    ticker_prices: Mapping | None = None,
):
    """Write ticker values to prices table and csv file.

    ticker_aliases is used to map name to actual ticker: GOLD -> GC=F
    """
    amounts_df, prices_df = write_ticker_sql(
        amounts_table, prices_table, ticker_aliases, ticker_prices
    )
    if ticker_aliases:
        # Revert back columns names/tickers.
        amounts_df = amounts_df.rename(
            columns={v: k for k, v in ticker_aliases.items()}
        )
    latest_amounts = amounts_df.iloc[-1].rename(ticker_amt_col).sort_index()
    latest_prices = prices_df.iloc[-1].rename("current_price").sort_index()
    # Multiply latest amounts by prices.
    latest_values = (latest_amounts * latest_prices.values).rename("value")
    new_df = pd.DataFrame([latest_amounts, latest_prices, latest_values]).T.rename_axis(
        ticker_col_name
    )
    new_df.to_csv(csv_output_path)


@contextmanager
def temporary_file_move(dest_file):
    """Provides a temporary file that is moved in place after context."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as write_file:
        yield write_file
    shutil.move(write_file.name, dest_file)


def schwab_browser_page(page, accept_cookies=False):
    """Click popups that sometimes appears."""
    page.get_by_text("Continue with a limited experience").click()
    # Only necessary outside of US.
    if accept_cookies:
        page.get_by_role("button", name="Accept All Cookies").click()
    return page


@contextmanager
def run_with_browser_page(url):
    """Run code with a Chromium browser page."""
    if not os.environ.get("SELENIUM_REMOTE_URL"):
        os.environ["SELENIUM_REMOTE_URL"] = SELENIUM_REMOTE_URL
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        try:
            page = browser.new_page()
            page.goto(url)
            yield page
        finally:
            page.screenshot(path=f"{PREFIX}/screenshot.png", full_page=True)
            browser.close()


def reduce_merge_asof(dataframes):
    """Reduce and merge date tables."""
    return reduce(
        lambda L, r: pd.merge_asof(L, r, left_index=True, right_index=True),
        dataframes,
    )


def load_sql_and_rename_col(table, rename_cols=None):
    """Load resampled table from sql and rename columns."""
    dataframe = read_sql_table(table)
    if rename_cols:
        dataframe = dataframe.rename(columns=rename_cols)
    return dataframe


if __name__ == "__main__":
    print(f"{get_ticker('SWYGX')}")
