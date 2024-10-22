#!/usr/bin/env python3
"""Common functions."""

import functools
import multiprocessing
import os
import shutil
import tempfile
import warnings
from contextlib import contextmanager
from functools import reduce
from pathlib import Path
from typing import Mapping, Sequence

import pandas as pd
import yahoofinancials
import yahooquery
import yfinance
from playwright.sync_api import sync_playwright
from sqlalchemy import create_engine
from sqlalchemy import text as sqlalchemy_text

PUBLIC_HTML = f"{Path.home()}/code/accounts/web/"
PREFIX = PUBLIC_HTML
LOCKFILE = f"{PREFIX}/run.lock"
LOCKFILE_TIMEOUT = 10 * 60
SQLITE_URI = f"sqlite:///{PREFIX}sqlite.db"
SQLITE_URI_RO = f"sqlite:///file:{PREFIX}sqlite.db?mode=ro&uri=true"
SQLITE3_URI_RO = f"file:{PREFIX}sqlite.db?mode=ro"
SELENIUM_REMOTE_URL = "http://selenium:4444"
LEDGER_BIN = "ledger"
LEDGER_DIR = f"{Path.home()}/code/ledger"
LEDGER_DAT = f"{LEDGER_DIR}/ledger.ledger"
LEDGER_PRICES_DB = f"{LEDGER_DIR}/prices.db"
# pylint: disable-next=line-too-long
LEDGER_PREFIX = f"{LEDGER_BIN} -f {LEDGER_DAT} --price-db {LEDGER_PRICES_DB} -X '$' -c --no-revalued"
GET_TICKER_TIMEOUT = 30


class GetTickerError(Exception):
    """Error getting ticker."""


def get_tickers(tickers: Sequence[str]) -> Mapping:
    """Get prices for a list of tickers."""
    ticker_dict = {}
    for ticker in tickers:
        ticker_dict[ticker] = get_ticker(ticker)
    return ticker_dict


def log_function_result(name, success, error_string=None):
    """Log the success or failure of a function."""
    to_sql(
        pd.DataFrame(
            {"name": name, "success": success, "error": error_string},
            index=[pd.Timestamp.now()],
        ),
        "function_result",
    )


@functools.cache
def get_ticker(ticker: str) -> float:
    """Get ticker prices by trying various methods."""
    get_ticker_methods = (
        get_ticker_yahooquery,
        get_ticker_yahoofinancials,
        get_ticker_yfinance,
    )
    for method in get_ticker_methods:
        name = method.__name__
        with multiprocessing.Pool(processes=1) as pool:
            async_result = pool.apply_async(method, (ticker,))
            try:
                return async_result.get(timeout=GET_TICKER_TIMEOUT)
            except multiprocessing.TimeoutError:
                log_function_result(name, False, "Timeout")
            # pylint: disable-next=broad-exception-caught
            except Exception as ex:
                log_function_result(name, False, str(ex))
    raise GetTickerError("No more methods to get ticker price")


def get_ticker_yahoofinancials(ticker: str) -> float:
    """Get ticker price via yahoofinancials library."""
    return yahoofinancials.YahooFinancials(ticker).get_current_price()  # type: ignore


def get_ticker_yahooquery(ticker: str) -> float:
    """Get ticker price via yahooquery library."""
    return yahooquery.Ticker(ticker).price[ticker]["regularMarketPrice"]  # type: ignore


def get_ticker_yfinance(ticker: str) -> float:
    """Get ticker price via yfinance library."""
    with warnings.catch_warnings():
        # See https://github.com/ranaroussi/yfinance/issues/1837
        warnings.simplefilter(action="ignore", category=FutureWarning)
        return yfinance.Ticker(ticker).history(period="5d")["Close"].iloc[-1]


def read_sql_table(table, index_col="date"):
    """Load table from sqlite."""
    with create_engine(SQLITE_URI_RO).connect() as conn:
        return pd.read_sql_table(table, conn, index_col=index_col)


def read_sql_query(query):
    """Load table from sqlite query."""
    with create_engine(SQLITE_URI_RO).connect() as conn:
        return pd.read_sql_query(
            sqlalchemy_text(query),
            conn,
            index_col="date",
            parse_dates=["date"],
        )


def to_sql(dataframe, table, if_exists="append", index_label="date", foreign_key=False):
    """Write dataframe to sqlite table."""
    with create_engine(SQLITE_URI).connect() as conn:
        if foreign_key:
            conn.execute(sqlalchemy_text("PRAGMA foreign_keys=ON"))
        dataframe.to_sql(table, conn, if_exists=if_exists, index_label=index_label)
        conn.commit()


def write_ticker_sql(
    amounts_table: str,
    prices_table: str,
    ticker_aliases: Mapping | None = None,
    ticker_prices: Mapping | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Just get the latest row and use columns to figure out tickers.
    amounts_df = read_sql_query(
        f"select * from {amounts_table} order by date desc limit 1"
    )
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
            browser.close()


def reduce_merge_asof(dataframes):
    """Reduce and merge date tables."""
    return reduce(
        lambda L, r: pd.merge_asof(L, r, left_index=True, right_index=True),
        dataframes,
    )


def load_sqlite_and_rename_col(table, rename_cols=None):
    """Load resampled table from sqlite and rename columns."""
    dataframe = read_sql_table(table)
    if rename_cols:
        dataframe = dataframe.rename(columns=rename_cols)
    return dataframe


def get_real_estate_df():
    """Get real estate price and rent data from sqlite."""
    price_df = (
        read_sql_table(
            "real_estate_prices",
        )[["name", "value"]]
        .groupby(["date", "name"])
        .mean()
        .unstack("name")
    )
    price_df.columns = price_df.columns.get_level_values(1) + " Price"
    price_df.columns.name = "variable"
    rent_df = (
        read_sql_table("real_estate_rents")
        .groupby(["date", "name"])
        .mean()
        .unstack("name")
    )
    rent_df.columns = rent_df.columns.get_level_values(1) + " Rent"
    rent_df.columns.name = "variable"
    return (
        reduce_merge_asof([price_df, rent_df])
        .sort_index(axis=1)
        .resample("D")
        .mean()
        .interpolate()
    )


if __name__ == "__main__":
    print(f'{get_ticker("SWYGX")}')
