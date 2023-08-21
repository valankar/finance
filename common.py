#!/usr/bin/env python3
"""Common functions."""

import functools
from functools import reduce
import shutil
import sqlite3
import tempfile
from contextlib import contextmanager, closing
from os import path
from pathlib import Path

import pandas as pd
import stockquotes
import yahoofinancials
import yahooquery
import yfinance
from retry import retry
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.support.wait import WebDriverWait
from sqlalchemy import create_engine
from sqlalchemy import text as sqlalchemy_text

import authorization

PREFIX = f"{Path.home()}{authorization.PUBLIC_HTML}"
SQLITE_URI = f"sqlite:///{PREFIX}sqlite.db"
SQLITE_URI_RO = f"sqlite:///file:{PREFIX}sqlite.db?mode=ro&uri=true"
SQLITE3_URI_RO = f"file:{PREFIX}sqlite.db?mode=ro"
LEDGER_BIN = f"{Path.home()}/miniforge3/envs/ledger/bin/ledger"
LEDGER_DIR = f"{Path.home()}/code/ledger"
LEDGER_DAT = f"{LEDGER_DIR}/ledger.ledger"
LEDGER_PRICES_DB = f"{LEDGER_DIR}/prices.db"
# pylint: disable-next=line-too-long
LEDGER_PREFIX = f"{LEDGER_BIN} -f {LEDGER_DAT} --price-db {LEDGER_PRICES_DB} -X '$' -c --no-revalued"
GECKODRIVER = f"{Path.home()}/miniforge3/envs/firefox/bin/geckodriver"
FIREFOX_BIN = f"{Path.home()}/miniforge3/envs/firefox/bin/firefox"


class GetTickerError(Exception):
    """Error getting ticker."""


def get_tickers(tickers: list) -> dict:
    """Get prices for a list of tickers."""
    ticker_dict = {}
    for ticker in tickers:
        ticker_dict[ticker] = get_ticker(ticker)
    return ticker_dict


# Only call this method once per script run.
@functools.cache
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
def function_failed_last_day(name):
    """Determine whether function has failed in the last day."""
    with closing(sqlite3.connect(SQLITE3_URI_RO, uri=True)) as con:
        res = con.execute(
            "select count(*) from function_result where success=False and "
            f"date > datetime('now', '-1 day') and name='{name}'"
        )
        return res.fetchone()[0] != 0


@functools.cache
def get_ticker(ticker):
    """Get ticker prices by trying various methods.

    Failed methods are not retried until 1 day has passed.
    """
    get_ticker_methods = (
        (get_ticker_yahooquery, Exception),
        (get_ticker_yahoofinancials, Exception),
        (get_ticker_yfinance, Exception),
        (get_ticker_stockquotes, Exception),
        (get_ticker_browser, NoSuchElementException),
    )
    for method, exc in get_ticker_methods:
        name = method.__name__
        if function_failed_last_day(name):
            continue
        try:
            result = method(ticker)
            log_function_result(name, True)
            return result
        # pylint: disable-next=broad-exception-caught
        except exc as ex:
            log_function_result(name, False, str(ex))
    raise GetTickerError("No more methods to get ticker price")


@functools.cache
def get_ticker_browser(ticker):
    """Get ticker price from Yahoo via Selenium."""

    def execute_before(browser):
        # First look for accept cookies dialog.
        try:
            browser.find_element(By.ID, "scroll-down-btn").click()
            browser.find_element(By.XPATH, '//button[text()="Accept all"]').click()
        except NoSuchElementException:
            pass

    return float(
        find_xpath_via_browser(
            f"https://finance.yahoo.com/quote/{ticker}",
            '//*[@id="quote-header-info"]/div[3]/div[1]/div/fin-streamer[1]',
            execute_before=execute_before,
        ).replace(",", "")
    )


@functools.cache
def get_ticker_stockquotes(ticker):
    """Get ticker price via stockquotes library."""
    return stockquotes.Stock(ticker).current_price


@functools.cache
def get_ticker_yahoofinancials(ticker):
    """Get ticker price via yahoofinancials library."""
    return yahoofinancials.YahooFinancials(ticker).get_current_price()


@functools.cache
def get_ticker_yahooquery(ticker):
    """Get ticker price via yahooquery library."""
    return yahooquery.Ticker(ticker).price[ticker]["regularMarketPrice"]


@functools.cache
def get_ticker_yfinance(ticker):
    """Get ticker price via yfinance library."""
    return yfinance.Ticker(ticker).history(period="5d")["Close"][-1]


def load_float_from_text_file(filename):
    """Get float value from a text file."""
    with open(filename, encoding="utf-8") as input_file:
        return float(input_file.read())


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


def read_sql_table_resampled_last(
    table, frequency="daily", extra_cols=None, other_group=None
):
    """Load table from sqlite resampling daily before loading.

    extra_cols is a list of columns auto-generated in sqlite to include.
    other_group is another group to partition by other than date.
    """
    append_sql = []
    match frequency:
        case "daily":
            partition_by = ["DATE(date)"]
        case "weekly":
            partition_by = ["DATE(date, 'weekday 5')"]
        case "hourly":
            partition_by = ["STRFTIME('%Y-%m-%d %H:00:00', date)"]
    with closing(sqlite3.connect(SQLITE3_URI_RO, uri=True)) as con:
        cols = [
            fields[1]
            for fields in con.execute(f"PRAGMA table_info({table})").fetchall()
        ]
        if extra_cols:
            cols.extend(extra_cols)
        for col in cols:
            if col == "date":
                continue
            if other_group and col == other_group:
                append_sql.append(other_group)
                partition_by.append(other_group)
                continue
            append_sql.append(f'last_value("{col}") OVER win AS "{col}"')
    sql = (
        f"SELECT DISTINCT {partition_by[0]} AS date, {', '.join(append_sql)} FROM "
        + f"{table} WINDOW win AS (PARTITION BY {', '.join(partition_by)} ORDER "
        + "BY date ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) "
        + "ORDER BY date ASC"
    )
    return read_sql_query(sql)


def to_sql(dataframe, table, if_exists="append", index_label="date", foreign_key=False):
    """Write dataframe to sqlite table."""
    with create_engine(SQLITE_URI).connect() as conn:
        if foreign_key:
            conn.execute(sqlalchemy_text("PRAGMA foreign_keys=ON"))
        dataframe.to_sql(table, conn, if_exists=if_exists, index_label=index_label)
        conn.commit()


def write_ticker_csv(
    amounts_table,
    prices_table,
    csv_output_path,
    ticker_col_name="ticker",
    ticker_amt_col="shares",
    ticker_aliases=None,
    ticker_prices=None,
):
    """Write ticker values to prices table and csv file.

    ticker_aliases is used to map name to actual ticker: GOLD -> GC=F
    """
    # Just get the latest row.
    amounts_df = read_sql_query(
        f"select * from {amounts_table} order by date desc limit 1"
    )
    if ticker_aliases:
        amounts_df = amounts_df.rename(columns=ticker_aliases)
    if not ticker_prices:
        ticker_prices = get_tickers(amounts_df.columns)
    prices_df = pd.DataFrame(
        ticker_prices, index=[pd.Timestamp.now()], columns=sorted(ticker_prices.keys())
    ).rename_axis("date")
    if ticker_aliases:
        prices_df = prices_df.rename(columns={v: k for k, v in ticker_aliases.items()})
    to_sql(prices_df, prices_table)

    if ticker_aliases:
        # Revert back columns names/tickers.
        amounts_df = amounts_df.rename(
            columns={v: k for k, v in ticker_aliases.items()}
        )
    latest_amounts = amounts_df.iloc[-1].rename(ticker_amt_col)
    latest_prices = prices_df.iloc[-1].rename("current_price")
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


@functools.cache
@retry(TimeoutException, delay=30, tries=4)
def find_xpath_via_browser(url, xpath, execute_before=None):
    """Find XPATH via Selenium with retries. Returns text of element.

    execute_before is method passed a browser that runs after the get and
    before waiting for xpath.
    """
    browser = get_browser()
    try:
        browser.get(url)
        if execute_before:
            execute_before(browser)
        try:
            return WebDriverWait(browser, timeout=30).until(
                lambda d: d.find_element(By.XPATH, xpath).text
            )
        except TimeoutException:
            browser.save_full_page_screenshot(f"{PREFIX}/selenium_screenshot.png")
            raise
    finally:
        browser.quit()


def get_browser():
    """Get a Selenium/Firefox browser."""
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    opts.binary_location = FIREFOX_BIN
    service = FirefoxService(log_path=path.devnull, executable_path=GECKODRIVER)
    browser = webdriver.Firefox(options=opts, service=service)
    return browser


def reduce_merge_asof(dataframes):
    """Reduce and merge date tables."""
    return reduce(
        lambda l, r: pd.merge_asof(l, r, left_index=True, right_index=True),
        dataframes,
    )


def load_sqlite_and_rename_col(
    table, frequency="daily", rename_cols=None, extra_cols=None, other_group=None
):
    """Load resampled table from sqlite and rename columns."""
    dataframe = read_sql_table_resampled_last(
        table, frequency=frequency, extra_cols=extra_cols, other_group=other_group
    )
    if rename_cols:
        dataframe = dataframe.rename(columns=rename_cols)
    return dataframe


def get_real_estate_df(frequency="daily"):
    """Get real estate price and rent data from sqlite."""
    price_df = (
        read_sql_table_resampled_last(
            "real_estate_prices",
            frequency=frequency,
            extra_cols=["value"],
            other_group="name",
        )[["name", "value"]]
        .groupby(["date", "name"])
        .mean()
        .unstack("name")
    )
    price_df.columns = price_df.columns.get_level_values(1) + " Price"
    price_df.columns.name = "variable"
    rent_df = (
        read_sql_table_resampled_last(
            "real_estate_rents", frequency=frequency, other_group="name"
        )
        .groupby(["date", "name"])
        .mean()
        .unstack("name")
    )
    rent_df.columns = rent_df.columns.get_level_values(1) + " Rent"
    rent_df.columns.name = "variable"
    return reduce_merge_asof([price_df, rent_df]).sort_index(axis=1).interpolate()
