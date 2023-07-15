#!/usr/bin/env python3
"""Common functions."""

import functools
import shelve
import shutil
import tempfile
from contextlib import contextmanager
from datetime import datetime
from os import path
from pathlib import Path
from threading import RLock
from timeit import default_timer as timer

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
from sqlalchemy import create_engine
from sqlalchemy import text as sqlalchemy_text

import authorization

PREFIX = str(Path.home()) + authorization.PUBLIC_HTML
SQLITE_URI = f"sqlite:///{PREFIX}sqlite.db"
TICKER_FAILURES_SHELF = f"{PREFIX}ticker_failures.shelf"

selenium_lock = RLock()


class GetTickerError(Exception):
    """Error getting ticker."""


def get_tickers(tickers: list) -> dict:
    """Get prices for a list of tickers."""
    ticker_dict = {}
    for ticker in tickers:
        ticker_dict[ticker] = get_ticker(ticker)
    return ticker_dict


def call_get_ticker(ticker, func, returns_dict, exception, force=False):
    """Calls a ticker getting method if it has not failed recently.

    If force is True, ignore previous errors and force a call.
    """
    with shelve.open(TICKER_FAILURES_SHELF) as ticker_failures:
        now = datetime.now()
        last_failure = ticker_failures.get(func.__name__)
        if not force and last_failure and ((now - last_failure).days < 1):
            raise GetTickerError
        try:
            if returns_dict:
                return func()[ticker]
            return func(ticker)
        except exception:
            ticker_failures[func.__name__] = now
    raise GetTickerError


@functools.cache
def get_ticker(ticker, test_all=False):
    """Get ticker prices by trying various methods.

    Failed methods are not retried until 1 day has passed.

    If test_all is True, all methods are tried with results printed.
    """
    get_ticker_methods = (
        (get_ticker_yahooquery, False, Exception),
        (get_ticker_yahoofinancials, False, Exception),
        (get_ticker_yfinance, False, Exception),
        (get_ticker_stockquotes, False, Exception),
        (get_ticker_browser, False, NoSuchElementException),
    )
    for method in get_ticker_methods:
        if test_all:
            print(f"{method[0].__name__}: ", end="")
            try:
                print(call_get_ticker(ticker, *method, True))
            except GetTickerError:
                print("FAILED")
            continue
        try:
            return call_get_ticker(ticker, *method)
        except GetTickerError:
            pass
    if not test_all:
        raise GetTickerError("No more methods to get ticker price")


@functools.cache
def get_ticker_browser(ticker):
    """Get ticker price from Yahoo via Selenium."""
    with selenium_lock:
        browser = get_browser()
        try:
            browser.get(f"https://finance.yahoo.com/quote/{ticker}")
            # First look for accept cookies dialog.
            try:
                browser.find_element(By.ID, "scroll-down-btn").click()
                browser.find_element(By.XPATH, '//button[text()="Accept all"]').click()
            except NoSuchElementException:
                pass
            try:
                return float(
                    browser.find_element(
                        By.XPATH,
                        '//*[@id="quote-header-info"]/div[3]/div[1]/div/fin-streamer[1]',
                    ).text.replace(",", "")
                )
            except NoSuchElementException:
                browser.save_full_page_screenshot(f"{PREFIX}/selenium_screenshot.png")
                raise
        finally:
            browser.quit()


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
    with create_engine(SQLITE_URI).connect() as conn:
        # Just get the latest row.
        amounts_df = pd.read_sql_query(
            sqlalchemy_text(
                f"select * from {amounts_table} order by rowid desc limit 1"
            ),
            conn,
            index_col="date",
            parse_dates=["date"],
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
    with create_engine(SQLITE_URI).connect() as conn:
        prices_df.to_sql(
            prices_table,
            conn,
            if_exists="append",
            index_label="date",
        )
        conn.commit()

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
@retry((NoSuchElementException, TimeoutException), delay=30, tries=4)
def find_xpath_via_browser(url, xpath):
    """Find XPATH via Selenium with retries. Returns text of element."""
    with selenium_lock:
        browser = get_browser()
        try:
            browser.get(url)
            try:
                if text := browser.find_element(By.XPATH, xpath).text:
                    return text
                raise NoSuchElementException
            except NoSuchElementException:
                browser.save_full_page_screenshot(f"{PREFIX}/selenium_screenshot.png")
                raise
        finally:
            browser.quit()


def get_browser():
    """Get a Selenium/Firefox browser."""
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    service = FirefoxService(log_path=path.devnull)
    browser = webdriver.Firefox(options=opts, service=service)
    return browser


def run_and_save_performance(funcs, table_name):
    """Run functions and save performance metrics."""
    perf_df_data = {}
    for func in funcs:
        column = f"{func.__module__}.{func.__name__}"
        start_time = timer()
        func()
        end_time = timer()
        perf_df_data[column] = end_time - start_time
    perf_df = pd.DataFrame(
        perf_df_data, index=[pd.Timestamp.now()], columns=sorted(perf_df_data.keys())
    )
    with create_engine(SQLITE_URI).connect() as conn:
        perf_df.to_sql(
            table_name,
            conn,
            if_exists="append",
            index_label="date",
        )
        conn.commit()
