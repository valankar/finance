#!/usr/bin/env python3
"""Common functions."""

import functools
import json
import shelve
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from datetime import datetime
from os import path
from pathlib import Path
from threading import RLock

import psycopg2
import psycopg2.extras
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

import authorization

PREFIX = str(Path.home()) + authorization.PUBLIC_HTML
TICKER_FAILURES_SHELF = f"{PREFIX}ticker_failures.shelf"
# When querying Steampipe, make one query to get all tickers needed.
STEAMPIPE_ALL_TICKERS = [
    "SGDUSD=X",
    "CHFUSD=X",
    "VWIAX",
    "SCHZ",
    "SCHX",
    "SCHO",
    "SCHF",
    "SCHE",
    "SCHB",
    "SCHA",
    "SCHR",
    "SI=F",
    "GC=F",
]
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
        (get_all_tickers_steampipe_cloud, True, psycopg2.Error),
        (get_ticker_yahooquery, False, Exception),
        (get_ticker_yahoofinancials, False, Exception),
        (get_ticker_yfinance, False, Exception),
        (get_ticker_stockquotes, False, Exception),
        (get_all_tickers_steampipe_local, True, subprocess.CalledProcessError),
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
    return yfinance.Ticker(ticker).history(period="1d")["Close"][0]


@functools.cache
def get_all_tickers_steampipe_cloud():
    """Get ticker prices via Steampipe Clound. Returns dict."""
    conn = psycopg2.connect(authorization.STEAMPIPE_CLOUD_CONN)
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as curs:
                tickers = ",".join([f"'{ticker}'" for ticker in STEAMPIPE_ALL_TICKERS])
                curs.execute(
                    # pylint: disable-next=line-too-long
                    f"select symbol, regular_market_price from finance_quote where symbol in ({tickers})"
                )
                result = curs.fetchall()
                ticker_prices = {}
                for res in result:
                    ticker_prices[res["symbol"]] = res["regular_market_price"]
                return ticker_prices
    finally:
        conn.close()


@functools.cache
def get_all_tickers_steampipe_local():
    """Get ticker price."""
    tickers = ",".join([f"'{ticker}'" for ticker in STEAMPIPE_ALL_TICKERS])
    process = subprocess.run(
        # pylint: disable-next=line-too-long
        f'$HOME/bin/steampipe query "select symbol, regular_market_price from finance_quote where symbol in ({tickers})" --output json',
        shell=True,
        check=True,
        text=True,
        capture_output=True,
    )
    ticker_prices = {}
    for res in json.loads(process.stdout):
        ticker_prices[res["symbol"]] = res["regular_market_price"]
    return ticker_prices


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
