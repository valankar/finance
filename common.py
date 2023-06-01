#!/usr/bin/env python3
"""Common functions."""

import functools
import json
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from os import path
from pathlib import Path
from threading import Lock, RLock

import psycopg2
import psycopg2.extras
from retry import retry
from selenium import webdriver
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from yahooquery import Ticker

import authorization

PREFIX = str(Path.home()) + authorization.PUBLIC_HTML
ALL_TICKERS = [
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
get_ticker_lock = Lock()
selenium_lock = RLock()
ticker_retrieval_fails = set()


def get_tickers(tickers: list) -> dict:
    """Get prices for a list of tickers."""
    ticker_dict = {}
    for ticker in tickers:
        ticker_dict[ticker] = get_ticker(ticker)
    return ticker_dict


@functools.cache
def get_ticker(ticker):
    """Get ticker prices by trying various methods."""
    ticker_retrieval_sequence = [
        get_ticker_yahooquery,
        get_all_tickers_steampipe_cloud,
        get_all_tickers_steampipe_local,
        get_ticker_browser,
    ]
    with get_ticker_lock:
        for method in ticker_retrieval_sequence:
            method_name = method.__name__
            match method_name:
                # Skip methods that have failed.
                case failed if failed in ticker_retrieval_fails:
                    pass
                case get_all_tickers_steampipe_cloud.__name__:
                    try:
                        return get_all_tickers_steampipe_cloud()[ticker]
                    except psycopg2.Error:
                        ticker_retrieval_fails.add(method_name)
                case get_ticker_yahooquery.__name__:
                    try:
                        return get_ticker_yahooquery(ticker)
                    # pylint: disable-next=broad-exception-caught
                    except Exception:
                        ticker_retrieval_fails.add(method_name)
                case get_all_tickers_steampipe_local.__name__:
                    try:
                        return get_all_tickers_steampipe_local()[ticker]
                    except subprocess.CalledProcessError:
                        ticker_retrieval_fails.add(method_name)
                case get_ticker_browser.__name__:
                    try:
                        return get_ticker_browser(ticker)
                    except NoSuchElementException:
                        ticker_retrieval_fails.add(method_name)
    print("No more methods to get ticker price")
    raise ValueError


@functools.cache
def get_ticker_browser(ticker):
    """Get ticker price from Yahoo via Selenium."""
    with selenium_lock:
        browser = get_browser()
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
def get_ticker_yahooquery(ticker):
    """Get ticker price via Yahooquery library."""
    return Ticker(ticker).price[ticker]["regularMarketPrice"]


@functools.cache
def get_all_tickers_steampipe_cloud():
    """Get ticker prices via Steampipe Clound. Returns dict."""
    conn = psycopg2.connect(authorization.STEAMPIPE_CLOUD_CONN)
    try:
        with conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as curs:
                tickers = ",".join([f"'{ticker}'" for ticker in ALL_TICKERS])
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
    tickers = ",".join([f"'{ticker}'" for ticker in ALL_TICKERS])
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
@retry(NoSuchElementException, delay=30, tries=4)
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


if __name__ == "__main__":
    for t in ALL_TICKERS:
        print(f"Ticker: {t} Value: {get_ticker(t)}")
