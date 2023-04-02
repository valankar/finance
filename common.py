"""Common functions."""

import functools
import json
import shutil
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path

import psycopg2
import psycopg2.extras
from retry import retry

import authorization

PREFIX = str(Path.home()) + authorization.PUBLIC_HTML
ALL_TICKERS = [
    'SGDUSD=X', 'CHFUSD=X', 'VWIAX', 'ETF7', 'ETF6', 'ETF5', 'ETF4', 'ETF3',
    'ETF2', 'ETF1', 'SI=F', 'GC=F'
]


def get_tickers(tickers: list) -> dict:
    """Get prices for a list of tickers."""
    ticker_dict = {}
    for ticker in tickers:
        ticker_dict[ticker] = get_ticker(ticker)
    return ticker_dict


def get_ticker(ticker):
    """Get ticker prices from cached data."""
    return get_all_tickers_steampipe_cloud()[ticker]


@functools.cache
def get_all_tickers_steampipe_cloud():
    """Get ticker prices via Steampipe Clound."""
    conn = psycopg2.connect(authorization.STEAMPIPE_CLOUD_CONN)
    try:
        with conn:
            with conn.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor) as curs:
                tickers = ','.join([f"'{ticker}'" for ticker in ALL_TICKERS])
                curs.execute(
                    # pylint: disable=line-too-long
                    f"select symbol, regular_market_price from finance_quote where symbol in ({tickers})"
                )
                result = curs.fetchall()
                ticker_prices = {}
                for res in result:
                    ticker_prices[res['symbol']] = res['regular_market_price']
                return ticker_prices
    finally:
        conn.close()


@functools.cache
@retry(subprocess.CalledProcessError, delay=1, jitter=1, tries=4)
def get_all_tickers_steampipe_local():
    """Get ticker price."""
    tickers = ','.join([f"'{ticker}'" for ticker in ALL_TICKERS])
    try:
        process = subprocess.run(
            # pylint: disable=line-too-long
            f'$HOME/bin/steampipe query "select symbol, regular_market_price from finance_quote where symbol in ({tickers})" --output json',
            shell=True,
            check=True,
            text=True,
            capture_output=True)
    except subprocess.CalledProcessError as ex:
        print(ex.stderr)
        raise
    ticker_prices = {}
    for res in json.loads(process.stdout):
        ticker_prices[res['symbol']] = res['regular_market_price']
    return ticker_prices


@contextmanager
def temporary_file_move(dest_file):
    """Provides a temporary file that is moved in place after context."""
    with tempfile.NamedTemporaryFile(mode='w', delete=False) as write_file:
        yield write_file
    shutil.move(write_file.name, dest_file)


if __name__ == '__main__':
    print(get_all_tickers_steampipe_local())
