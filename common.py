#!/usr/bin/env python3
"""Common functions."""

import json
import multiprocessing
import os
import shutil
import tempfile
import typing
from contextlib import contextmanager
from datetime import date, datetime
from functools import reduce
from pathlib import Path
from typing import Any, ClassVar, Generator, Mapping, Optional

import duckdb
import pandas as pd
import requests
import schwab
import walrus
import yahoofinancials
import yahooquery
import yfinance
from authlib.integrations.starlette_client import OAuth, StarletteOAuth2App
from loguru import logger
from playwright.sync_api import sync_playwright
from pyngleton import singleton
from schwab.client import AsyncClient, Client
from schwab.orders.options import OptionSymbol

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


class TickerOption(typing.NamedTuple):
    ticker: str
    expiration: date
    contract_type: str
    strike: float


class FutureQuote(typing.NamedTuple):
    mark: float
    multiplier: float


@singleton
class Schwab:
    SCHWAB_TOKEN_FILE: ClassVar[str] = f"{CODE_DIR}/.schwab_token.json"

    def __init__(self):
        self.api_key: str = os.environ.get("SCHWAB_API_KEY", "")
        self.secret: str = os.environ.get("SCHWAB_SECRET", "")
        if not all([self.api_key, self.secret]):
            raise GetTickerError("No schwab environment variables found")
        self._client: Optional[Client | AsyncClient] = None
        self._oauth: Optional[StarletteOAuth2App] = None

    @property
    def client(self) -> Client | AsyncClient:
        if not self._client:
            logger.info("Creating Schwab client")
            self._client = schwab.auth.client_from_token_file(
                self.SCHWAB_TOKEN_FILE, self.api_key, self.secret
            )
        return self._client

    @property
    def oauth(self) -> StarletteOAuth2App:
        if not self._oauth:
            self._oauth = OAuth().register(
                name="schwab",
                client_id=self.api_key,
                client_secret=self.secret,
                access_token_url="https://api.schwabapi.com/v1/oauth/token",
                authorize_url="https://api.schwabapi.com/v1/oauth/authorize",
                client_kwargs={
                    "scope": "read",
                },
            )
            if self._oauth is None:
                raise GetTickerError("Cannot create oauth")
        return self._oauth

    def write_token(self, token: dict):
        logger.info("Writing token")
        try:
            with open(self.SCHWAB_TOKEN_FILE) as f:
                data = json.load(f)
        except FileNotFoundError:
            data = {}
        data["creation_timestamp"] = int(datetime.now().timestamp())
        data["token"] = token
        with open(self.SCHWAB_TOKEN_FILE, "w") as f:
            f.write(json.dumps(data))

    def get_quote(self, ticker: str) -> float:
        if ticker.startswith("^"):
            ticker = "$" + ticker[1:]
        invert = False
        match ticker:
            case "CHFUSD=X":
                ticker = "USD/CHF"
                invert = True
            case "SGDUSD=X":
                ticker = "USD/SGD"
                invert = True
            case "SMI":
                return 0
        try:
            p = self.client.get_quotes([ticker]).json()[ticker]
        except KeyError:
            logger.error(f"Cannot find {ticker} in quote")
            raise GetTickerError("ticker not found")
        if "regular" in p:
            value = p["regular"]["regularMarketLastPrice"]
        elif "quote" in p:
            value = p["quote"]["lastPrice"]
        else:
            logger.error(p)
            raise GetTickerError("cannot find schwab price field")
        if value == 0:
            raise GetTickerError("received 0 as quote")
        logger.info(f"{ticker=} {value=}")
        return value if not invert else 1 / value

    def get_option_quote(self, t: TickerOption) -> Optional[float]:
        ticker = t.ticker
        option_tickers = [ticker]
        if ticker == "SPX":
            option_tickers.append(f"{ticker}W")
        for option_ticker in option_tickers:
            if t.expiration < date.today():
                return 0
            symbol = OptionSymbol(
                option_ticker, t.expiration, t.contract_type[0], str(t.strike)
            ).build()
            try:
                value = self.client.get_quotes([symbol]).json()[symbol]["quote"]["mark"]
                logger.info(f"{symbol=} {value=}")
                return value
            except KeyError:
                logger.error(f"Cannot find quote for {symbol=}")
        return None

    def get_future_quote(self, ticker: str) -> FutureQuote:
        j = {}
        try:
            j = self.client.get_quotes([ticker]).json()[ticker]
            mark = j["quote"]["mark"]
            multiplier = j["reference"]["futureMultiplier"]
            q = FutureQuote(mark=mark, multiplier=multiplier)
            logger.info(f"{ticker=} {q=}")
            return q
        except KeyError:
            raise GetTickerError(f"Cannot find future quote for {ticker=} {j=}")


@contextmanager
def pandas_options():
    """Set pandas output options."""
    with pd.option_context(
        "display.max_rows", None, "display.max_columns", None, "display.width", 1000
    ):
        yield


GET_TICKER_FAILURES: set[str] = set()


def get_ticker_all(ticker: str) -> float:
    """Get ticker prices by trying various methods."""
    get_ticker_methods = (
        get_ticker_yahooquery,
        get_ticker_yahoofinancials,
        get_ticker_yfinance,
        get_ticker_alphavantage,
    )
    for method in get_ticker_methods:
        if method.__name__ in GET_TICKER_FAILURES:
            continue
        logger.info(f"Running {method.__name__}({ticker=})")
        with multiprocessing.Pool(processes=1) as pool:
            async_result = pool.apply_async(method, (ticker,))
            try:
                return async_result.get(timeout=30)
            except Exception as e:
                if err := str(e):
                    logger.error(err)
                GET_TICKER_FAILURES.add(method.__name__)
    raise GetTickerError("No more methods to get ticker price")


@WalrusDb().cache.cached(timeout=30 * 60)
def get_ticker(ticker: str) -> float:
    return Schwab().get_quote(ticker)


@WalrusDb().cache.cached(timeout=30 * 60)
def get_option_quote(t: TickerOption) -> float | None:
    return Schwab().get_option_quote(t)


@WalrusDb().cache.cached(timeout=30 * 60)
def get_future_quote(ticker: str) -> FutureQuote:
    return Schwab().get_future_quote(ticker)


def get_ticker_alphavantage(ticker: str) -> float:
    if key := os.environ.get("ALPHA_VANTAGE_KEY"):
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={ticker}&apikey={key}"
        data = requests.get(url).json()
        try:
            return float(data["Global Quote"]["05. price"])
        except KeyError:
            logger.error(data)
            raise
    raise GetTickerError("No alpha vantage key")


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
    ticker_prices: Mapping | None = None,
):
    # Just get the latest row and use columns to figure out tickers.
    amounts_df = read_sql_last(amounts_table)
    if not ticker_prices:
        ticker_prices = {}
        for ticker in amounts_df.columns:
            ticker_prices[ticker] = get_ticker(ticker)
    if not ticker_prices:
        logger.info("No ticker prices found. Not writing table.")
        return
    prices_df = pd.DataFrame(
        ticker_prices, index=[pd.Timestamp.now()], columns=sorted(ticker_prices.keys())
    ).rename_axis("date")
    to_sql(prices_df, prices_table)


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
        page = browser.new_page()
        try:
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
