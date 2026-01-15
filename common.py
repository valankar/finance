#!/usr/bin/env python3
"""Common functions."""

import json
import os
import shutil
import socket
import tempfile
import typing
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import date, datetime
from functools import reduce
from pathlib import Path
from typing import Any, ClassVar, Final, Generator, Mapping, Optional, TypedDict

import duckdb
import pandas as pd
import schwab
import walrus
from authlib.integrations.starlette_client import OAuth, StarletteOAuth2App
from loguru import logger
from playwright.sync_api import sync_playwright
from schwab.client import AsyncClient, Client
from schwab.orders.options import OptionSymbol

CODE_DIR = f"{Path.home()}/code/accounts"
PUBLIC_HTML = f"{CODE_DIR}/web/"
PREFIX = PUBLIC_HTML
LOCK_TTL_SECONDS = 10 * 60
DUCKDB = f"{PREFIX}/db.duckdb"
DUCKDB_LOCK_NAME = "duckdb"
SCRIPT_LOCK_NAME = "script"
LEDGER_BIN = "ledger"
LEDGER_DIR = f"{Path.home()}/code/ledger"
LEDGER_DAT = f"{LEDGER_DIR}/ledger.ledger"
LEDGER_PRICES_DB = f"{LEDGER_DIR}/prices.db"
LEDGER_PREFIX = f"{LEDGER_BIN} -f {LEDGER_DAT} --price-db {LEDGER_PRICES_DB} -X '$' -c --no-revalued"
GET_TICKER_TIMEOUT = 30
PLOTLY_THEME = "plotly_dark"
# Include currency equivalents like money marketsa with $1 price.
CURRENCIES_REGEX = r"^(\\$|CHF|EUR|GBP|SGD|SWVXX)$"
LEDGER_CURRENCIES_CMD = f"{LEDGER_PREFIX} --limit 'commodity=~/{CURRENCIES_REGEX}/'"
BROKERAGES = ("Interactive Brokers", "Charles Schwab Brokerage")
SUBPLOT_MARGIN = {"l": 0, "r": 50, "b": 0, "t": 50}


class GetTickerError(Exception):
    """Error getting ticker."""


class WalrusDb:
    def __init__(self):
        self.db = walrus.Database(host=os.environ.get("REDIS_HOST", "localhost"))
        self.cache = walrus.Cache(self.db)


walrus_db: Final[WalrusDb] = WalrusDb()


class TickerOption(typing.NamedTuple):
    ticker: str
    expiration: date
    contract_type: str
    strike: float


class FutureQuote(typing.NamedTuple):
    mark: float
    multiplier: float


class OptionQuote(typing.NamedTuple):
    mark: float
    delta: float


schwab_lock: Final[walrus.Lock] = walrus_db.db.lock(
    "schwab", ttl=LOCK_TTL_SECONDS * 1000
)


class Schwab:
    class TickerMod(TypedDict):
        alias: str
        invert: bool

    SCHWAB_TOKEN_FILE: ClassVar[str] = f"{CODE_DIR}/.schwab_token.json"
    TICKER_MODS: dict[str, TickerMod] = {
        "CHFUSD=X": {"alias": "USD/CHF", "invert": True},
        "SGDUSD=X": {"alias": "USD/SGD", "invert": True},
    }

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

    def get_quotes(self, ts: Iterable[str]) -> dict[str, float]:
        results: dict[str, float] = {}
        fetch_tickers: set[str] = set()
        rename_results: dict[str, str] = {}
        for ticker in ts:
            if ticker.startswith("^"):
                alias = "$" + ticker[1:]
                rename_results[alias] = ticker
                ticker = alias
            elif alias := self.TICKER_MODS.get(ticker, {}).get("alias"):
                rename_results[alias] = ticker
                ticker = alias
            fetch_tickers.add(ticker)
        if not fetch_tickers:
            return results
        j = self.client.get_quotes(fetch_tickers).json()
        for ticker in fetch_tickers:
            try:
                p = j[ticker]
            except KeyError:
                logger.error(f"Cannot find {ticker} in quote: {j}")
                raise GetTickerError(f"{ticker=} ticker not found")
            if "regular" in p:
                value = p["regular"]["regularMarketLastPrice"]
            elif "quote" in p:
                value = p["quote"]["lastPrice"]
            else:
                logger.error(p)
                raise GetTickerError(f"{ticker=} cannot find schwab price field")
            if value == 0:
                raise GetTickerError(f"{ticker=} received 0 as quote")
            logger.info(f"{ticker=} {value=}")
            if original := rename_results.get(ticker):
                ticker = original
                if self.TICKER_MODS.get(ticker, {}).get("invert"):
                    value = 1 / value
            results[ticker] = value
        return results

    def get_quote(self, ticker: str) -> float:
        return self.get_quotes([ticker])[ticker]

    def get_option_quotes(
        self, ts: Iterable[TickerOption]
    ) -> dict[TickerOption, Optional[OptionQuote]]:
        results: dict[TickerOption, Optional[OptionQuote]] = {}
        fetch_tickers: dict[str, TickerOption] = {}
        for t in ts:
            ticker = t.ticker
            option_tickers = [ticker]
            for option_ticker in option_tickers:
                if t.expiration < date.today():
                    results[t] = None
                symbol = OptionSymbol(
                    option_ticker, t.expiration, t.contract_type[0], str(t.strike)
                ).build()
                fetch_tickers[symbol] = t
        if not fetch_tickers:
            return results
        js = self.client.get_quotes(fetch_tickers.keys()).json()
        for symbol, j in js.items():
            mark = j["quote"]["mark"]
            delta = j["quote"]["delta"]
            logger.info(f"{symbol=} {mark=} {delta=}")
            if abs(delta) > 1:
                raise GetTickerError(f"Invalid delta value: {delta=}")
            results[fetch_tickers[symbol]] = OptionQuote(mark=mark, delta=delta)
        return results

    def get_option_quote(self, t: TickerOption) -> Optional[OptionQuote]:
        return self.get_option_quotes([t]).get(t, None)

    def get_future_quotes(self, ts: Iterable[str]) -> dict[str, FutureQuote]:
        results: dict[str, FutureQuote] = {}
        fetch_tickers: set[str] = set(ts)
        if not fetch_tickers:
            return results
        j = self.client.get_quotes(fetch_tickers).json()
        for t, p in j.items():
            mark = p["quote"]["mark"]
            multiplier = p["reference"]["futureMultiplier"]
            q = FutureQuote(mark=mark, multiplier=multiplier)
            logger.info(f"{t=} {q=}")
            results[t] = q
        return results

    def get_future_quote(self, ticker: str) -> FutureQuote:
        return self.get_future_quotes([ticker])[ticker]


schwab_conn: Final[Schwab] = Schwab()


@contextmanager
def pandas_options():
    """Set pandas output options."""
    with pd.option_context(
        "display.max_rows", None, "display.max_columns", None, "display.width", 1000
    ):
        yield


@schwab_lock
@walrus_db.cache.cached(key_fn=lambda a, _: a[0])
def get_ticker(ticker: str) -> float:
    return schwab_conn.get_quote(ticker)


@schwab_lock
def cache_tickers(ts: Iterable[str]):
    for t, p in schwab_conn.get_quotes(ts).items():
        walrus_db.cache.set(f"get_ticker:{t}", p)


def make_option_key(t: TickerOption) -> str:
    return f"{t.ticker} {t.expiration} {t.strike} {t.contract_type}"


@schwab_lock
def cache_option_quotes(ts: Iterable[TickerOption]):
    for t, p in schwab_conn.get_option_quotes(ts).items():
        walrus_db.cache.set(f"get_option_quote:{make_option_key(t)}", p)


@schwab_lock
@walrus_db.cache.cached(key_fn=lambda a, _: make_option_key(a[0]))
def get_option_quote(t: TickerOption) -> Optional[OptionQuote]:
    return schwab_conn.get_option_quote(t)


@schwab_lock
@walrus_db.cache.cached(key_fn=lambda a, _: a[0])
def get_future_quote(ticker: str) -> FutureQuote:
    return schwab_conn.get_future_quote(ticker)


@contextmanager
def duckdb_lock(
    read_only: bool = False,
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    with walrus_db.db.lock(DUCKDB_LOCK_NAME, ttl=LOCK_TTL_SECONDS * 1000):
        with duckdb.connect(DUCKDB, read_only=read_only) as con:
            yield con


def compact_db():
    with walrus_db.db.lock(DUCKDB_LOCK_NAME, ttl=LOCK_TTL_SECONDS * 1000):
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
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(
            f"http://{socket.gethostbyname('chrome-cdp')}:9222"
        )
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
