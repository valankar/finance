#!/usr/bin/env python3
"""Common functions."""

import csv
import json
import os
import shutil
import socket
import tempfile
import typing
from collections.abc import Iterable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from enum import StrEnum
from functools import reduce
from pathlib import Path
from typing import Any, ClassVar, Final, Generator, Mapping, Optional

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
# Include currency equivalents like money markets with $1 price.
CURRENCIES_REGEX = r"^(\\$|CHF|EUR|GBP|SGD|SWVXX)$"
LEDGER_CURRENCIES_CMD = f"{LEDGER_PREFIX} --limit 'commodity=~/{CURRENCIES_REGEX}/'"
SUBPLOT_MARGIN = {"l": 0, "r": 50, "b": 0, "t": 50}
SCHWAB_PAL_INTEREST_SPREAD = 2.8


class Brokerage(StrEnum):
    SCHWAB = "Charles Schwab Brokerage"
    SCHWAB_PAL = "Charles Schwab PAL Brokerage"
    IBKR = "Interactive Brokers"


OPTIONS_BROKERAGES = (Brokerage.IBKR, Brokerage.SCHWAB)


@dataclass
class FutureSpec:
    multiplier: float
    margin_requirement_percent: dict[Brokerage, float]


# Get margin requirements from Schwab or IBKR
FUTURE_SPEC: dict[str, FutureSpec] = {
    "10Y": FutureSpec(
        multiplier=1000,
        margin_requirement_percent={Brokerage.SCHWAB: 9, Brokerage.IBKR: 15},
    ),
    "M2K": FutureSpec(
        multiplier=5,
        margin_requirement_percent={Brokerage.SCHWAB: 9, Brokerage.IBKR: 9},
    ),
    "MBT": FutureSpec(
        multiplier=0.1,
        margin_requirement_percent={Brokerage.SCHWAB: 40, Brokerage.IBKR: 40},
    ),
    "MES": FutureSpec(
        multiplier=5,
        margin_requirement_percent={Brokerage.SCHWAB: 7, Brokerage.IBKR: 7},
    ),
    "MFS": FutureSpec(multiplier=50, margin_requirement_percent={Brokerage.IBKR: 6}),
    "MGC": FutureSpec(
        multiplier=10,
        margin_requirement_percent={Brokerage.SCHWAB: 16, Brokerage.IBKR: 11},
    ),
    "MTN": FutureSpec(multiplier=100, margin_requirement_percent={Brokerage.IBKR: 4}),
    # Silver 2500oz
    "QI": FutureSpec(
        multiplier=2500,
        margin_requirement_percent={Brokerage.SCHWAB: 30, Brokerage.IBKR: 21},
    ),
    # Silver 1000oz
    "SIL": FutureSpec(
        multiplier=1000,
        margin_requirement_percent={Brokerage.SCHWAB: 30},
    ),
    # Silver 5000oz
    "SI": FutureSpec(
        multiplier=5000,
        margin_requirement_percent={Brokerage.SCHWAB: 30, Brokerage.IBKR: 21},
    ),
    "TN": FutureSpec(multiplier=1000, margin_requirement_percent={Brokerage.IBKR: 4}),
    "ZN": FutureSpec(
        multiplier=1000,
        margin_requirement_percent={Brokerage.SCHWAB: 2, Brokerage.IBKR: 2},
    ),
}


def get_future_spec(ticker: str) -> FutureSpec:
    # Ticker ends with M26: /TNM26
    ticker = ticker[1:-3]
    return FUTURE_SPEC[ticker]


class GetTickerError(Exception):
    """Error getting ticker."""


class WalrusDb:
    def __init__(self):
        self.db = walrus.Database(host=os.environ.get("REDIS_HOST", "localhost"))
        self.cache = walrus.Cache(self.db, default_timeout=5 * 60)


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
    underlying_price: float


schwab_lock: Final[walrus.Lock] = walrus_db.db.lock(
    "schwab", ttl=LOCK_TTL_SECONDS * 1000
)


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

    def get_quotes(self, ts: Iterable[str]) -> dict[str, float]:
        r: dict[str, float] = {}
        j = self.client.get_quotes(ts).json()
        for t in ts:
            try:
                p = j[t]
            except KeyError:
                logger.error(f"Cannot find {t} in quote: {j}")
                raise GetTickerError(f"{t=} ticker not found")
            if "regular" in p:
                q = p["regular"]["regularMarketLastPrice"]
            elif "quote" in p:
                q = p["quote"]["lastPrice"]
            else:
                logger.error(p)
                raise GetTickerError(f"{t=} cannot find schwab price field")
            if q == 0:
                raise GetTickerError(f"{t=} received 0 as quote")
            logger.info(f"{t=} {q=}")
            r[t] = q
        return r

    def get_delta_override(self, symbol: str) -> float:
        if (p := Path(f"{PUBLIC_HTML}delta_overrides")).exists():
            with p.open("r") as f:
                reader = csv.reader(f, delimiter=":")
                for row in reader:
                    if row[0] == symbol:
                        return float(row[1])
        return 0

    def get_option_quotes(
        self, ts: Iterable[TickerOption]
    ) -> dict[TickerOption, OptionQuote]:
        results: dict[TickerOption, OptionQuote] = {}
        fetch_tickers: dict[str, TickerOption] = {}
        for t in ts:
            if t.expiration < date.today():
                raise GetTickerError(f"Option is expired: {t=}")
            symbol = OptionSymbol(
                t.ticker, t.expiration, t.contract_type[0], str(t.strike)
            ).build()
            fetch_tickers[symbol] = t
        if not fetch_tickers:
            return results
        js = self.client.get_quotes(fetch_tickers.keys()).json()
        for symbol, j in js.items():
            q = j["quote"]
            mark = q["mark"]
            delta = q["delta"]
            underlying_price = q["underlyingPrice"]
            if delta == 0:
                if new_delta := self.get_delta_override(symbol):
                    delta = new_delta
                    logger.info(f"{symbol=} overriding {delta=}")
            logger.info(f"{symbol=} {mark=} {delta=} {underlying_price=}")
            if abs(delta) > 1:
                raise GetTickerError(f"Invalid delta value: {delta=}")
            results[fetch_tickers[symbol]] = OptionQuote(
                mark=mark,
                delta=delta,
                underlying_price=underlying_price,
            )
        return results

    def get_option_quote(self, t: TickerOption) -> Optional[OptionQuote]:
        return self.get_option_quotes([t]).get(t, None)

    def get_future_quotes(self, ts: Iterable[str]) -> dict[str, FutureQuote]:
        r: dict[str, FutureQuote] = {}
        j = self.client.get_quotes(ts).json()
        for t, p in j.items():
            mark = p["quote"]["mark"]
            multiplier = p["reference"]["futureMultiplier"]
            q = FutureQuote(mark=mark, multiplier=multiplier)
            logger.info(f"{t=} {q=}")
            r[t] = q
        return r

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
def get_tickers(ts: Iterable[str]) -> dict[str, float]:
    prefix = "get_ticker:"
    r: dict[str, float] = {}
    fetch_ts = set()
    for t in ts:
        if t.endswith("USD"):
            fetch_ts.add(f"USD/{t[:3]}")
        elif t.startswith("SPX"):
            fetch_ts.add("$SPX")
        elif t.startswith("XSP"):
            fetch_ts.add("$XSP")
        else:
            fetch_ts.add(t)
    for t, p in walrus_db.cache.get_many([f"{prefix}{t}" for t in fetch_ts]).items():
        r[t.lstrip(prefix)] = p
    fetch_ts = set(fetch_ts) - set(r)
    if fetch_ts:
        qs = schwab_conn.get_quotes(fetch_ts)
        cache_qs = {f"{prefix}{t}": qs[t] for t in qs}
        walrus_db.cache.set_many(cache_qs)
        r.update(qs)
    for t in ts:
        if t.endswith("USD"):
            r[t] = 1 / r[f"USD/{t[:3]}"]
        elif t.startswith("SPX"):
            r[t] = r["$SPX"]
        elif t.startswith("XSP"):
            r[t] = r["$XSP"]
    return r


def make_option_key(t: TickerOption) -> str:
    return f"{t.ticker} {t.expiration} {t.strike} {t.contract_type}"


@schwab_lock
def get_option_quotes(
    ts: Iterable[TickerOption],
) -> dict[TickerOption, Optional[OptionQuote]]:
    r: dict[TickerOption, Optional[OptionQuote]] = {}
    prefix = "get_option_quote:"
    cache_keys: list[str] = [f"{prefix}{make_option_key(t)}" for t in ts]
    cached: dict[str, OptionQuote] = walrus_db.cache.get_many(cache_keys)
    needed: set[TickerOption] = set()
    for t in ts:
        if o := cached.get(f"{prefix}{make_option_key(t)}"):
            r[t] = o
        else:
            needed.add(t)
    if needed:
        qs = schwab_conn.get_option_quotes(needed)
        cache_qs = {f"{prefix}{make_option_key(t)}": qs[t] for t in qs}
        walrus_db.cache.set_many(cache_qs)
        r.update(qs)
        # Underlying prices are returned as well, save them
        cache_qs = {f"get_ticker:{k.ticker}": v.underlying_price for k, v in qs.items()}
        walrus_db.cache.set_many(cache_qs)
    return r


@schwab_lock
def get_future_quotes(ts: Iterable[str]) -> dict[str, FutureQuote]:
    prefix = "get_future_quote:"
    r: dict[str, FutureQuote] = {}
    fetch_ts = set()
    for t in ts:
        if t.startswith("/MTN"):
            fetch_ts.add(t.replace("/MTN", "/TN"))
        else:
            fetch_ts.add(t)
    for t, p in walrus_db.cache.get_many([f"{prefix}{t}" for t in fetch_ts]).items():
        r[t.lstrip(prefix)] = p
    fetch_ts = set(fetch_ts) - set(r)
    if fetch_ts:
        qs = schwab_conn.get_future_quotes(fetch_ts)
        cache_qs = {f"{prefix}{t}": qs[t] for t in qs}
        walrus_db.cache.set_many(cache_qs)
        r.update(qs)
    for t in ts:
        if t.startswith("/MTN"):
            q = r[t.replace("/MTN", "/TN")]
            r[t] = FutureQuote(mark=q.mark, multiplier=get_future_spec(t).multiplier)
    return r


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
    prices_table: str,
    ticker_prices: Mapping | None = None,
):
    # Just get the latest row and use columns to figure out tickers.
    if not ticker_prices:
        ticker_prices = get_tickers(read_sql_last(prices_table).columns)
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
