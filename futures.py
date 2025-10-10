import io
import pickle
import re
import subprocess
import typing
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import pandas as pd
from loguru import logger
from playwright.sync_api import Error as PlaywrightError

import common
import ledger_ops
import margin_loan

MONTH_CODES: dict[str, str] = {
    "F": "Jan",
    "G": "Feb",
    "H": "Mar",
    "J": "Apr",
    "K": "May",
    "M": "Jun",
    "N": "Jul",
    "Q": "Aug",
    "U": "Sep",
    "V": "Oct",
    "X": "Nov",
    "Z": "Dec",
}


class Brokerage(Enum):
    SCHWAB = "Charles Schwab Brokerage"
    IBKR = "Interactive Brokers"


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
        margin_requirement_percent={Brokerage.SCHWAB: 6, Brokerage.IBKR: 7},
    ),
    "MTN": FutureSpec(multiplier=100, margin_requirement_percent={Brokerage.IBKR: 4}),
    "ZN": FutureSpec(
        multiplier=1000,
        margin_requirement_percent={Brokerage.SCHWAB: 2, Brokerage.IBKR: 2},
    ),
}


REDIS_KEY = "FuturesData"


class IceUnknownFuture(Exception):
    pass


class FuturesData(typing.NamedTuple):
    main_output: str
    updated: datetime


@common.walrus_db.cache.cached()
def get_future_quote_mtn(ticker: str) -> common.FutureQuote:
    try:
        with common.run_with_browser_page(
            "https://www.cmegroup.com/markets/interest-rates/us-treasury/micro-ultra-10-year-us-treasury-note.quotes.html"
        ) as page:
            if selector := page.wait_for_selector("div.last-value"):
                value = selector.inner_text().replace("'", ".")
                q = common.FutureQuote(
                    mark=float(value), multiplier=FUTURE_SPEC["MTN"].multiplier
                )
                logger.info(f"ticker=/MTN {q=}")
                return q
    except PlaywrightError as e:
        logger.info(f"PlaywrightError: {e}, using /TNZ")
        m = FUTURE_SPEC[ticker[1:4]].multiplier
        ticker = ticker.replace("/MTN", "/TN")
        fq = common.get_future_quote(ticker)
        if fq.multiplier != m:
            fq = common.FutureQuote(mark=fq.mark, multiplier=m)
        return fq
    raise IceUnknownFuture("cannot find price")


@common.walrus_db.cache.cached()
def get_future_quote_mfs(ticker: str) -> common.FutureQuote:
    month_prefix = MONTH_CODES[ticker[4]]
    year = ticker[5:]
    contract = f"{month_prefix}{year}"
    logger.info(f"Looking for {ticker=} {contract=}")
    with common.run_with_browser_page(
        "https://www.ice.com/products/31196848/MSCI-EAFE-Index-Future/data"
    ) as page:
        cell = page.get_by_role("cell", name=contract)
        row = cell.locator("xpath=ancestor::tr")
        if price := row.locator("td").nth(1).text_content():
            q = common.FutureQuote(
                mark=float(price), multiplier=FUTURE_SPEC[ticker[1:4]].multiplier
            )
            logger.info(f"{ticker=} {q=}")
            return q
    raise IceUnknownFuture("cannot find price")


def get_ledger_entries_command(broker: str, name: str) -> str:
    return f"""{common.LEDGER_PREFIX} print expr 'any(commodity == "\\"{name}\\"" and account =~ /{broker}/)'"""


def get_ledger_total(broker: str, future_name: str) -> float:
    total = 0.0
    ledger_cmd = get_ledger_entries_command(broker, future_name)
    for entry in ledger_ops.get_ledger_entries_from_command(ledger_cmd):
        found_name = False
        for line in entry.full_list():
            if "Expenses:Broker:Fees" in line and found_name:
                amount = line.split(maxsplit=1)[-1]
                if amount.startswith("$"):
                    total += float(amount[1:])
            elif future_name in line:
                s = re.split(r"\s{2,}", line.lstrip())
                count = int(s[1].split()[0])
                amount = s[1].split("@")[-1].lstrip()
                if amount.startswith("$"):
                    total += float(amount[1:]) * count
                found_name = True
            elif "@" in line:
                found_name = False
    return total


class Futures:
    def future_quote(self, ticker: str) -> pd.Series:
        if ticker.startswith("/MFS"):
            fq = get_future_quote_mfs(ticker)
        elif ticker.startswith("/MTN"):
            fq = get_future_quote_mtn(ticker)
        else:
            fq = common.get_future_quote(ticker)
            m = FUTURE_SPEC[ticker[1:4]].multiplier
            if fq.multiplier != m:
                fq = common.FutureQuote(mark=fq.mark, multiplier=m)
        return pd.Series([fq.mark, fq.multiplier])

    @property
    def ledger_df(self) -> pd.DataFrame:
        cmd = (
            f"""{common.LEDGER_BIN} -f {common.LEDGER_DAT} --limit 'commodity=~/^"\\//' """
            + 'bal --no-total --flat --balance-format "%(partial_account)\n%(strip(T))\n"'
        )
        entries = []
        account = ""
        for line in io.StringIO(subprocess.check_output(cmd, shell=True, text=True)):
            if line[0].isalpha():
                account = line.strip().split(":")[-1]
                continue
            count = int(line.split(maxsplit=1)[0])
            future_name = line.split(maxsplit=1)[1].strip().strip('"')
            commodity = future_name.split()[0]
            current_price, multiplier = self.future_quote(commodity)
            trade_price = get_ledger_total(account, future_name) / count / multiplier
            if account:
                current_price, multiplier = self.future_quote(commodity)
                notional_value = current_price * multiplier * count
                entries.append(
                    {
                        "account": account,
                        "commodity": commodity,
                        "count": count,
                        "value": (current_price - trade_price) * multiplier * count,
                        "notional_value": notional_value,
                        "current_price": current_price,
                        "trade_price": trade_price,
                        "multiplier": multiplier,
                        "margin_requirement": round(
                            abs(
                                (
                                    FUTURE_SPEC[
                                        commodity[1:4]
                                    ].margin_requirement_percent[Brokerage(account)]
                                    / 100
                                )
                                * notional_value
                            )
                        ),
                    }
                )
        return pd.DataFrame(entries)

    @property
    def futures_df(self) -> pd.DataFrame:
        df = self.ledger_df.groupby(["account", "commodity"]).agg(
            {
                "count": "sum",
                "value": "sum",
                "notional_value": "sum",
                "current_price": "first",
                "trade_price": "mean",
                "multiplier": "first",
                "margin_requirement": "sum",
            }
        )
        return df

    @property
    def notional_values_df(self) -> pd.DataFrame:
        df = self.futures_df.reset_index()[["commodity", "notional_value"]]
        df.loc[:, "ticker"] = df["commodity"].str[:-3]
        df = (
            df.drop(columns="commodity")
            .groupby("ticker")
            .sum()
            .rename(columns={"notional_value": "value"})
        )
        return df

    @property
    def redis_data(self) -> FuturesData:
        return pickle.loads(common.walrus_db.db[REDIS_KEY])

    def get_summary(self) -> str:
        with common.pandas_options():
            df = self.futures_df
            total = df["value"].sum()
            total_by_account = df.groupby(level="account")["value"].sum()
            notional = df["notional_value"].sum()
            notional_by_account = df.groupby(level="account")["notional_value"].sum()
            margin_by_account = df.groupby(level="account")["margin_requirement"].sum()
            margin_reqs = []
            for broker in (Brokerage.IBKR.value, Brokerage.SCHWAB.value):
                cash = margin_loan.get_balances_broker(
                    margin_loan.find_loan_brokerage(broker)
                )["Cash Balance"].iloc[-1]
                req = margin_by_account[broker] * 2
                percent = cash / req
                margin_reqs.append(
                    f"{broker}: {cash:.0f} ({(percent * 100):.0f}% of 2x margin requirement "
                    f"({req:.0f}), Difference: {(cash - req):.0f})"
                )
            return (
                f"{df}\n\n"
                f"Total value: {total:.0f}\n"
                f"Total value by account:\n{total_by_account}\n\n"
                f"Notional value: {notional:.0f}\n"
                f"Notional value by account:\n{notional_by_account}\n\n"
                f"Margin requirement by account:\n{margin_by_account}\n\n"
                f"Cash available:\n" + "\n".join(margin_reqs)
            )

    def save_to_redis(self):
        data = FuturesData(
            main_output=self.get_summary(),
            updated=datetime.now(),
        )
        common.walrus_db.db[REDIS_KEY] = pickle.dumps(data)
