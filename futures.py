import io
import pickle
import re
import subprocess
import typing
from datetime import datetime

import pandas as pd
from loguru import logger

import common
import ledger_ops

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

MULTIPLIERS: dict[str, float] = {
    "10Y": 1000,
    "M2K": 5,
    "MBT": 0.1,
    "MES": 5,
    "MFS": 50,
    "MGC": 10,
}

REDIS_KEY = "FuturesData"


class IceUnknownFuture(Exception):
    pass


class FuturesData(typing.NamedTuple):
    main_output: str
    updated: datetime


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
                mark=float(price), multiplier=MULTIPLIERS[ticker[1:4]]
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
        for line in entry.full_list():
            if future_name in line:
                s = re.split(r"\s{2,}", line.lstrip())
                count = int(s[1].split()[0])
                amount = s[1].split("@")[-1].lstrip()
                if amount.startswith("$"):
                    total += float(amount[1:]) * count
    return total


class Futures:
    def future_quote(self, ticker: str) -> pd.Series:
        if ticker.startswith("/MFS"):
            fq = get_future_quote_mfs(ticker)
        else:
            fq = common.get_future_quote(ticker)
            m = MULTIPLIERS[ticker[1:4]]
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
                entries.append(
                    {
                        "account": account,
                        "commodity": commodity,
                        "count": count,
                        "value": (current_price - trade_price) * multiplier * count,
                        "notional_value": current_price * multiplier * count,
                        "current_price": current_price,
                        "trade_price": trade_price,
                        "multiplier": multiplier,
                        "future": future_name,
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

    def save_to_redis(self):
        with common.pandas_options():
            df = self.futures_df
            total = df["value"].sum()
            notional = df.groupby(level="account")["notional_value"].sum()
            main_output = f"{df}\n\nTotal value: {total:.0f}\n\nNotional value by account:\n{notional}"
        data = FuturesData(
            main_output=main_output,
            updated=datetime.now(),
        )
        common.walrus_db.db[REDIS_KEY] = pickle.dumps(data)
