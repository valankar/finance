import csv
import io
import re
import subprocess
from pathlib import Path
from typing import Iterable

import pandas as pd
from loguru import logger

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


class IceUnknownFuture(Exception):
    pass


@common.walrus_db.cache.cached(key_fn=lambda a, _: a[0])
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
                mark=float(price), multiplier=common.get_future_spec(ticker).multiplier
            )
            logger.info(f"{ticker=} {q=}")
            return q
    raise IceUnknownFuture("cannot find price")


def get_trade_price_override(broker: str, future_name: str) -> float:
    if (p := Path(f"{common.PUBLIC_HTML}futures_trade_price_overrides")).exists():
        with p.open("r") as f:
            reader = csv.reader(f, delimiter=":")
            for row in reader:
                if row[0] == broker and row[1] == future_name:
                    return float(row[2])
    return 0


def get_trade_price(broker: str, future_name: str, want_count: int) -> float:
    if p := get_trade_price_override(broker, future_name):
        return p
    cmd = f"""{common.LEDGER_PREFIX} print expr 'any(commodity == "{future_name}" and account =~ /{broker}:Futures/)'"""
    total_count = total_cost = 0
    for entry in reversed(ledger_ops.get_ledger_entries_from_command(cmd)):
        for line in entry.body:
            s = line.split()
            if len(s) < 4:
                continue
            if future_name not in s[-3]:
                continue
            count = int(s[-4])
            if count * want_count < 0:
                continue
            cost = float(re.sub(r"[^\d.]", "", s[-1])) * count
            total_count += count
            total_cost += cost
            if total_count == want_count:
                return (
                    total_cost
                    / total_count
                    / common.get_future_spec(future_name).multiplier
                )
    return total_cost / total_count / common.get_future_spec(future_name).multiplier


class Futures:
    def future_quotes(self, ts: Iterable[str]) -> dict[str, common.FutureQuote]:
        r: dict[str, common.FutureQuote] = {}
        fetch_tickers = set()
        for t in ts:
            if t.startswith("/MFS"):
                r[t] = get_future_quote_mfs(t)
            else:
                fetch_tickers.add(t)
        r.update(common.get_future_quotes(fetch_tickers))
        return r

    @property
    def ledger_df(self) -> pd.DataFrame:
        cmd = (
            f"""{common.LEDGER_BIN} -f {common.LEDGER_DAT} --limit 'commodity=~/^"\\//' """
            + 'bal --no-total --flat --balance-format "%(partial_account)\n%(strip(T))\n"'
        )
        entries = []
        account = ""
        ts = set()
        for line in io.StringIO(subprocess.check_output(cmd, shell=True, text=True)):
            if line[0].isalpha():
                account = line.strip().split(":")[-2]
                continue
            count = int(line.split(maxsplit=1)[0])
            future_name = line.split(maxsplit=1)[1].strip().strip('"')
            commodity = future_name.split()[0]
            trade_price = get_trade_price(account, future_name, count)
            if account:
                entries.append(
                    {
                        "account": account,
                        "commodity": commodity,
                        "count": count,
                        "trade_price": trade_price,
                    }
                )
                ts.add(commodity)
        qs = self.future_quotes(ts)
        for e in entries:
            commodity = e["commodity"]
            trade_price = e["trade_price"]
            count = e["count"]
            current_price = qs[commodity].mark
            multiplier = qs[commodity].multiplier
            notional_value = current_price * multiplier * count
            margin_req = abs(
                round(
                    (
                        common.get_future_spec(commodity).margin_requirement_percent[
                            common.Brokerage(account)
                        ]
                        / 100
                    )
                    * notional_value
                )
            )
            e.update(
                {
                    "value": (current_price - trade_price) * multiplier * count,
                    "notional_value": notional_value,
                    "current_price": current_price,
                    "multiplier": multiplier,
                    "margin_requirement": margin_req,
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

    def get_summary(self) -> str:
        with common.pandas_options():
            df = self.futures_df
            total = df["value"].sum()
            total_by_account = df.groupby(level="account")["value"].sum()
            notional = df["notional_value"].sum()
            notional_by_account = df.groupby(level="account")["notional_value"].sum()
            margin_by_account = df.groupby(level="account")["margin_requirement"].sum()
            margin_reqs = []
            overrides = []
            b = margin_loan.get_balances_broker()
            for broker in (common.Brokerage.IBKR, common.Brokerage.SCHWAB):
                margin_df = b[broker]
                cash = (
                    margin_df["Cash Balance"].iloc[-1]
                    - margin_df["Money Market"].iloc[-1]
                )
                if broker not in margin_by_account:
                    continue
                for f in df.xs(broker, level="account").itertuples():
                    if get_trade_price_override(broker, f.Index):  # type: ignore
                        overrides.append(
                            f"{broker} trade price for {f.Index} is overridden"
                        )
                req = margin_by_account[broker]
                percent = cash / req
                margin_reqs.append(
                    f"{broker}: {cash:.0f} ({(percent * 100):.0f}% of margin requirement "
                    f"({req:.0f}), Difference: {(cash - req):.0f})"
                )
            return (
                f"{df}\n"
                f"{'\n'.join(overrides)}\n\n"
                f"Total value: {total:.0f}\n"
                f"Total value by account:\n{total_by_account}\n\n"
                f"Notional value: {notional:.0f}\n"
                f"Notional value by account:\n{notional_by_account}\n\n"
                f"Margin requirement by account:\n{margin_by_account}\n\n"
                f"Cash available:\n" + "\n".join(margin_reqs)
            )
