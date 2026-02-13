#!/usr/bin/env python3

import io
import subprocess
from datetime import date
from typing import NamedTuple

import pandas as pd
from loguru import logger

import common
import etfs
import futures
import history
import ledger_ops
import stock_options

MONEY_MARKETS = r"^(SWVXX|CHF)$"


class LoanBrokerage(NamedTuple):
    name: str
    loan_balance_cmd: str


LOAN_BROKERAGES = (
    LoanBrokerage(
        name=common.Brokerage.IBKR,
        loan_balance_cmd=f"{common.LEDGER_CURRENCIES_CMD} -J -E bal ^Assets:Investments:'{common.Brokerage.IBKR}'$",
    ),
    LoanBrokerage(
        name=common.Brokerage.SCHWAB,
        loan_balance_cmd=f"{common.LEDGER_CURRENCIES_CMD} -J -E bal ^Assets:Investments:'{common.Brokerage.SCHWAB}'$",
    ),
    LoanBrokerage(
        name=common.Brokerage.SCHWAB_PAL,
        loan_balance_cmd=f"""{common.LEDGER_CURRENCIES_CMD} -J -E bal ^Liabilities:'{common.Brokerage.SCHWAB_PAL.rstrip(" Brokerage")}'$""",
    ),
)


def get_balances_all(b: dict[str, pd.DataFrame]) -> pd.DataFrame:
    dfs = [b[k] for k in sorted(b)]
    df = (
        pd.concat(dfs, axis=0, ignore_index=True)
        .select_dtypes("number")
        .cumsum()
        .tail(1)
    )
    retirement = ledger_ops.get_ledger_balance(history.LEDGER_RETIREMENT_CMD)
    df["Equity Balance"] += retirement
    df["Total"] += retirement
    df["Leverage Ratio"] = df["Equity Balance"] / df["Total"]
    return df


# This is used in separate graph generation processes so redis caching makes sense.
@common.walrus_db.db.lock("get_balances_broker", ttl=common.LOCK_TTL_SECONDS * 1000)
@common.walrus_db.cache.cached()
def get_balances_broker() -> dict[str, pd.DataFrame]:
    r: dict[str, pd.DataFrame] = {}
    opts = stock_options.get_options_and_spreads()
    futures_df = futures.Futures().futures_df
    for broker in LOAN_BROKERAGES:
        loan_df = load_loan_balance_df(broker)
        equity_df = load_ledger_equity_balance_df(broker)
        money_market = get_money_market_value(broker)
        notional_value = equity_df["Equity Balance"].sum()
        logger.info(f"Equity balance for {broker.name}: {notional_value:.0f}")
        cash_balance = loan_df["Loan Balance"].sum()
        logger.info(f"Cash balance for {broker.name}: {cash_balance:.0f}")
        portfolio_equity = notional_value + cash_balance
        if options_value := opts.options_value_by_brokerage.get(broker.name):
            logger.info(f"Options value for {broker.name}: {options_value.value:.0f}")
            notional_value += options_value.notional_value
            portfolio_equity += options_value.value
        try:
            futures_value = futures_df.xs(broker.name, level="account")["value"].sum()
            futures_notional_value = futures_df.xs(broker.name, level="account")[
                "notional_value"
            ].sum()
            logger.info(f"Futures value for {broker.name}: {futures_value:.0f}")
            notional_value += futures_notional_value
            portfolio_equity += futures_value
            cash_balance += futures_value
        except KeyError:
            pass
        logger.info(f"Cash + futures value for {broker.name}: {cash_balance:.0f}")
        logger.info(f"Portfolio notional value for {broker.name}: {notional_value:.0f}")
        logger.info(f"Portfolio equity for {broker.name}: {portfolio_equity:.0f}")
        equity_df["Cash Balance"] = cash_balance
        equity_df["Money Market"] = money_market
        equity_df["Real Cash"] = cash_balance - money_market
        equity_df["Equity Balance"] = notional_value
        equity_df["Leverage Ratio"] = notional_value / portfolio_equity
        equity_df["Loan Balance"] = portfolio_equity - (
            equity_df["Leverage Ratio"] * portfolio_equity
        )
        equity_df["Total"] = portfolio_equity
        r[broker.name] = equity_df
    return r


def get_money_market_value(brokerage: LoanBrokerage) -> float:
    return ledger_ops.get_ledger_balance(
        f"{common.LEDGER_PREFIX} --limit 'commodity=~/{MONEY_MARKETS}/' -J -E bal ^Assets:Investments:'{brokerage.name}'$"
    )


def load_ledger_equity_balance_df(brokerage: LoanBrokerage) -> pd.DataFrame:
    """Get dataframe of equity balance."""
    balance = etfs.get_etfs_df(brokerage.name)["value"].sum()
    return pd.DataFrame({"Equity Balance": balance}, index=[pd.Timestamp(date.today())])


def load_loan_balance_df(brokerage: LoanBrokerage) -> pd.DataFrame:
    """Get dataframe of margin loan balance."""
    loan_balance_df = pd.read_csv(
        io.StringIO(
            subprocess.check_output(brokerage.loan_balance_cmd, shell=True, text=True)
        ),
        sep=" ",
        index_col=0,
        parse_dates=True,
        names=["date", "Loan Balance"],
    )
    return loan_balance_df.tail(1)


def main():
    """Main."""
    b = get_balances_broker()
    for k in sorted(b):
        df = b[k]
        print(k, "\n", df.round(2), "\n")
    df = get_balances_all(b)
    print("Overall", "\n", df.round(2), "\n")


if __name__ == "__main__":
    main()
