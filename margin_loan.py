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
import stock_options


class LoanBrokerage(NamedTuple):
    name: str
    loan_balance_cmd: str


LOAN_BROKERAGES = (
    LoanBrokerage(
        name=(broker_name := "Interactive Brokers"),
        loan_balance_cmd=f"{common.LEDGER_CURRENCIES_OPTIONS_CMD} -J -E bal ^Assets:Investments:'{broker_name}'$",
    ),
    LoanBrokerage(
        name=(broker_name := "Charles Schwab Brokerage"),
        loan_balance_cmd=f"{common.LEDGER_CURRENCIES_OPTIONS_CMD} -J -E bal ^Assets:Investments:'{broker_name}'$",
    ),
    LoanBrokerage(
        name=(broker_name := "Charles Schwab PAL Brokerage"),
        loan_balance_cmd=f"{common.LEDGER_CURRENCIES_OPTIONS_CMD} -J -E bal ^Liabilities:'Charles Schwab PAL'$",
    ),
)


def find_loan_brokerage(broker: str) -> LoanBrokerage:
    for brokerage in LOAN_BROKERAGES:
        if brokerage.name == broker:
            return brokerage
    raise ValueError(f"Brokerage {broker} not found")


def get_balances_all() -> pd.DataFrame:
    dfs = []
    for broker in LOAN_BROKERAGES:
        df = get_balances_broker(broker)
        dfs.append(df)
    df = (
        pd.concat(dfs, axis=0, ignore_index=True)
        .select_dtypes("number")
        .cumsum()
        .tail(1)
    )
    retirement = common.read_sql_last("history")["total_retirement"].iloc[-1]
    df["Equity Balance"] += retirement
    df["Total"] += retirement
    df["Leverage Ratio"] = df["Equity Balance"] / df["Total"]
    return df


@common.walrus_db.cache.cached(key_fn=lambda a, _: a[0].name)
def get_balances_broker(broker: LoanBrokerage) -> pd.DataFrame:
    loan_df = load_loan_balance_df(broker)
    equity_df = load_ledger_equity_balance_df(broker)
    notional_value = equity_df["Equity Balance"].sum()
    cash_balance = loan_df["Loan Balance"].sum()
    portfolio_equity = notional_value + cash_balance
    if not (od := stock_options.get_options_data()):
        raise ValueError("Could not get options data")
    if options_value := od.opts.options_value_by_brokerage.get(broker.name):
        logger.info(f"Options value for {broker.name}: {options_value.value:.0f}")
        logger.info(
            f"Options notional value for {broker.name}: {options_value.notional_value:.0f}"
        )
        notional_value += options_value.notional_value
        portfolio_equity += options_value.value
    futures_df = futures.Futures().futures_df
    try:
        futures_value = futures_df.xs(broker.name, level="account")["value"].sum()
        futures_notional_value = futures_df.xs(broker.name, level="account")[
            "notional_value"
        ].sum()
        logger.info(f"Futures value for {broker.name}: {futures_value:.0f}")
        logger.info(
            f"Futures notional value for {broker.name}: {futures_notional_value:.0f}"
        )
        notional_value += futures_notional_value
        portfolio_equity += futures_value
        cash_balance += futures_value
    except KeyError:
        pass
    logger.info(f"Cash balance for {broker.name}: {cash_balance:.0f}")
    logger.info(f"Portfolio notional value for {broker.name}: {notional_value:.0f}")
    logger.info(f"Portfolio equity for {broker.name}: {portfolio_equity:.0f}")
    equity_df["Cash Balance"] = cash_balance
    equity_df["Equity Balance"] = notional_value
    equity_df["Leverage Ratio"] = notional_value / portfolio_equity
    equity_df["Loan Balance"] = portfolio_equity - (
        equity_df["Leverage Ratio"] * portfolio_equity
    )
    equity_df["Total"] = portfolio_equity
    return equity_df


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
    for brokerage in LOAN_BROKERAGES:
        df = get_balances_broker(brokerage)
        print(brokerage.name, "\n", df.round(2), "\n")
    df = get_balances_all()
    print("Overall", "\n", df.round(2), "\n")


if __name__ == "__main__":
    main()
