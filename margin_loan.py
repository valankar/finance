#!/usr/bin/env python3
"""Calculate the maximum balance on pledged asset line given a monthly payment."""

import io
import subprocess
from typing import NamedTuple

import pandas as pd
from loguru import logger

import common
import futures
import ledger_amounts
import stock_options


class LoanBrokerage(NamedTuple):
    name: str
    loan_balance_cmd: str
    balance_cmd: str


LOAN_BROKERAGES = (
    LoanBrokerage(
        name=(broker_name := "Interactive Brokers"),
        loan_balance_cmd=f"{common.LEDGER_CURRENCIES_OPTIONS_CMD} -J -E bal ^Assets:Investments:'{broker_name}'",
        balance_cmd=f"{common.LEDGER_PREFIX} {ledger_amounts.LEDGER_LIMIT_ETFS} -J -E bal ^Assets:Investments:'{broker_name}'",
    ),
    LoanBrokerage(
        name=(broker_name := "Charles Schwab Brokerage"),
        loan_balance_cmd=f"{common.LEDGER_CURRENCIES_OPTIONS_CMD} -J -E bal ^Assets:Investments:'{broker_name}'",
        balance_cmd=f"{common.LEDGER_PREFIX} {ledger_amounts.LEDGER_LIMIT_ETFS} -J -E bal ^Assets:Investments:'{broker_name}'",
    ),
    LoanBrokerage(
        name=(broker_name := "Charles Schwab PAL Brokerage"),
        loan_balance_cmd=f"{common.LEDGER_CURRENCIES_OPTIONS_CMD} -J -E bal ^Liabilities:'Charles Schwab PAL'",
        balance_cmd=f"{common.LEDGER_PREFIX} {ledger_amounts.LEDGER_LIMIT_ETFS} -J -E bal ^Assets:Investments:'{broker_name}'",
    ),
)


def find_loan_brokerage(broker: str) -> LoanBrokerage:
    for brokerage in LOAN_BROKERAGES:
        if brokerage.name == broker:
            return brokerage
    raise ValueError(f"Brokerage {broker} not found")


@common.walrus_db.cache.cached()
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
    except KeyError:
        pass
    logger.info(f"Cash balance for for {broker.name}: {cash_balance:.0f}")
    logger.info(f"Portfolio notional value for {broker.name}: {notional_value:.0f}")
    logger.info(f"Portfolio equity for {broker.name}: {portfolio_equity:.0f}")
    equity_df["Equity Balance"] = notional_value
    equity_df["Leverage Ratio"] = notional_value / portfolio_equity
    equity_df["30% Equity Balance"] = equity_df["Equity Balance"] * 0.3
    equity_df["50% Equity Balance"] = equity_df["Equity Balance"] * 0.5
    equity_df["Loan Balance"] = portfolio_equity - (
        equity_df["Leverage Ratio"] * portfolio_equity
    )
    equity_df["Total"] = equity_df["Equity Balance"] + equity_df["Loan Balance"]
    equity_df["Distance to 30%"] = (
        equity_df["Loan Balance"] + equity_df["30% Equity Balance"]
    )
    equity_df["Distance to 50%"] = (
        equity_df["Loan Balance"] + equity_df["50% Equity Balance"]
    )
    return equity_df


def load_ledger_equity_balance_df(brokerage: LoanBrokerage) -> pd.DataFrame:
    """Get dataframe of equity balance."""
    equity_balance_df = pd.read_csv(
        io.StringIO(
            subprocess.check_output(brokerage.balance_cmd, shell=True, text=True)
        ),
        sep=" ",
        index_col=0,
        parse_dates=True,
        names=["date", "Equity Balance"],
    )
    return equity_balance_df


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
    return loan_balance_df


def main():
    """Main."""
    for brokerage in LOAN_BROKERAGES:
        df = get_balances_broker(brokerage)
        print(brokerage.name, "\n", df.round(2), "\n")


if __name__ == "__main__":
    main()
