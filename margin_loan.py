#!/usr/bin/env python3
"""Calculate the maximum balance on pledged asset line given a monthly payment."""

import io
import subprocess
from typing import NamedTuple, Optional

import pandas as pd

import common
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


def find_loan_brokerage(broker: str) -> Optional[LoanBrokerage]:
    for brokerage in LOAN_BROKERAGES:
        if brokerage.name == broker:
            return brokerage
    return None


def get_loan_brokerage(broker: LoanBrokerage) -> Optional[LoanBrokerage]:
    for brokerage in LOAN_BROKERAGES:
        if brokerage.name == broker.name:
            return brokerage
    return None


def get_balances_broker(
    broker: LoanBrokerage, options_value_by_brokerage: dict[str, float]
) -> Optional[pd.DataFrame]:
    if (brokerage := get_loan_brokerage(broker)) is None:
        return None
    loan_df = load_loan_balance_df(brokerage)
    equity_df = load_ledger_equity_balance_df(brokerage)
    if options_value := options_value_by_brokerage.get(brokerage.name):
        equity_df.iloc[-1, equity_df.columns.get_loc("Equity Balance")] += (  # type: ignore
            options_value
        )
    equity_df["30% Equity Balance"] = equity_df["Equity Balance"] * 0.3
    equity_df["50% Equity Balance"] = equity_df["Equity Balance"] * 0.5
    equity_df["Loan Balance"] = loan_df.iloc[-1]["Loan Balance"]
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
    loan_balance_df.loc[loan_balance_df["Loan Balance"] > 0, "Loan Balance"] = 0
    return loan_balance_df


def main():
    """Main."""
    if (options_data := stock_options.get_options_data()) is None:
        raise ValueError("No options data available")
    for brokerage in LOAN_BROKERAGES:
        if (
            df := get_balances_broker(
                brokerage, options_data.opts.options_value_by_brokerage
            )
        ) is not None:
            print(brokerage.name, "\n", df.round(2), "\n")


if __name__ == "__main__":
    main()
