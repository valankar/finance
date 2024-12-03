#!/usr/bin/env python3
"""Calculate the maximum balance on pledged asset line given a monthly payment."""

import io
import subprocess
from typing import Callable

import pandas as pd

import common
import ledger_amounts
import stock_options

LEDGER_LOAN_BALANCE_HISTORY_IBKR = (
    f"{common.LEDGER_PREFIX} "
    + r"""--limit 'commodity=~/^(\\$|CHF|"SPX)/' -J -E reg ^Assets:Investments:'Interactive Brokers'"""
)
LEDGER_BALANCE_HISTORY_IBKR = (
    f"{common.LEDGER_PREFIX} "
    + f"""--limit 'commodity=~/^{ledger_amounts.ETFS_REGEX}/' -J -E reg ^Assets:Investments:'Interactive Brokers'"""
)
LEDGER_LOAN_BALANCE_CHF = (
    f"{common.LEDGER_BIN} -f {common.LEDGER_DAT} -c "
    + r"""--limit 'commodity=~/^CHF$/' -J -E reg ^Assets:Investments:'Interactive Brokers'"""
)
LEDGER_BALANCE_HISTORY_SCHWAB_NONPAL = (
    f"{common.LEDGER_PREFIX} "
    + f"""--limit 'commodity=~/^{ledger_amounts.ETFS_REGEX}|^SWVXX/' -J -E reg ^Assets:Investments:'Charles Schwab Brokerage'"""
)
LEDGER_LOAN_BALANCE_HISTORY_SCHWAB_NONPAL = (
    f"{common.LEDGER_PREFIX} "
    + r"""--limit 'commodity=~/^(\\$|"SPX)/' -J -E -b 2024-03-26 reg ^Assets:Investments:'Charles Schwab Brokerage'"""
)


def get_options_value(broker: str) -> float:
    try:
        options_df = stock_options.options_df_with_value().loc[broker]
        options_value = options_df[
            options_df["ticker"].str.match(ledger_amounts.ETFS_REGEX)
        ]["value"].sum()
        return options_value
    except KeyError:
        return 0


def get_balances_broker(
    broker: str, loan_balance_cmd: str, balance_cmd: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    loan_df = load_loan_balance_df(loan_balance_cmd)
    equity_df = load_ledger_equity_balance_df(balance_cmd)
    equity_df.iloc[-1, equity_df.columns.get_loc("Equity Balance")] += (  # type: ignore
        get_options_value(broker)
    )
    return loan_df, equity_df


def get_balances_ibkr() -> tuple[pd.DataFrame, pd.DataFrame]:
    return get_balances_broker(
        "Interactive Brokers",
        LEDGER_LOAN_BALANCE_HISTORY_IBKR,
        LEDGER_BALANCE_HISTORY_IBKR,
    )


def get_balances_schwab_nonpal() -> tuple[pd.DataFrame, pd.DataFrame]:
    return get_balances_broker(
        "Charles Schwab Brokerage",
        LEDGER_LOAN_BALANCE_HISTORY_SCHWAB_NONPAL,
        LEDGER_BALANCE_HISTORY_SCHWAB_NONPAL,
    )


def load_ledger_equity_balance_df(ledger_balance_cmd: str) -> pd.DataFrame:
    """Get dataframe of equity balance."""
    equity_balance_df = pd.read_csv(
        io.StringIO(subprocess.check_output(ledger_balance_cmd, shell=True, text=True)),
        sep=" ",
        index_col=0,
        parse_dates=True,
        names=["date", "Equity Balance"],
    )
    equity_balance_latest_df = pd.read_csv(
        io.StringIO(
            subprocess.check_output(
                ledger_balance_cmd.replace(" reg ", " bal "), shell=True, text=True
            )
        ),
        sep=" ",
        index_col=0,
        parse_dates=True,
        names=["date", "Equity Balance"],
    )
    equity_balance_df = pd.concat([equity_balance_df, equity_balance_latest_df])
    equity_balance_df["30% Equity Balance"] = equity_balance_df["Equity Balance"] * 0.3
    equity_balance_df["50% Equity Balance"] = equity_balance_df["Equity Balance"] * 0.5
    return equity_balance_df


def load_loan_balance_df(ledger_loan_balance_cmd: str) -> pd.DataFrame:
    """Get dataframe of margin loan balance."""
    loan_balance_df = pd.read_csv(
        io.StringIO(
            subprocess.check_output(ledger_loan_balance_cmd, shell=True, text=True)
        ),
        sep=" ",
        index_col=0,
        parse_dates=True,
        names=["date", "Loan Balance"],
    )
    loan_balance_latest_df = pd.read_csv(
        io.StringIO(
            subprocess.check_output(
                ledger_loan_balance_cmd.replace(" reg ", " bal "), shell=True, text=True
            )
        ),
        sep=" ",
        index_col=0,
        parse_dates=True,
        names=["date", "Loan Balance"],
    )
    loan_balance_df = pd.concat([loan_balance_df, loan_balance_latest_df])
    loan_balance_df.loc[loan_balance_df["Loan Balance"] > 0, "Loan Balance"] = 0
    return loan_balance_df


def display_loan(
    title: str, get_balances: Callable[[], tuple[pd.DataFrame, pd.DataFrame]]
):
    loan_balance_df, equity_balance_df = get_balances()
    new_df = equity_balance_df.iloc[-1].copy()
    new_df["Loan Balance"] = loan_balance_df.iloc[-1]["Loan Balance"]
    new_df["Distance to 30%"] = new_df["Loan Balance"] + new_df["30% Equity Balance"]
    new_df["Distance to 50%"] = new_df["Loan Balance"] + new_df["50% Equity Balance"]
    print(title)
    print(new_df)


def main():
    """Main."""
    display_loan("Interactive Brokers", get_balances_ibkr)
    print()
    display_loan("Charles Schwab", get_balances_schwab_nonpal)


if __name__ == "__main__":
    main()
