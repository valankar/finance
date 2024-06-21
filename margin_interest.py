#!/usr/bin/env python3
"""Compare margin interest between CHF and USD."""

import common
import plot

LEDGER_LOAN_BALANCE_CHF = (
    f"{common.LEDGER_BIN} -f {common.LEDGER_DAT} -c "
    + r"""--limit 'commodity=~/^CHF$/' -J -E reg ^Assets:Investments:'Interactive Brokers'"""
)


def interest_comparison_df():
    """Get a monthly interest comparison dataframe."""
    balance_df = (
        plot.load_loan_balance_df(
            ledger_loan_balance_cmd=LEDGER_LOAN_BALANCE_CHF,
        )
        .resample("D")
        .last()
        .ffill()
    )

    ibkr_rates_df = common.read_sql_table_resampled_last(
        "interactive_brokers_margin_rates"
    )
    forex_df = common.read_sql_table_resampled_last("forex")[["CHFUSD"]]
    merged_df = common.reduce_merge_asof([balance_df, ibkr_rates_df, forex_df]).dropna()

    merged_df["CHF Interest"] = merged_df["Loan Balance"] * (
        merged_df["CHF"] / 100 / 365
    )
    merged_df["CHF Interest in USD"] = merged_df["CHF Interest"] * merged_df["CHFUSD"]
    merged_df["USD Interest"] = (merged_df["Loan Balance"] * merged_df["CHFUSD"]) * (
        merged_df["USD"] / 100 / 365
    )

    return merged_df[["CHF Interest in USD", "USD Interest"]].resample("ME").sum()


if __name__ == "__main__":
    print(interest_comparison_df())
