#!/usr/bin/env python3
"""Compare margin interest between CHF and USD."""

import common
import margin_loan


def chf_interest_as_percentage_of_usd():
    """Determine CHF interest paid as a percentage of if USD interest were paid."""
    ibkr_rates_df = common.read_sql_last("interactive_brokers_margin_rates")
    return ibkr_rates_df.iloc[-1]["CHF"] / ibkr_rates_df.iloc[-1]["USD"]


def interest_comparison_df():
    """Get a monthly interest comparison dataframe."""
    balance_df = (
        margin_loan.load_loan_balance_df(
            ledger_loan_balance_cmd=margin_loan.LEDGER_LOAN_BALANCE_CHF,
        )
        .resample("D")
        .last()
        .ffill()
    )

    ibkr_rates_df = common.read_sql_table("interactive_brokers_margin_rates")
    forex_df = common.read_sql_table("forex")[["CHFUSD"]]
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
    dataframe = interest_comparison_df()
    print("Monthly:")
    print(dataframe)
    print("\nCumulative:")
    print(dataframe.cumsum())
    print(
        "\nCost of CHF loan as percentage of USD loan: "
        + f"{chf_interest_as_percentage_of_usd()*100:.2f}%"
    )
