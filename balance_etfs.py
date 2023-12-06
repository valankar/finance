#!/usr/bin/env python3
"""Calculate ETF values."""

import sys
from datetime import date

import pandas as pd

import common

ETFS_PATH = f"{common.PREFIX}schwab_etfs_values.csv"
IRA_PATH = f"{common.PREFIX}schwab_ira_values.csv"
COMMODITIES_PATH = f"{common.PREFIX}commodities_values.csv"
BIRTHDAY = date(1975, 2, 28)
# Modeled from:
# https://www.morningstar.com/etfs/arcx/vt/portfolio
DESIRED_ALLOCATION = {
    "SWTSX": 60,  # US equities
    "SWISX": 40,  # International equities
    "SWAGX": 0,  # Bonds/Fixed Income, replaced with (age - 15)
    "COMMODITIES": 10,  # Bonds are further reduced by this to make room
}
# Conversion of SWYGX to mutual fund allocation. Update with data from:
# https://www.morningstar.com/funds/xnas/swygx/portfolio
# Last updated: 2023-11-22
IRA_CURRENT_ALLOCATION = {
    "SWTSX": 56.77,  # US equities
    "SWISX": 23.87,  # International equities
    "SWAGX": 17.25,  # Bonds/Fixed Income
}


def trade(etfs_df, amount, original_amount, total):
    """Simulate a trade."""
    etfs_df["usd_to_reconcile"] = (amount * (etfs_df["wanted_percent"] / 100)) + (
        ((etfs_df["wanted_percent"] / 100) * total) - etfs_df["value"]
    )
    etfs_df["shares_to_trade"] = etfs_df["usd_to_reconcile"] / etfs_df["current_price"]
    etfs_df = etfs_df.round(2)
    # Can't buy or sell fractional shares.
    etfs_df["shares_to_trade"] = etfs_df["shares_to_trade"].round(0)
    # When percent is 0, it means to sell everything. In this case, fractional
    # share selling is allowed.
    etfs_df.loc[(etfs_df["wanted_percent"] == 0), "shares_to_trade"] = -etfs_df[
        "shares"
    ]
    cost = (etfs_df["shares_to_trade"] * etfs_df["current_price"]).sum()
    if round(cost, 0) > original_amount:
        return trade(etfs_df, amount - 1, original_amount, total)
    return (etfs_df, cost)


def age_adjustment(allocation):
    """Make bond adjustment based on age (age - 15)."""
    allocation = allocation.copy()
    age_in_days = (date.today() - BIRTHDAY).days
    wanted_bonds = (age_in_days / 365) - 15
    allocation["SWAGX"] = wanted_bonds - allocation["COMMODITIES"]
    remaining = (100 - wanted_bonds) / 100
    allocation["SWTSX"] *= remaining
    allocation["SWISX"] *= remaining
    return allocation


def convert_ira_to_mutual_funds(ira_df):
    """Convert SWYGX to mutual funds in 3-fund portfolio."""
    # Equivalent ETFs
    ira_df.loc["SWTSX"] = (
        0,
        0,
        ira_df.loc["SWYGX"].value * IRA_CURRENT_ALLOCATION["SWTSX"] / 100,
    )
    ira_df.loc["SWISX"] = (
        0,
        0,
        ira_df.loc["SWYGX"].value * IRA_CURRENT_ALLOCATION["SWISX"] / 100,
    )
    ira_df.loc["SWAGX"] = (
        0,
        0,
        ira_df.loc["SWYGX"].value * IRA_CURRENT_ALLOCATION["SWAGX"] / 100,
    )
    return ira_df.loc[["SWTSX", "SWISX", "SWAGX"]]


def convert_etfs_to_mutual_funds(etfs_df):
    """Convert ETFs to mutual funds in 3-fund portfolio."""
    # Equivalent ETFs
    etfs_df.loc["SWTSX"] = (
        0,
        etfs_df.loc["SWTSX"].current_price,
        sum(etfs_df.loc[["SCHA", "SCHX", "SWTSX"]]["value"].fillna(0)),
    )
    etfs_df.loc["SWISX"] = (
        0,
        etfs_df.loc["SWISX"].current_price,
        sum(etfs_df.loc[["SCHF", "SWISX"]]["value"].fillna(0)),
    )
    etfs_df.loc["SWAGX"] = (
        0,
        etfs_df.loc["SWAGX"].current_price,
        sum(etfs_df.loc[["SCHR", "SCHZ", "SWAGX"]]["value"].fillna(0)),
    )
    return etfs_df.loc[["SWTSX", "SWISX", "SWAGX"]]


def get_desired_df(amount):
    """Get dataframe, cost to get to desired allocation."""
    desired_allocation = age_adjustment(DESIRED_ALLOCATION)
    if (s := round(sum(desired_allocation.values()))) != 100:
        print(f"Sum of percents {s} != 100")
        return

    etfs_df = pd.read_csv(ETFS_PATH, index_col=0)
    ira_df = pd.read_csv(IRA_PATH, index_col=0)
    commodities_df = (
        pd.read_csv(COMMODITIES_PATH, index_col=0)
        .rename_axis("ticker")
        .rename(columns={"troy_oz": "shares"})
    )
    wanted_df = pd.DataFrame({"wanted_percent": pd.Series(desired_allocation)})
    mf_df = convert_etfs_to_mutual_funds(etfs_df) + convert_ira_to_mutual_funds(ira_df)
    mf_df.loc["COMMODITIES"] = commodities_df.loc["GOLD"] + commodities_df.loc["SILVER"]
    mf_df.loc["COMMODITIES"]["current_price"] = 1
    total = mf_df["value"].sum()
    mf_df["current_percent"] = (mf_df["value"] / total) * 100
    mf_df["shares"] = mf_df["value"] / mf_df["current_price"]
    mf_df = mf_df.join(wanted_df, how="outer").fillna(0).sort_index()
    return trade(mf_df, amount, amount, total)


def main():
    """Main."""
    amount = 0
    if len(sys.argv) > 1:
        amount = float(sys.argv[1])

    mf_df, cost = get_desired_df(amount)
    print(mf_df)
    print(f"Sum of trades: {round(cost, 2)}")


if __name__ == "__main__":
    main()
