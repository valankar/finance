#!/usr/bin/env python3
"""Calculate ETF values."""

import sys
from datetime import date

import pandas as pd

import common

ETFS_PATH = f"{common.PREFIX}schwab_etfs_values.csv"
DESIRED_PERCENT = {
    # Large cap
    "SCHX": 47,
    # Small cap
    "SCHA": 16,
    # International
    "SCHF": 21,
    # Fixed income
    "SCHR": 16,
}
BIRTHDAY = date(1975, 2, 28)


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


def fill_unknown_prices(etfs_df):
    """Get prices for tickers if they are unknown."""
    unknown_tickers = list(etfs_df[etfs_df["current_price"] == 0].index.unique())
    prices = common.get_tickers(unknown_tickers)
    for ticker, price in prices.items():
        etfs_df.loc[ticker, "current_price"] = price
        etfs_df.loc[ticker, "value"] = price * etfs_df.loc[ticker, "shares"]
    return etfs_df


def age_adjustment(allocation):
    """Make bond adjustment based on age."""
    age_in_days = (date.today() - BIRTHDAY).days
    current_bonds = allocation["SCHR"]
    wanted_bonds = (age_in_days / 365) - 32
    diff_bonds = current_bonds - wanted_bonds
    if current_bonds == wanted_bonds:
        return allocation
    diff = diff_bonds / (len(allocation) - 1)
    new_allocation = {}
    for ticker, percent in allocation.items():
        if ticker == "SCHR":
            new_allocation[ticker] = wanted_bonds
        else:
            new_allocation[ticker] = percent + diff
    return new_allocation


def main():
    """Main."""
    amount = 0
    if len(sys.argv) > 1:
        amount = float(sys.argv[1])
    desired_allocation = age_adjustment(DESIRED_PERCENT)

    if sum(desired_allocation.values()) != 100:
        print("Sum of percents != 100")
        return

    etfs_df = pd.read_csv(ETFS_PATH, index_col=0)
    wanted_df = pd.DataFrame({"wanted_percent": pd.Series(desired_allocation)})
    total = etfs_df["value"].sum()
    etfs_df["current_percent"] = (etfs_df["value"] / total) * 100
    # ETFs that don't exist in desired_allocation get a default of 0.
    etfs_df = etfs_df.join(wanted_df, how="outer").fillna(0).sort_index()
    etfs_df = fill_unknown_prices(etfs_df)

    etfs_df, cost = trade(etfs_df, amount, amount, total)
    print(etfs_df)
    print(f"Sum of trades: {round(cost, 2)}")


if __name__ == "__main__":
    main()
