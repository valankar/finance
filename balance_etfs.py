#!/usr/bin/env python3
"""Calculate ETF values."""

import sys

import pandas as pd

import common

ETFS_PATH = common.PREFIX + "etfs_values.csv"
DESIRED_PERCENT = (
    ("SCHA", 15),
    ("SCHB", 15),
    ("SCHE", 5),
    ("SCHF", 15),
    ("SCHO", 10),
    ("SCHX", 30),
    ("SCHZ", 10),
)


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


def main():
    """Main."""
    amount = 0
    if len(sys.argv) > 1:
        amount = float(sys.argv[1])
    percents = [x[1] for x in DESIRED_PERCENT]
    if sum(percents) != 100:
        print("Sum of percents != 100")
        return

    etfs_df = pd.read_csv(ETFS_PATH, index_col=0)
    data = {
        "wanted_percent": pd.Series(
            percents,
            index=[x[0] for x in DESIRED_PERCENT],
        )
    }
    total = etfs_df["value"].sum()
    etfs_df["current_percent"] = (etfs_df["value"] / total) * 100
    # ETFs that don't exist in DESIRED_PERCENT get a default of 0.
    etfs_df = etfs_df.join(pd.DataFrame(data), how="outer").fillna(0)

    etfs_df, cost = trade(etfs_df, amount, amount, total)
    print(etfs_df)
    print(f"Sum of trades: {round(cost, 2)}")


if __name__ == "__main__":
    main()
