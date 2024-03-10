#!/usr/bin/env python3
"""Calculate ETF values."""

import functools
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
    "US_EQUITIES": 60,  # US equities, split up into US_EQUITIES_ALLOCATION
    "INTERNATIONAL_EQUITIES": 40,  # International equities
    "US_BONDS": 0,  # Bonds/Fixed Income, replaced with (age - 15)
    "COMMODITIES": 7,  # Bonds are further reduced by this to make room
}
ETF_TYPE_MAP = {
    "US_SMALL_CAP": ["SCHA"],
    "US_LARGE_CAP": ["SCHX"],
    "US_BONDS": ["SCHO", "SCHR", "SCHZ", "SWAGX"],
    "INTERNATIONAL_EQUITIES": ["SCHE", "SCHF", "SWISX"],
}
# These get expanded out into US_SMALL_CAP and US_LARGE_CAP according to US_EQUITIES_ALLOCATION.
TOTAL_MARKET_FUNDS = ["SWTSX", "SCHB"]


def reconcile(etfs_df, amount, total):
    """Add reconciliation column."""
    etfs_df["usd_to_reconcile"] = (amount * (etfs_df["wanted_percent"] / 100)) + (
        ((etfs_df["wanted_percent"] / 100) * total) - etfs_df["value"]
    )
    return etfs_df.round(2)


@functools.cache
def get_swtsx_market_cap():
    """Get market cap distribution from swtsx_market_cap DB table."""
    return common.read_sql_table_resampled_last("swtsx_market_cap").iloc[-1]


def age_adjustment(allocation):
    """Make bond adjustment based on age (age - 15)."""
    allocation = allocation.copy()
    age_in_days = (date.today() - BIRTHDAY).days
    wanted_bonds = (age_in_days / 365) - 15
    allocation["US_BONDS"] = wanted_bonds - allocation["COMMODITIES"]
    remaining = (100 - wanted_bonds) / 100
    for market_cap, market_cap_allocation in get_swtsx_market_cap().items():
        allocation[market_cap] = (
            allocation["US_EQUITIES"] * remaining * (market_cap_allocation / 100)
        )
    allocation["INTERNATIONAL_EQUITIES"] *= remaining
    del allocation["US_EQUITIES"]
    return allocation


def convert_ira_to_mutual_funds(ira_df):
    """Convert SWYGX to mutual funds in 3-fund portfolio."""
    holdings = common.read_sql_table_resampled_last("swygx_holdings").iloc[-1]
    for etf_type, etfs in ETF_TYPE_MAP.items():
        ira_df.loc[etf_type] = (
            ira_df.loc["SWYGX"].value
            * holdings[holdings.index.intersection(etfs)].sum()
            / 100
        )
    return ira_df.loc[ETF_TYPE_MAP.keys()]


def convert_etfs_to_mutual_funds(etfs_df):
    """Convert ETFs to mutual funds in 3-fund portfolio."""
    for etf_type, etfs in ETF_TYPE_MAP.items():
        etfs_df.loc[etf_type] = sum(
            etfs_df.loc[etfs_df.index.intersection(etfs)]["value"].fillna(0)
        )

    # Expand total market funds into allocation.
    for etf in TOTAL_MARKET_FUNDS:
        if etf not in etfs_df.index:
            continue
        for market_cap, market_cap_allocation in get_swtsx_market_cap().items():
            etfs_df.loc[market_cap] += etfs_df.loc[etf].fillna(0) * (
                market_cap_allocation / 100
            )
    return etfs_df.loc[ETF_TYPE_MAP.keys()]


def get_desired_df(amount):
    """Get dataframe, cost to get to desired allocation."""
    desired_allocation = age_adjustment(DESIRED_ALLOCATION)
    if (s := round(sum(desired_allocation.values()))) != 100:
        print(f"Sum of percents {s} != 100")
        return

    etfs_df = pd.read_csv(ETFS_PATH, index_col=0, usecols=["ticker", "value"]).fillna(0)
    ira_df = pd.read_csv(IRA_PATH, index_col=0, usecols=["ticker", "value"]).fillna(0)
    commodities_df = (
        pd.read_csv(COMMODITIES_PATH, index_col=0, usecols=["commodity", "value"])
        .rename_axis("ticker")
        .dropna()
    ).fillna(0)
    wanted_df = pd.DataFrame({"wanted_percent": pd.Series(desired_allocation)})
    mf_df = convert_etfs_to_mutual_funds(etfs_df) + convert_ira_to_mutual_funds(ira_df)
    mf_df.loc["COMMODITIES"] = commodities_df.sum()
    total = mf_df["value"].sum()
    mf_df["current_percent"] = (mf_df["value"] / total) * 100
    mf_df = mf_df.join(wanted_df, how="outer").fillna(0).sort_index()
    return reconcile(mf_df, amount, total)


def get_buy_only_df(allocation_df, amount):
    """Get an allocation dataframe that only involves buying and not selling.

    See https://arxiv.org/pdf/2305.12274.pdf. This is the l1 adjustment.
    """
    if len(allocation_df[allocation_df["usd_to_reconcile"] < 0]) == 0:
        return allocation_df
    clipped_df = allocation_df.clip(lower=0)
    allocation_df["buy_only"] = clipped_df["usd_to_reconcile"] * (
        amount / clipped_df["usd_to_reconcile"].sum()
    )
    allocation_df["percent_after_buy_only"] = (
        (allocation_df["value"] + allocation_df["buy_only"])
        / (allocation_df["value"].sum() + allocation_df["buy_only"].sum())
    ) * 100
    return allocation_df.round(2)


def main():
    """Main."""
    amount = 0
    if len(sys.argv) > 1:
        amount = float(sys.argv[1])
    allocation_df = get_desired_df(amount)
    if amount > 0:
        allocation_df = get_buy_only_df(allocation_df, amount)
    print(allocation_df)


if __name__ == "__main__":
    main()
