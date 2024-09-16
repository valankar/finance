#!/usr/bin/env python3
"""Calculate ETF values."""

import functools
import sys
from datetime import date

import pandas as pd

import common
import etfs
import schwab_ira

BIRTHDAY = date(1975, 2, 28)
# Modeled from:
# https://www.morningstar.com/etfs/arcx/vt/portfolio
DESIRED_ALLOCATION = {
    "US_EQUITIES": 60,  # US equities, split up into US_SMALL_CAP and US_LARGE_CAP.
    "INTERNATIONAL_EQUITIES": 40,  # International equities
    "US_BONDS": 0,  # Bonds/Fixed Income, replaced with (age - 15)
    "COMMODITIES": 7,  # Bonds are further reduced by this to make room
}
ETF_TYPE_MAP = {
    "COMMODITIES": ["GLDM", "SGOL", "SIVR"],
    "US_SMALL_CAP": ["SCHA"],
    "US_LARGE_CAP": ["SCHX"],
    "US_BONDS": ["SCHO", "SCHR", "SCHZ", "SWAGX"],
    "INTERNATIONAL_EQUITIES": ["SCHE", "SCHF", "SWISX"],
}
# These get expanded out into US_SMALL_CAP and US_LARGE_CAP according to allocation
# of SWTSX.
TOTAL_MARKET_FUNDS = ["SWTSX", "SCHB"]


def reconcile(etfs_df, amount, total):
    """Add reconciliation column."""
    etfs_df["diff_percent"] = etfs_df["wanted_percent"] - etfs_df["current_percent"]
    etfs_df["usd_to_reconcile"] = (amount * (etfs_df["wanted_percent"] / 100)) + (
        ((etfs_df["wanted_percent"] / 100) * total) - etfs_df["value"]
    )
    return etfs_df.round(2)


@functools.cache
def get_swtsx_market_cap():
    """Get market cap distribution from swtsx_market_cap DB table."""
    return common.read_sql_table("swtsx_market_cap").iloc[-1]


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


def convert_ira_to_types(ira_df):
    """Convert SWYGX to types/categories."""
    holdings = common.read_sql_table("swygx_holdings").iloc[-1]
    for etf_type, etfs_list in ETF_TYPE_MAP.items():
        ira_df.loc[etf_type] = (
            ira_df.loc["SWYGX"].value
            * holdings[holdings.index.intersection(etfs_list)].sum()
            / 100
        )
    return ira_df.loc[ETF_TYPE_MAP.keys()]


def convert_etfs_to_types(etfs_df):
    """Convert ETFs to types/categories."""
    for etf_type, etfs_list in ETF_TYPE_MAP.items():
        etfs_df.loc[etf_type] = sum(
            etfs_df.loc[etfs_df.index.intersection(etfs_list)]["value"].fillna(0)
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
        return None

    etfs_df = pd.read_csv(
        etfs.CSV_OUTPUT_PATH, index_col=0, usecols=["ticker", "value"]
    ).fillna(0)
    ira_df = pd.read_csv(
        schwab_ira.CSV_OUTPUT_PATH, index_col=0, usecols=["ticker", "value"]
    ).fillna(0)
    wanted_df = pd.DataFrame({"wanted_percent": pd.Series(desired_allocation)})
    mf_df = convert_etfs_to_types(etfs_df) + convert_ira_to_types(ira_df)
    total = mf_df["value"].sum()
    mf_df["current_percent"] = (mf_df["value"] / total) * 100
    mf_df = mf_df.join(wanted_df, how="outer").fillna(0).sort_index()
    return reconcile(mf_df, amount, total)


def get_common_only_df(allocation_df, clipped_df, amount, xact):
    """Common function for only buying or selling.

    See https://arxiv.org/pdf/2305.12274.pdf. This is the l1 adjustment.
    """
    allocation_df[f"{xact}_only"] = clipped_df["usd_to_reconcile"] * (
        amount / clipped_df["usd_to_reconcile"].sum()
    )
    allocation_df[f"percent_after_{xact}_only"] = (
        (allocation_df["value"] + allocation_df[f"{xact}_only"])
        / (allocation_df["value"].sum() + allocation_df[f"{xact}_only"].sum())
    ) * 100
    return allocation_df.round(2)


def get_buy_only_df(allocation_df, amount):
    """Get an allocation dataframe that only involves buying and not selling."""
    if len(allocation_df[allocation_df["usd_to_reconcile"] < 0]) == 0:
        return allocation_df
    return get_common_only_df(allocation_df, allocation_df.clip(lower=0), amount, "buy")


def get_sell_only_df(allocation_df, amount):
    """Get an allocation dataframe that only involves selling and not buying."""
    if len(allocation_df[allocation_df["usd_to_reconcile"] > 0]) == 0:
        return allocation_df
    return get_common_only_df(
        allocation_df, allocation_df.clip(upper=0), amount, "sell"
    )


def get_rebalancing_df(amount):
    """Get rebalancing dataframe."""
    try:
        amount = float(amount)
    except ValueError:
        amount = 0
    allocation_df = get_desired_df(amount)
    if amount > 0:
        allocation_df = get_buy_only_df(allocation_df, amount)
    elif amount < 0:
        allocation_df = get_sell_only_df(allocation_df, amount)
    return allocation_df


def main():
    """Main."""
    amount = 0
    if len(sys.argv) > 1:
        amount = sys.argv[1]
    print(get_rebalancing_df(amount))


if __name__ == "__main__":
    main()
