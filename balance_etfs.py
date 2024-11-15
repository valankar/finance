#!/usr/bin/env python3
"""Calculate ETF values."""

import argparse
import functools
from datetime import date
from typing import Any

import pandas as pd

import common
import etfs
import schwab_ira
import stock_options

BIRTHDAY = date(1975, 2, 28)
# Modeled from:
# https://www.morningstar.com/etfs/arcx/vt/portfolio
# Last updated 10/25/2024
DESIRED_ALLOCATION = {
    "US_EQUITIES": 60,  # US equities, split up into US_SMALL_CAP and US_LARGE_CAP.
    "INTERNATIONAL_EQUITIES": 40,  # International equities
    "US_BONDS": 0,  # Bonds/Fixed Income, replaced with (age - 15)
    "COMMODITIES": 8,  # Bonds are further reduced by this to make room
}
# International equities allocation
# If this is empty, it is determined from SWYGX current holdings.
INTERNATIONAL_PERCENTAGE = {
    "DEVELOPED": 90,
    "EMERGING": 10,
}
COMMODITIES_PERCENTAGE = {
    "GOLD": 62,
    "SILVER": 5,
    "CRYPTO": 33,
}
ETF_TYPE_MAP = {
    "COMMODITIES_GOLD": ["GLDM", "SGOL"],
    "COMMODITIES_SILVER": ["SIVR"],
    "COMMODITIES_CRYPTO": ["COIN", "BITX", "MSTR"],
    "US_SMALL_CAP": ["SCHA"],
    "US_LARGE_CAP": ["SCHX"],
    "US_BONDS": ["SCHO", "SCHR", "SCHZ", "SWAGX"],
    "INTERNATIONAL_DEVELOPED": ["SCHF", "SWISX"],
    "INTERNATIONAL_EMERGING": ["SCHE"],
}
# These get expanded out into US_SMALL_CAP and US_LARGE_CAP according to allocation
# of SWTSX.
TOTAL_MARKET_FUNDS = ["SWTSX", "SCHB"]


def reconcile(etfs_df: pd.DataFrame, amount: int, total: float) -> pd.DataFrame:
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


def adjustment(allocation: dict[str, Any]) -> dict[str, Any] | None:
    """Make bond adjustment based on age (age - 15)."""
    allocation = allocation.copy()
    age_in_days = (date.today() - BIRTHDAY).days
    wanted_bonds = (age_in_days / 365) - 15
    allocation["US_BONDS"] = wanted_bonds - allocation["COMMODITIES"]
    if sum(COMMODITIES_PERCENTAGE.values()) != 100:
        print("Sum of COMMODITIES_PERCENTAGE != 100")
        return None
    allocation["COMMODITIES_GOLD"] = (
        allocation["COMMODITIES"] * COMMODITIES_PERCENTAGE["GOLD"]
    ) / 100
    allocation["COMMODITIES_SILVER"] = (
        allocation["COMMODITIES"] * COMMODITIES_PERCENTAGE["SILVER"]
    ) / 100
    allocation["COMMODITIES_CRYPTO"] = (
        allocation["COMMODITIES"] * COMMODITIES_PERCENTAGE["CRYPTO"]
    ) / 100
    del allocation["COMMODITIES"]
    remaining = (100 - wanted_bonds) / 100
    # Figure out US large vs small cap percentages.
    for (
        market_cap,
        market_cap_allocation,
    ) in get_swtsx_market_cap().items():
        allocation[str(market_cap)] = (
            allocation["US_EQUITIES"] * remaining * (market_cap_allocation / 100)
        )
    remaining *= allocation["INTERNATIONAL_EQUITIES"]
    del allocation["US_EQUITIES"]
    if len(INTERNATIONAL_PERCENTAGE):
        if sum(INTERNATIONAL_PERCENTAGE.values()) != 100:
            print("Sum of INTERNATIONAL_PERCENTAGE != 100")
            return None
        developed_allocation = INTERNATIONAL_PERCENTAGE["DEVELOPED"] / 100
        emerging_allocation = INTERNATIONAL_PERCENTAGE["EMERGING"] / 100
    else:
        swygx_holdings = common.read_sql_table("swygx_holdings").iloc[-1]
        developed_allocation = (
            swygx_holdings[
                swygx_holdings.index.intersection(
                    ETF_TYPE_MAP["INTERNATIONAL_DEVELOPED"]
                )
            ].sum()
            / 100
        )
        emerging_allocation = (
            swygx_holdings[
                swygx_holdings.index.intersection(
                    ETF_TYPE_MAP["INTERNATIONAL_EMERGING"]
                )
            ].sum()
            / 100
        )
    allocation["INTERNATIONAL_DEVELOPED"] = (
        developed_allocation / (developed_allocation + emerging_allocation)
    ) * remaining
    allocation["INTERNATIONAL_EMERGING"] = (
        emerging_allocation / (developed_allocation + emerging_allocation)
    ) * remaining
    del allocation["INTERNATIONAL_EQUITIES"]
    return allocation


def convert_ira_to_types(ira_df, etf_type_map: dict[str, list[str]]):
    """Convert SWYGX to types/categories."""
    holdings = common.read_sql_table("swygx_holdings").iloc[-1]
    for etf_type, etfs_list in etf_type_map.items():
        ira_df.loc[etf_type] = (
            ira_df.loc["SWYGX"].value
            * holdings[holdings.index.intersection(etfs_list)].sum()
            / 100
        )
    return ira_df.loc[etf_type_map.keys()]


def convert_etfs_to_types(etfs_df, etf_type_map: dict[str, list[str]]):
    """Convert ETFs to types/categories."""
    for etf_type, etfs_list in etf_type_map.items():
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
    return etfs_df.loc[etf_type_map.keys()]


def get_desired_df(amount: int, otm: bool) -> pd.DataFrame | None:
    """Get dataframe, cost to get to desired allocation."""
    if not (desired_allocation := adjustment(DESIRED_ALLOCATION)):
        return None
    if (s := round(sum(desired_allocation.values()))) != 100:
        print(f"Sum of percents in desired allocation {s} != 100")
        return None

    etfs_df = pd.read_csv(
        etfs.CSV_OUTPUT_PATH, index_col=0, usecols=["ticker", "value"]
    ).fillna(0)
    # Take into account options assignment
    options_df = stock_options.options_df()
    if not otm:
        options_df = options_df.loc[lambda df: df["in_the_money"]]
    itm_df = stock_options.after_assignment_df(options_df)
    etfs_df["value"] = etfs_df["value"].add(itm_df["value_change"], fill_value=0)
    ira_df = pd.read_csv(
        schwab_ira.CSV_OUTPUT_PATH, index_col=0, usecols=["ticker", "value"]
    ).fillna(0)
    wanted_df = pd.DataFrame({"wanted_percent": pd.Series(desired_allocation)})
    mf_df = convert_etfs_to_types(etfs_df, ETF_TYPE_MAP) + convert_ira_to_types(
        ira_df, ETF_TYPE_MAP
    )
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


def get_buy_only_df(allocation_df: pd.DataFrame, amount: int) -> pd.DataFrame:
    """Get an allocation dataframe that only involves buying and not selling."""
    if len(allocation_df[allocation_df["usd_to_reconcile"] < 0]) == 0:
        return allocation_df
    return get_common_only_df(allocation_df, allocation_df.clip(lower=0), amount, "buy")


def get_sell_only_df(allocation_df: pd.DataFrame, amount: int) -> pd.DataFrame:
    """Get an allocation dataframe that only involves selling and not buying."""
    if len(allocation_df[allocation_df["usd_to_reconcile"] > 0]) == 0:
        return allocation_df
    return get_common_only_df(
        allocation_df, allocation_df.clip(upper=0), amount, "sell"
    )


def get_rebalancing_df(
    amount: int,
    otm: bool = True,
) -> pd.DataFrame | None:
    """Get rebalancing dataframe."""
    if (allocation_df := get_desired_df(amount=amount, otm=otm)) is None:
        return None
    if amount > 0:
        allocation_df = get_buy_only_df(allocation_df, amount)
    elif amount < 0:
        allocation_df = get_sell_only_df(allocation_df, amount)
    return allocation_df


def main():
    """Main."""
    parser = argparse.ArgumentParser(
        description="Rebalance ETFs",
    )
    parser.add_argument("--value", default=0, type=int)
    parser.add_argument("--otm", default=True, action=argparse.BooleanOptionalAction)
    args = parser.parse_args()
    print(get_rebalancing_df(amount=args.value, otm=args.otm))


if __name__ == "__main__":
    main()
