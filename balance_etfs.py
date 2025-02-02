#!/usr/bin/env python3
"""Balance portfolio based on SWYGX."""

import argparse
from typing import Any, Optional

import pandas as pd

import common
import etfs
import schwab_ira
import stock_options

# All allocations come from SWYGX portfolio.
# SCHH and money markets are ignored. Instead, the leftover is replaced with commodities.
# The commodities are broken down by percent defined here.
COMMODITIES_PERCENTAGE = {
    "GOLD": 62,
    "SILVER": 5,
    "CRYPTO": 33,
}
# Minimum required percentage of commodities. This eats into the US_BONDS percentage.
COMMODITIES_PERCENTAGE_FLOOR = 8
ETF_TYPE_MAP = {
    "COMMODITIES_GOLD": ["GLDM", "SGOL"],
    "COMMODITIES_SILVER": ["SIVR"],
    "COMMODITIES_CRYPTO": ["COIN", "BITX", "MSTR"],
    "US_SMALL_CAP": ["SCHA"],
    "US_LARGE_CAP": ["SCHX", "VV"],
    "US_BONDS": ["SCHO", "SCHR", "SCHZ", "SGOV", "SWAGX"],
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


def get_swtsx_market_cap():
    """Get market cap distribution from swtsx_market_cap DB table."""
    return common.read_sql_last("swtsx_market_cap").iloc[-1]


def get_swygx_allocations() -> dict[str, float]:
    swygx_holdings = common.read_sql_last("swygx_holdings").iloc[-1]
    allocations = {}
    for etf_type in (
        "US_LARGE_CAP",
        "US_SMALL_CAP",
        "US_BONDS",
        "INTERNATIONAL_DEVELOPED",
        "INTERNATIONAL_EMERGING",
    ):
        allocations[etf_type] = swygx_holdings[
            swygx_holdings.index.intersection(ETF_TYPE_MAP[etf_type])
        ].sum()
    return allocations


def get_desired_allocation() -> dict[str, Any] | None:
    allocation = get_swygx_allocations()
    allocation["COMMODITIES"] = 100 - sum(allocation.values())
    if allocation["COMMODITIES"] < COMMODITIES_PERCENTAGE_FLOOR:
        allocation["US_BONDS"] -= (
            COMMODITIES_PERCENTAGE_FLOOR - allocation["COMMODITIES"]
        )
        allocation["COMMODITIES"] = COMMODITIES_PERCENTAGE_FLOOR
    if sum(COMMODITIES_PERCENTAGE.values()) != 100:
        print("Sum of COMMODITIES_PERCENTAGE != 100")
        return None
    for commodity, percentage in COMMODITIES_PERCENTAGE.items():
        allocation[f"COMMODITIES_{commodity}"] = (
            allocation["COMMODITIES"] * percentage
        ) / 100
    del allocation["COMMODITIES"]
    if sum(allocation.values()) != 100:
        print("Sum of desired percentages != 100")
        return None
    if any([x < 0 for x in allocation.values()]):
        print("Desired percentages has negative value")
        return None
    return allocation


def convert_ira_to_types(ira_df, etf_type_map: dict[str, list[str]]):
    """Convert SWYGX to types/categories."""
    holdings = common.read_sql_last("swygx_holdings").iloc[-1]
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


def get_desired_df(
    amount: int,
    include_options: bool,
    otm: bool,
    long_calls: bool,
    adjustment: dict[str, int],
) -> pd.DataFrame | None:
    """Get dataframe, cost to get to desired allocation."""
    if not (desired_allocation := get_desired_allocation()):
        return None
    if (s := round(sum(desired_allocation.values()))) != 100:
        print(f"Sum of percents in desired allocation {s} != 100")
        return None
    etfs_df = pd.read_csv(
        etfs.CSV_OUTPUT_PATH, index_col=0, usecols=["ticker", "value"]
    ).fillna(0)
    if include_options:
        if long_calls:
            query = "((count > 0) or (count < 0))"
        else:
            query = "(count < 0)"
        if otm:
            query += " & ~in_the_money"
        else:
            query += " & in_the_money"
        options_df = stock_options.options_df().query(query)
        itm_df = stock_options.after_assignment_df(options_df)
        etfs_df["value"] = etfs_df["value"].add(itm_df["value_change"], fill_value=0)
    ira_df = pd.read_csv(
        schwab_ira.CSV_OUTPUT_PATH, index_col=0, usecols=["ticker", "value"]
    ).fillna(0)
    wanted_df = pd.DataFrame({"wanted_percent": pd.Series(desired_allocation)})
    mf_df = convert_etfs_to_types(etfs_df, ETF_TYPE_MAP) + convert_ira_to_types(
        ira_df, ETF_TYPE_MAP
    )
    for category, adjust in adjustment.items():
        mf_df.loc[category] += adjust
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
    include_options: bool = False,
    otm: bool = False,
    long_calls: bool = False,
    adjustment: Optional[dict[str, int]] = None,
) -> pd.DataFrame | None:
    """Get rebalancing dataframe."""
    if (
        allocation_df := get_desired_df(
            amount=amount,
            include_options=include_options,
            otm=otm,
            long_calls=long_calls,
            adjustment=adjustment or {},
        )
    ) is None:
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
    parser.add_argument(
        "--include-options", default=False, action=argparse.BooleanOptionalAction
    )
    parser.add_argument("--otm", default=False, action=argparse.BooleanOptionalAction)
    parser.add_argument(
        "--long-calls", default=False, action=argparse.BooleanOptionalAction
    )
    parser.add_argument("--commodities-percentage-floor", type=int)
    parser.add_argument("--adjustment", nargs="*", default=[])
    args = parser.parse_args()
    if args.commodities_percentage_floor:
        global COMMODITIES_PERCENTAGE_FLOOR
        COMMODITIES_PERCENTAGE_FLOOR = args.commodities_percentage_floor
    adjustment: dict[str, int] = {}
    for a in args.adjustment:
        category, amount = a.split("=")
        adjustment[category] = int(amount)
    if (
        df := get_rebalancing_df(
            amount=args.value,
            include_options=args.include_options,
            otm=args.otm,
            long_calls=args.long_calls,
            adjustment=adjustment,
        )
    ) is not None:
        print(df)


if __name__ == "__main__":
    main()
