#!/usr/bin/env python3
"""Balance portfolio based on SWYGX."""

from typing import Any, Optional

import pandas as pd
from cyclopts import App
from loguru import logger

import common
import etfs
import futures
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
# See https://testfol.io/?s=fkCyZLoXExo
ETF_TYPE_MAP = {
    "COMMODITIES_GOLD": ["GLD", "GLDM", "SGOL", "/MGC"],
    "COMMODITIES_SILVER": ["SIVR"],
    "COMMODITIES_CRYPTO": ["BITX", "IBIT", "MSTR", "/MBT"],
    "US_SMALL_CAP": ["SCHA", "VB", "IWM", "/M2K", "/RTY"],
    "US_LARGE_CAP": ["SCHX", "SPLG", "VOO", "VV", "/MES"],
    "US_BONDS": ["BND", "SCHO", "SCHR", "SCHZ", "SWAGX", "/10Y"],
    "INTERNATIONAL_DEVELOPED": ["SCHF", "SWISX", "VEA", "/MFS"],
    "INTERNATIONAL_EMERGING": ["SCHE", "VWO"],
}
FUTURES_INVERSE_CORRELATION = {"/10Y"}
# These get expanded out into US_SMALL_CAP and US_LARGE_CAP according to allocation
# of SWTSX.
TOTAL_MARKET_FUNDS = ["SWTSX", "SCHB", "VTSAX", "VTI"]


class RebalancingError(Exception):
    pass


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


def get_desired_allocation() -> dict[str, Any]:
    allocation = get_swygx_allocations()
    allocation["COMMODITIES"] = 100 - sum(allocation.values())
    if allocation["COMMODITIES"] < COMMODITIES_PERCENTAGE_FLOOR:
        allocation["US_BONDS"] -= (
            COMMODITIES_PERCENTAGE_FLOOR - allocation["COMMODITIES"]
        )
        allocation["COMMODITIES"] = COMMODITIES_PERCENTAGE_FLOOR
    if sum(COMMODITIES_PERCENTAGE.values()) != 100:
        raise RebalancingError("Sum of COMMODITIES_PERCENTAGE != 100")
    for commodity, percentage in COMMODITIES_PERCENTAGE.items():
        allocation[f"COMMODITIES_{commodity}"] = (
            allocation["COMMODITIES"] * percentage
        ) / 100
    del allocation["COMMODITIES"]
    if round(sum(allocation.values())) != 100:
        raise RebalancingError("Sum of desired percentages != 100")
    if any([x < 0 for x in allocation.values()]):
        raise RebalancingError("Desired percentages has negative value")
    return allocation


def convert_etfs_to_types(etfs_df, etf_type_map: dict[str, list[str]]):
    """Convert ETFs to types/categories."""
    for etf_type, etfs_list in etf_type_map.items():
        etfs_df.loc[etf_type] = sum(
            etfs_df.loc[etfs_df.index.intersection(etfs_list)]["value"].fillna(0)
        )

    # IRA
    holdings = common.read_sql_last("swygx_holdings").iloc[-1]
    for etf_type, etfs_list in etf_type_map.items():
        etfs_df.loc[etf_type] += (
            etfs_df.loc["SWYGX"].value
            * holdings[holdings.index.intersection(etfs_list)].sum()
            / 100
        )

    # Expand total market funds into allocation.
    swtsx_market_cap = get_swtsx_market_cap()
    for etf in TOTAL_MARKET_FUNDS:
        if etf not in etfs_df.index:
            continue
        for market_cap, market_cap_allocation in swtsx_market_cap.items():
            etfs_df.loc[market_cap] += etfs_df.loc[etf].fillna(0) * (
                market_cap_allocation / 100
            )
    return etfs_df.loc[etf_type_map.keys()]


def get_desired_df(
    amount: int,
    adjustment: dict[str, int],
) -> pd.DataFrame:
    """Get dataframe, cost to get to desired allocation."""
    desired_allocation = get_desired_allocation()
    etfs_df = etfs.get_etfs_df()[["value"]]
    for ticker, adjust in adjustment.items():
        if ticker in ETF_TYPE_MAP:
            continue
        logger.info(f"Adjusting {ticker=} {adjust=}")
        if ticker in etfs_df.index:
            etfs_df.loc[ticker, "value"] += adjust  # type: ignore
        else:
            etfs_df.loc[ticker, "value"] = adjust
    # Add in options value
    if (options_data := stock_options.get_options_data()) is None:
        raise ValueError("No options data available")
    futures_tickers = futures.Futures().notional_values_df
    futures_tickers.loc[
        futures_tickers.index.isin(FUTURES_INVERSE_CORRELATION), "value"
    ] *= -1
    etfs_df = etfs_df.add(futures_tickers, fill_value=0)
    options_df = options_data.opts.pruned_options
    etfs_df = etfs_df.add(
        options_df.groupby("ticker")
        .sum()[["notional_value"]]
        .rename(columns={"notional_value": "value"}),
        fill_value=0,
    )
    wanted_df = pd.DataFrame({"wanted_percent": pd.Series(desired_allocation)})
    mf_df = convert_etfs_to_types(etfs_df, ETF_TYPE_MAP)
    # Treat Pillar 2 as bonds
    mf_df.loc["US_BONDS", "value"] += common.read_sql_last("history")["pillar2"].iloc[
        -1
    ]
    for category, adjust in adjustment.items():
        if category not in ETF_TYPE_MAP:
            continue
        if category in mf_df.index:
            logger.info(f"Adjusting {category=} {adjust=}")
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
    adjustment: Optional[dict[str, int]] = None,
) -> pd.DataFrame:
    """Get rebalancing dataframe."""
    allocation_df = get_desired_df(
        amount=amount,
        adjustment=adjustment or {},
    )
    if amount > 0:
        allocation_df = get_buy_only_df(allocation_df, amount)
    elif amount < 0:
        allocation_df = get_sell_only_df(allocation_df, amount)
    return allocation_df


app = App()


@app.default
def main(
    value: int = 0,
    commodities_percentage_floor: int = COMMODITIES_PERCENTAGE_FLOOR,
    adjustment: dict[str, int] = {},
):
    """Balance ETFs.

    Parameters
    ----------
    value: int
        Amount to buy/sell. Positive means buy, negative means sell.
    commodities_percentage_floor: int
        Minimum percentage of commodities.
    adjustment: dict[str, int]
        Adjustments to a category's (or ETF's) current balance.
        Example: --adjustment.INTERNATIONAL_DEVELOPED -10000
    """
    global COMMODITIES_PERCENTAGE_FLOOR
    COMMODITIES_PERCENTAGE_FLOOR = commodities_percentage_floor
    df = get_rebalancing_df(
        amount=value,
        adjustment=adjustment,
    )
    print(df)


if __name__ == "__main__":
    app()
