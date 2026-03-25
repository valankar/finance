#!/usr/bin/env python3
"""Balance portfolio based on SWYGX."""

import typing
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
    "GOLD": 34,
    "SILVER": 33,
    "CRYPTO": 33,
}
# Minimum required percentage of commodities. This eats into the US_BONDS percentage.
COMMODITIES_PERCENTAGE_FLOOR = 8
# See https://testfol.io/?s=fkCyZLoXExo
ETF_TYPE_MAP = {
    "COMMODITIES_GOLD": ["GLD", "GLDM", "IAU", "SGOL", "/MGC"],
    "COMMODITIES_SILVER": ["SIVR", "SLV", "/SIL"],
    "COMMODITIES_CRYPTO": ["BITX", "IBIT", "MSTR", "/MBT"],
    "US_SMALL_CAP": ["SCHA", "VB", "IWM", "/M2K", "/RTY"],
    "US_LARGE_CAP": [
        "SCHX",
        "SPY",
        "SPYM",
        "SPX",
        "SPXW",
        "XSP",
        "XSPW",
        "VOO",
        "VV",
        "/MES",
        "/ES",
    ],
    "US_BONDS": [
        "BND",
        "IEF",
        "SCHO",
        "SCHR",
        "SCHZ",
        "SWAGX",
        "TLH",
        "TLT",
        "/10Y",
        "/MTN",
        "/ZN",
    ],
    "INTERNATIONAL_DEVELOPED": ["EFA", "SCHF", "SWISX", "VEA", "/MFS"],
    "INTERNATIONAL_EMERGING": ["EEM", "SCHE", "VWO"],
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
    opts: stock_options.OptionsAndSpreads = stock_options.get_options_and_spreads()
    futures_tickers = futures.Futures().notional_values_df
    futures_tickers.loc[
        futures_tickers.index.isin(FUTURES_INVERSE_CORRELATION), "value"
    ] *= -1
    etfs_df = etfs_df.add(futures_tickers, fill_value=0)
    options_df = opts.get_all_without_box_spreads()
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


def _rebalancing(
    rebalancing_df: pd.DataFrame,
    limit_broker: Optional[str],
    header: str,
    positions_df: pd.DataFrame,
    get_ticker: typing.Callable[[tuple], str],
    get_profit: typing.Callable[[pd.Series, int], float],
    extra_lines: typing.Callable[[pd.Series, int], str],
):
    positions_by_category: dict[str, list[tuple[tuple, pd.Series, str]]] = {}
    for idx, pos in positions_df.iterrows():
        idx_tuple = typing.cast(tuple, idx)
        if pos["count"] == 0:
            continue
        if limit_broker and limit_broker not in idx_tuple[0]:
            continue
        ticker = get_ticker(idx_tuple)
        for cat, tickers in ETF_TYPE_MAP.items():
            if ticker in tickers:
                positions_by_category.setdefault(cat, []).append(
                    (idx_tuple, pos, ticker)
                )
                break

    header_printed = False
    df = rebalancing_df[rebalancing_df["usd_to_reconcile"].abs() > 1000]
    for category, row in df.iterrows():
        category_str = typing.cast(str, category)
        positions = positions_by_category.get(category_str, [])
        if not positions:
            continue
        subheader = (
            f"Category: {category_str} Need to reconcile: {row['usd_to_reconcile']:.0f}"
        )
        subheader_printed = False
        for idx_tuple, pos, ticker in positions:
            nv = pos["notional_value"]
            if ticker in FUTURES_INVERSE_CORRELATION:
                nv *= -1
            if (value_per_contract := nv / abs(pos["count"])) == 0:
                continue
            if value_per_contract * row["usd_to_reconcile"] > 0:
                continue
            count = round(
                min(
                    abs(row["usd_to_reconcile"] / value_per_contract),
                    abs(pos["count"]),
                )
            )
            if count < 1:
                continue
            if get_profit(pos, count) < 0:
                continue
            value = -(value_per_contract * count)
            if abs(value) > abs(row["usd_to_reconcile"]):
                continue
            xact = "Selling" if pos["count"] > 0 else "Buying"
            if not header_printed:
                print(header)
                header_printed = True
            if not subheader_printed:
                print(subheader)
                subheader_printed = True
            print(
                f"  {xact} {count} {idx_tuple[0]} {idx_tuple[1]} contracts worth {value / count:.0f} results in value change of: {value:.0f}"
            )
            print(extra_lines(pos, count))


def futures_rebalancing(rebalancing_df: pd.DataFrame, limit_broker: Optional[str]):
    def get_ticker(idx: tuple) -> str:
        return idx[1][:-3]

    def get_profit(pos: pd.Series, count: int) -> float:
        return (pos["value"] / abs(pos["count"])) * count

    def extra_lines(pos: pd.Series, count: int) -> str:
        return f"    Futures cash value/profit: {get_profit(pos, count):.0f}"

    _rebalancing(
        rebalancing_df,
        limit_broker,
        "\nFutures to close for rebalancing:",
        futures.Futures().futures_df,
        get_ticker,
        get_profit,
        extra_lines,
    )


def options_rebalancing(rebalancing_df: pd.DataFrame, limit_broker: Optional[str]):
    def get_ticker(idx: tuple) -> str:
        return typing.cast(pd.Series, options_df.loc[idx])["ticker"]

    def get_profit(pos: pd.Series, count: int) -> float:
        return (pos["profit_option_value"] / abs(pos["count"])) * count

    def extra_lines(pos: pd.Series, count: int) -> str:
        cash = (pos["value"] / pos["count"]) * count
        return f"    Options cash value: {cash:.0f}\n    Profit: {get_profit(pos, count):.0f}"

    options_df = stock_options.get_options_and_spreads().get_all_without_box_spreads()
    _rebalancing(
        rebalancing_df,
        limit_broker,
        "\nOptions to close for rebalancing:",
        options_df,
        get_ticker,
        get_profit,
        extra_lines,
    )


app = App()


@app.default
def main(
    value: int = 0,
    commodities_percentage_floor: int = COMMODITIES_PERCENTAGE_FLOOR,
    adjustment: dict[str, int] = {},
    limit_broker: Optional[str] = None,
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
    limit_broker: str
        Limit options and futures rebalancing to broker with this string.
    """
    global COMMODITIES_PERCENTAGE_FLOOR
    COMMODITIES_PERCENTAGE_FLOOR = commodities_percentage_floor
    df = get_rebalancing_df(
        amount=value,
        adjustment=adjustment,
    )
    print(df)
    options_rebalancing(df, limit_broker)
    futures_rebalancing(df, limit_broker)


if __name__ == "__main__":
    app()
