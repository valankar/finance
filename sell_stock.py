#!/usr/bin/env python3
"""Sell owned stock or options."""

import argparse
import typing
from typing import Literal, Optional

import pandas as pd

import balance_etfs
import etfs
import ledger_amounts
import stock_options


def sell_stock(stock: str, value: float) -> pd.DataFrame:
    etfs_df = etfs.get_etfs_df()
    current_price = etfs_df.loc[stock := stock.upper()]["current_price"]
    needed = value / current_price
    etfs_df.loc[stock, "value_to_sell"] = value
    etfs_df.loc[stock, "shares_to_sell"] = needed
    df = etfs_df.dropna()
    return df


def sell_from_rebalancing(
    rebalancing_df: pd.DataFrame, brokerage_df: pd.DataFrame
) -> Optional[pd.DataFrame]:
    new_df = pd.DataFrame()
    for etf_type, etfs_in_type in balance_etfs.ETF_TYPE_MAP.items():
        if etf_type not in rebalancing_df.index:
            continue
        to_sell = -typing.cast(float, rebalancing_df.loc[etf_type, "sell_only"])
        if to_sell <= 0:
            continue
        for etf in etfs_in_type:
            if etf in brokerage_df.columns:
                sell_df = sell_stock(etf, to_sell)
                max_shares = brokerage_df.iloc[0][etf]
                # Only sell what is available.
                sell_df.loc[etf, "shares_to_sell"] = min(
                    max_shares,
                    typing.cast(float, sell_df.loc[etf, "shares_to_sell"]),
                )
                identical_etfs = brokerage_df.columns.intersection(etfs_in_type)
                if len(identical_etfs) > 1:
                    sell_df["identical_to"] = " ".join(
                        sorted(set(identical_etfs) - set([etf]))
                    )
                else:
                    sell_df["identical_to"] = ""
                new_df = pd.concat([new_df, sell_df])
    if len(new_df):
        new_df["value_to_sell"] = new_df["shares_to_sell"] * new_df["current_price"]
        new_df["options_to_sell"] = new_df["shares_to_sell"] // 100
        return new_df
    return None


def sell_stock_brokerage(
    brokerage: Literal["ibkr", "schwab"], value: int
) -> Optional[pd.DataFrame]:
    # Find how much to balance
    if (rebalancing_df := balance_etfs.get_rebalancing_df(amount=-value)) is None:
        print("Cannot get rebalancing dataframe")
        return None
    if "sell_only" not in rebalancing_df.columns:
        rebalancing_df["sell_only"] = rebalancing_df["usd_to_reconcile"]
    print(rebalancing_df)
    match brokerage:
        case "ibkr":
            account = "Interactive Brokers"
        case "schwab":
            account = "Charles Schwab Brokerage"
    if (
        brokerage_df := ledger_amounts.get_commodity_df(
            ledger_amounts.LEDGER_LIMIT_ETFS + f' --limit "account=~/{account}/"'
        )
    ) is None:
        print("No ETFs found")
        return None
    # Take into account options assignment
    if (options_data := stock_options.get_options_data()) is None:
        raise ValueError("No options data available")
    options_df = options_data.opts.all_options
    if account in options_df.index.get_level_values(0):
        options_df = stock_options.after_assignment_df(
            typing.cast(pd.DataFrame, options_df.xs(account))
        )
        for etf in brokerage_df.columns:
            try:
                brokerage_df[etf] = brokerage_df[etf].add(
                    options_df.loc[etf, "shares_change"], fill_value=0
                )
            except KeyError:
                pass
    print("\nShares at brokerage including option assignment")
    print(brokerage_df, "\n")
    remaining = value
    if (sell_df := sell_from_rebalancing(rebalancing_df, brokerage_df)) is None:
        print("No ETFs to sell")
    i = 0
    while (sell_df is None) or (
        remaining := value
        - sell_df.drop_duplicates(subset="value_to_sell")["value_to_sell"].sum()
    ) > 0:
        if len(new_rebalancing_df := rebalancing_df.query("sell_only == 0")) == 0:
            new_rebalancing_df = rebalancing_df
        new_rebalancing_df = new_rebalancing_df.sort_values(by="usd_to_reconcile").iloc[
            i : i + 1
        ]
        new_rebalancing_df["sell_only"] = -remaining
        if (
            new_sell_df := sell_from_rebalancing(new_rebalancing_df, brokerage_df)
        ) is not None:
            sell_df = pd.concat([sell_df, new_sell_df])
        i += 1
        if i > 50:
            print(
                f"Reached 50 iterations without selling enough: {remaining:.0f} remaining"
            )
            break
    if sell_df is not None:
        return sell_df.query("shares_to_sell > 0")
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Figure out which stocks to sell at each brokerage.",
    )
    parser.add_argument("--ticker", default=None, type=str)
    parser.add_argument("--brokerage", default=None, choices=["ibkr", "schwab"])
    parser.add_argument("--value", default=None, type=int)
    args = parser.parse_args()
    if not args.value or args.value <= 0:
        parser.error("Positive value required")
    if not args.ticker and not args.brokerage:
        parser.error("Must specify ticker or brokerage")
    if args.ticker:
        print(sell_stock(args.ticker, args.value).round(2))
    else:
        if (sell_df := sell_stock_brokerage(args.brokerage, args.value)) is not None:
            sell_df = sell_df.round(2)
            print(sell_df)
            print(
                f"Total value to sell: {sell_df.drop_duplicates(subset='value_to_sell')['value_to_sell'].sum():.2f}"
            )


if __name__ == "__main__":
    main()
