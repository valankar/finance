#!/usr/bin/env python3
"""Sell owned stock or options."""

import argparse
import typing
from typing import Literal

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


def sell_stock_brokerage(brokerage: Literal["ibkr", "schwab"], value: int):
    # Find how much to balance
    if (
        rebalancing_df := balance_etfs.get_rebalancing_df(amount=-value, otm=False)
    ) is None:
        print("Cannot get rebalancing dataframe")
        return
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
        return
    new_df = pd.DataFrame()
    # Take into account options assignment
    options_df = stock_options.options_df()
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
    print("\nShares at brokerage including ITM option assignment")
    print(brokerage_df, "\n")
    for etf_type, etfs_in_type in balance_etfs.ETF_TYPE_MAP.items():
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
    new_df["value_to_sell"] = new_df["shares_to_sell"] * new_df["current_price"]
    new_df["options_to_sell"] = new_df["shares_to_sell"] // 100
    return new_df


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
            print(sell_df.round(2))
            print(f"Total value to sell: {sell_df['value_to_sell'].sum():.2f}")


if __name__ == "__main__":
    main()
