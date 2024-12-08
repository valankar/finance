#!/usr/bin/env python3
"""Buy specific stocks or options."""

import argparse
import typing

import pandas as pd

import balance_etfs
import etfs


def buy_stock(stock: str, value: float) -> pd.DataFrame:
    etfs_df = etfs.get_etfs_df()
    current_price = etfs_df.loc[stock := stock.upper()]["current_price"]
    needed = value / current_price
    etfs_df.loc[stock, "value_to_buy"] = value
    etfs_df.loc[stock, "shares_to_buy"] = needed
    df = etfs_df.dropna()
    return df


def buy_stock_any(value: int):
    # Find how much to balance
    if (
        rebalancing_df := balance_etfs.get_rebalancing_df(amount=value, otm=False)
    ) is None:
        print("Cannot get rebalancing dataframe")
        return
    print(rebalancing_df)
    new_df = pd.DataFrame()
    for etf_type, etfs_in_type in balance_etfs.ETF_TYPE_MAP.items():
        to_buy = typing.cast(float, rebalancing_df.loc[etf_type, "buy_only"])
        if to_buy <= 0:
            continue
        for etf in etfs_in_type:
            buy_df = buy_stock(etf, to_buy)
            if len(etfs_in_type) > 1:
                buy_df["identical_to"] = " ".join(
                    sorted(set(etfs_in_type) - set([etf]))
                )
            else:
                buy_df["identical_to"] = ""
            new_df = pd.concat([new_df, buy_df])
    new_df["value_to_buy"] = new_df["shares_to_buy"] * new_df["current_price"]
    new_df["options_to_buy"] = new_df["shares_to_buy"] // 100
    return new_df


def main():
    parser = argparse.ArgumentParser(
        description="Figure out which stocks to buy.",
    )
    parser.add_argument("--ticker", default=None, type=str)
    parser.add_argument("--value", default=None, type=int)
    args = parser.parse_args()
    if not args.value:
        parser.error("Value required")
    if args.ticker:
        print(buy_stock(args.ticker, args.value).round(2))
    else:
        if (buy_df := buy_stock_any(args.value)) is not None:
            print(buy_df.round(2))
            print(f"Total value to buy: {buy_df['value_to_buy'].sum():.2f}")


if __name__ == "__main__":
    main()
