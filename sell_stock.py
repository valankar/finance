#!/usr/bin/env python3
"""Store IBKR margin rate history for USD, CHF."""

import argparse
from typing import Literal

import pandas as pd

import balance_etfs
import etfs
import ledger_amounts


def sell_stock(stock: str, value: int) -> pd.DataFrame:
    etfs_df = etfs.get_etfs_df()
    current_price = etfs_df.loc[stock := stock.upper()]["current_price"]
    needed = value / current_price
    etfs_df.loc[stock, "value_to_sell"] = value
    etfs_df.loc[stock, "shares_to_sell"] = needed
    df = etfs_df.dropna()
    return df


def sell_stock_brokerage(brokerage: Literal["ibkr", "schwab"], value: int):
    # Find how much to balance
    if (rebalancing_df := balance_etfs.get_rebalancing_df(-value)) is None:
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
    for etf_type, etfs_in_type in balance_etfs.ETF_TYPE_MAP.items():
        to_sell = -rebalancing_df.loc[etf_type, "usd_to_reconcile"]  # type: ignore
        if to_sell <= 0:
            continue
        for etf in etfs_in_type:
            if etf in brokerage_df.columns:
                new_df = pd.concat([new_df, sell_stock(etf, to_sell)])
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
    if not args.value:
        parser.error("Value required")
    if args.ticker:
        print(sell_stock(args.ticker, args.value).round(2))
    elif args.brokerage:
        if (sell_df := sell_stock_brokerage(args.brokerage, args.value)) is not None:
            print(sell_df.round(2))
            print(f"Total value to sell: {sell_df['value_to_sell'].sum():.2f}")


if __name__ == "__main__":
    main()
