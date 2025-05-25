#!/usr/bin/env python3
"""Calculate ETF values."""

from datetime import datetime
from typing import Optional

import pandas as pd

import common
import ledger_amounts
import stock_options

TICKER_PRICES_TABLE = "ticker_prices"


def get_etfs_df() -> pd.DataFrame:
    data = []
    for ticker, amount in ledger_amounts.get_etfs_amounts().items():
        price = get_price_from_db(ticker)
        data.append(
            {
                "ticker": ticker,
                "shares": amount,
                "current_price": price,
                "value": amount * price,
            }
        )
    df = pd.DataFrame(data).set_index("ticker").sort_index()
    return df


def get_price_from_db(ticker: str) -> float:
    df = common.read_sql_query(
        f"select date, price from {TICKER_PRICES_TABLE} where ticker = '{ticker}' order by date desc limit 1"
    )
    return df.iloc[-1]["price"]


def get_tickers() -> set:
    cols = set(ledger_amounts.get_etfs_amounts().keys())
    if od := stock_options.get_options_data():
        cols |= set(od.opts.pruned_options["ticker"].unique())
    return cols


def get_prices_wide_df() -> pd.DataFrame:
    prices_df = (
        common.read_sql_table(TICKER_PRICES_TABLE)
        .reset_index()
        .pivot(index="date", columns="ticker", values="price")
        .resample("h")
        .last()
        .interpolate()
    )
    prices_df = prices_df[sorted(prices_df.columns.intersection(list(get_tickers())))]
    return prices_df


def get_prices_percent_diff_df(
    r: Optional[tuple[str | datetime, str | datetime]] = None,
) -> pd.DataFrame:
    prices_df = get_prices_wide_df()
    if r is not None:
        start, end = r
        prices_df = prices_df[start:end]
    prices_df = (1 + prices_df.pct_change()).cumprod() * 100 - 100
    return prices_df


def convert_long():
    etfs = common.read_sql_table("schwab_etfs_prices")
    ira = common.read_sql_table("schwab_ira_prices")
    df = common.reduce_merge_asof([etfs, ira])
    df = (
        df.reset_index()
        .melt(id_vars="date", var_name="ticker", value_name="price")
        .set_index("date")
        .sort_index()
        .dropna()
    )
    common.to_sql(df, TICKER_PRICES_TABLE, if_exists="replace")


def main():
    """Main."""
    for ticker in sorted(get_tickers()):
        common.insert_sql(
            TICKER_PRICES_TABLE, {"ticker": ticker, "price": common.get_ticker(ticker)}
        )


if __name__ == "__main__":
    main()
