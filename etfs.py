#!/usr/bin/env python3
"""Calculate ETF values."""

from datetime import datetime
from typing import Optional

import pandas as pd

import common
import futures
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
    cols = get_tickers()
    cols |= set(futures.Futures().futures_df.groupby("commodity").first().index)
    prices_df = prices_df[sorted(prices_df.columns.intersection(list(cols)))]
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


def main():
    """Main."""
    for ticker in sorted(get_tickers()):
        common.insert_sql(
            TICKER_PRICES_TABLE, {"ticker": ticker, "price": common.get_ticker(ticker)}
        )
    for row in futures.Futures().futures_df.groupby("commodity").first().itertuples():
        common.insert_sql(
            TICKER_PRICES_TABLE,
            {"ticker": row.Index, "price": row.current_price},  # type: ignore
        )


if __name__ == "__main__":
    main()
