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


def get_etfs_df(account: Optional[str] = None) -> pd.DataFrame:
    data = []
    tas = {}
    for ticker, amount in ledger_amounts.get_etfs_amounts(account).items():
        tas[ticker] = amount
    ps = common.get_tickers(tas.keys())
    for ticker, amount in tas.items():
        price = ps[ticker]
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


def get_tickers() -> set[str]:
    cols = set(ledger_amounts.get_etfs_amounts().keys())
    cols |= set(stock_options.options_df_raw()["ticker"].unique())
    return cols


# This is used in separate graph generation processes so redis caching makes sense.
@common.walrus_db.db.lock("get_prices_wide_df", ttl=common.LOCK_TTL_SECONDS * 1000)
@common.walrus_db.cache.cached()
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
    q = common.get_tickers(get_tickers())
    for t in sorted(q):
        common.insert_sql(TICKER_PRICES_TABLE, {"ticker": t, "price": q[t]})
    for row in futures.Futures().futures_df.groupby("commodity").first().itertuples():
        common.insert_sql(
            TICKER_PRICES_TABLE,
            {"ticker": row.Index, "price": row.current_price},  # type: ignore
        )


if __name__ == "__main__":
    main()
