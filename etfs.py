#!/usr/bin/env python3
"""Calculate ETF values."""

import pandas as pd

import common

TABLE_PREFIX = "schwab_etfs"


def get_etfs_df() -> pd.DataFrame:
    prices_df = common.read_sql_last("schwab_etfs_prices")
    amounts_df = common.read_sql_last("schwab_etfs_amounts")
    df = (
        pd.DataFrame(
            {
                "shares": amounts_df.iloc[0],
                "current_price": prices_df.iloc[0],
                "value": amounts_df.iloc[0] * prices_df.iloc[0],
            }
        )
        .fillna(0)
        .rename_axis("ticker")
        .sort_index()
    )
    return df


def main():
    """Main."""
    common.write_ticker_sql(f"{TABLE_PREFIX}_amounts", f"{TABLE_PREFIX}_prices")


if __name__ == "__main__":
    main()
