#!/usr/bin/env python3
"""Get Schwab IRA value."""

import pandas as pd

import common

TABLE_PREFIX = "schwab_ira"


def get_ira_df() -> pd.DataFrame:
    prices_df = common.read_sql_last("schwab_ira_prices")
    amounts_df = common.read_sql_last("schwab_ira_amounts")
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
    common.write_ticker_sql(
        f"{TABLE_PREFIX}_amounts",
        f"{TABLE_PREFIX}_prices",
        ticker_prices={"SWYGX": common.get_ticker("SWYGX")},
    )


if __name__ == "__main__":
    main()
