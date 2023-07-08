#!/usr/bin/env python3
"""Calculate commodity values."""

import pandas as pd
from sqlalchemy import create_engine

import common

OUTPUT_PATH = common.PREFIX + "commodities_values.csv"


def write_ticker_csv(amounts_table, output_path):
    """Write ticker values to prices table and csv file."""
    with create_engine(common.SQLITE_URI).connect() as conn:
        amounts_df = pd.read_sql_table(amounts_table, conn, index_col="date").rename(
            columns={"GOLD": "GC=F", "SILVER": "SI=F"}
        )
    ticker_prices = common.get_tickers(amounts_df.columns)
    prices_df = (
        pd.DataFrame(
            ticker_prices,
            index=[pd.Timestamp.now()],
            columns=sorted(ticker_prices.keys()),
        )
        .rename_axis("date")
        .rename(columns={"GC=F": "GOLD", "SI=F": "SILVER"})
    )
    with create_engine(common.SQLITE_URI).connect() as conn:
        prices_df.to_sql(
            amounts_table.replace("_amounts", "_prices"),
            conn,
            if_exists="append",
            index_label="date",
        )
        conn.commit()

    # Multiply latest amounts by prices.
    amounts_df = amounts_df.rename(columns={"GC=F": "GOLD", "SI=F": "SILVER"})
    latest_amounts = amounts_df.iloc[-1].rename("troy_oz")
    latest_prices = prices_df.iloc[-1].rename("current_price")
    latest_values = (latest_amounts * latest_prices.values).rename("value")
    new_df = pd.DataFrame([latest_amounts, latest_prices, latest_values]).T.rename_axis(
        "commodity"
    )
    new_df.to_csv(output_path)


def main():
    """Main."""
    write_ticker_csv("commodities_amounts", OUTPUT_PATH)


if __name__ == "__main__":
    main()
