#!/usr/bin/env python
"""Update prices.db with latest data."""

import typing
from datetime import datetime

import common
import stock_options

COMMODITY_TABLES = [
    "schwab_etfs_prices",
    "schwab_ira_prices",
]
DATE_FORMAT = "%Y/%m/%d %H:%M:%S"
NOW = datetime.now().strftime(DATE_FORMAT)


def main():
    """Main."""
    with common.temporary_file_move(common.LEDGER_PRICES_DB) as output_file:
        # Commodities
        for commodity_table in COMMODITY_TABLES:
            series = common.read_sql_last(commodity_table).iloc[-1]
            for ticker, price in series.items():
                output_file.write(f"P {NOW} {ticker} ${price}\n")

        # Forex values
        series = common.read_sql_last("forex").iloc[-1]
        for ticker, price in series.items():
            ticker = str(ticker).replace("USD", "")
            output_file.write(f"P {NOW} {ticker} ${price}\n")

        # Properties
        real_estate_df = common.get_real_estate_df()
        for p in common.PROPERTIES:
            col = f"{p.name} Price"
            output_file.write(
                f'P {NOW} "{p.address}" ${real_estate_df[col].iloc[-1]}\n'
            )

        # Stock options
        options_df = stock_options.options_df_with_value()
        options_written = set()
        for idx, row in options_df.iterrows():
            idx = typing.cast(tuple, idx)
            name = idx[1]
            if name in options_written:
                continue
            value = row["value"] / row["count"]
            output_file.write(f'P {NOW} "{name}" ${value}\n')
            options_written.add(name)


if __name__ == "__main__":
    main()
