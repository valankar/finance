#!/usr/bin/env python
"""Update prices.db with latest data."""

from datetime import datetime

import common

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
            series = common.read_sql_table(commodity_table).iloc[-1]
            for ticker, price in series.items():
                output_file.write(f"P {NOW} {ticker} ${price}\n")

        # Forex values
        series = common.read_sql_table("forex").iloc[-1]
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


if __name__ == "__main__":
    main()
