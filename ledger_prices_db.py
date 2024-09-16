#!/usr/bin/env python
"""Update prices.db with latest data."""

from datetime import datetime

import common

COMMODITY_TABLES = [
    "schwab_etfs_prices",
    "schwab_ira_prices",
]
PROPERTY_COLS = {
    "Prop 1": "Prop 1 Price",
}
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
        forex_df = common.read_sql_table("forex").resample("D").last().loc["2023":]
        for date_index, series in forex_df.iterrows():
            new_date = date_index.strftime(DATE_FORMAT)
            if series.notna()["CHFUSD"]:
                output_file.write(f"P {new_date} CHF ${series['CHFUSD']}\n")
            if series.notna()["SGDUSD"]:
                output_file.write(f"P {new_date} SGD ${series['SGDUSD']}\n")

        # Properties
        real_estate_df = common.get_real_estate_df()
        for estate, col in PROPERTY_COLS.items():
            output_file.write(f'P {NOW} "{estate}" ${real_estate_df[col].iloc[-1]}\n')


if __name__ == "__main__":
    main()
