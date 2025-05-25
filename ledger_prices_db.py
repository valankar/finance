#!/usr/bin/env python
"""Update prices.db with latest data."""

import typing
from datetime import datetime

import common
import etfs
import futures
import homes
import stock_options

DATE_FORMAT = "%Y/%m/%d %H:%M:%S"
NOW = datetime.now().strftime(DATE_FORMAT)


def main():
    """Main."""
    with common.temporary_file_move(common.LEDGER_PRICES_DB) as output_file:
        # Commodities
        for row in etfs.get_etfs_df().itertuples():
            output_file.write(f"P {NOW} {row.Index} ${row.current_price}\n")

        # Forex values
        series = common.get_latest_forex()
        for ticker, price in series.items():
            ticker = str(ticker).replace("USD", "")
            output_file.write(f"P {NOW} {ticker} ${price}\n")

        # Properties
        real_estate_df = homes.get_real_estate_df()
        for p in homes.PROPERTIES:
            col = f"{p.name} Price"
            output_file.write(
                f'P {NOW} "{p.address}" ${real_estate_df[col].iloc[-1]}\n'
            )

        # Stock options
        if (options_data := stock_options.get_options_data()) is None:
            raise ValueError("No options data available")
        options_df = options_data.opts.all_options
        commodities_written = set()
        for idx, row in options_df.iterrows():
            idx = typing.cast(tuple, idx)
            name = idx[1]
            if name in commodities_written:
                continue
            if row["ticker"] not in ("SPX", "SMI"):
                value = row["value"] / row["count"]
            else:
                value = row["intrinsic_value"] / row["count"]
            output_file.write(f'P {NOW} "{name}" ${value:.2f}\n')
            commodities_written.add(name)

        # Futures
        for row in futures.Futures().ledger_df.itertuples():
            if row.future in commodities_written:
                continue
            output_file.write(f'P {NOW} "{row.future}" ${row.value:.2f}\n')


if __name__ == "__main__":
    main()
