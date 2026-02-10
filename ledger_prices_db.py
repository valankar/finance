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
        for ticker in ("CHF", "SGD"):
            price = common.get_ticker(f"{ticker}USD=X")
            output_file.write(f"P {NOW} {ticker} ${price}\n")

        # Properties
        real_estate_df = homes.get_real_estate_df()
        for p in homes.PROPERTIES:
            col = f"{p.name} Price"
            output_file.write(
                f'P {NOW} "{p.address}" ${real_estate_df[col].iloc[-1]:.0f}\n'
            )

        # Stock options
        options_df = stock_options.get_options_and_spreads().all_options
        commodities_written = set()
        for idx, row in options_df.iterrows():
            idx = typing.cast(tuple, idx)
            name = idx[1]
            if name in commodities_written:
                continue
            value = row["value"] / row["count"]
            output_file.write(f'P {NOW} "{name}" ${value:.2f}\n')
            commodities_written.add(name)

        # Futures
        for row in futures.Futures().ledger_df.itertuples():
            if row.commodity in commodities_written:
                continue
            value = row.current_price * row.multiplier  # type: ignore
            output_file.write(f'P {NOW} "{row.commodity}" ${value:.2f}\n')
            commodities_written.add(row.commodity)


if __name__ == "__main__":
    main()
