#!/usr/bin/env python3
"""Manually add home values."""

import statistics
import sys

import pandas as pd

import common

NAME_TO_FILE = {
    "Property Name": "prop1.txt",
}


def write_prices_table(name, redfin, zillow):
    """Write prices to sqlite."""
    home_df = pd.DataFrame(
        {"name": name, "redfin_value": redfin, "zillow_value": zillow},
        index=[pd.Timestamp.now()],
    )
    common.to_sql(home_df, "real_estate_prices", foreign_key=True)


def write_rents_table(name, value):
    """Write rents to sqlite."""
    home_df = pd.DataFrame({"name": name, "value": value}, index=[pd.Timestamp.now()])
    common.to_sql(home_df, "real_estate_rents", foreign_key=True)


def main():
    """Main."""
    try:
        name = sys.argv[1]
        redfin_price = int(sys.argv[2].replace(",", ""))
        zillow_price = int(sys.argv[3].replace(",", ""))
        rent = int(sys.argv[4].replace(",", ""))
    except IndexError:
        print(f"Usage: {sys.argv[0]} name redfin_price zillow_price rent")
        sys.exit(1)
    average = round(statistics.mean([redfin_price, zillow_price]))
    with common.temporary_file_move(
        f"{common.PREFIX}{NAME_TO_FILE[name]}"
    ) as output_file:
        output_file.write(str(average))
    write_prices_table(name, redfin_price, zillow_price)
    write_rents_table(name, rent)


if __name__ == "__main__":
    main()
