#!/usr/bin/env python3
"""Manually add home values."""

import argparse
import statistics

import pandas as pd

import common


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
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", required=True)
    parser.add_argument("--redfin-price", required=True, type=int)
    parser.add_argument("--zillow-price", required=True, type=int)
    parser.add_argument("--rent", required=True, type=int)
    args = parser.parse_args()
    average = round(statistics.mean([args.redfin_price, args.zillow_price]))
    if p := common.get_property(args.name):
        with common.temporary_file_move(f"{common.PREFIX}{p.file}") as output_file:
            output_file.write(str(average))
        write_prices_table(args.name, args.redfin_price, args.zillow_price)
        write_rents_table(args.name, args.rent)
    else:
        print(f"Property {args.name} is unknown")


if __name__ == "__main__":
    main()
