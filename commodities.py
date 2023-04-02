#!/usr/bin/env python3
"""Calculate commodity values."""

import csv

import common

COMMODITIES_PATH = common.PREFIX + 'commodities_amounts.csv'
OUTPUT_PATH = common.PREFIX + 'commodities_values.csv'


class UnknownCommodity(Exception):
    """Unknown commodity."""


def write_output_file(csvfile, commodities, commodity_prices):
    """Write etf values csv file."""
    fieldnames = ['commodity', 'troy_oz', 'current_price', 'value']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for commodity in sorted(commodities.keys()):
        match commodity:
            case 'GOLD':
                ticker = 'GC=F'
            case 'SILVER':
                ticker = 'SI=F'
            case _:
                raise UnknownCommodity
        current_price = commodity_prices[ticker]
        writer.writerow(
            {
                'commodity': commodity,
                'troy_oz': commodities[commodity],
                'current_price': current_price,
                'value': current_price*commodities[commodity]
            })


def main():
    """Main."""
    commodities = {}
    with open(COMMODITIES_PATH, 'r', newline='', encoding='utf-8') as amounts:
        csv_amounts = csv.DictReader(amounts)
        for row in csv_amounts:
            commodities[row['commodity']] = float(row['troy_oz'])
    commodity_prices = common.get_tickers(['GC=F', 'SI=F'])
    with common.temporary_file_move(OUTPUT_PATH) as csvfile:
        write_output_file(csvfile, commodities, commodity_prices)


if __name__ == '__main__':
    main()
