#!/usr/bin/env python3
"""Get 401k value."""

import csv

import common

VANGUARD_PATH = common.PREFIX + '401k_amounts.csv'
OUTPUT_PATH = common.PREFIX + '401k_values.csv'


def write_output_file(output_file, vanguard_amounts, prices):
    """Write vanguard csv file."""
    fieldnames = ['ticker', 'shares', 'current_price', 'value']
    writer = csv.DictWriter(output_file, fieldnames=fieldnames)
    writer.writeheader()
    for ticker in sorted(vanguard_amounts.keys()):
        writer.writerow({
            'ticker': ticker,
            'shares': vanguard_amounts[ticker],
            'current_price': prices[ticker],
            'value': prices[ticker]*vanguard_amounts[ticker],
        })


def main():
    """Main."""
    vanguard_amounts = {}
    with open(VANGUARD_PATH, 'r', newline='', encoding='utf-8') as input_file:
        csv_file = csv.DictReader(input_file)
        for row in csv_file:
            vanguard_amounts[row['ticker']] = float(row['shares'])

    prices = {}
    prices['VWIAX'] = common.get_ticker('VWIAX')

    with open(common.PREFIX + 'vanguard.txt', encoding='utf-8') as vanguard_file:
        prices['Vanguard Target Retirement 2040 Trust'] = float(
            vanguard_file.read())

    with common.temporary_file_move(OUTPUT_PATH) as output_file:
        write_output_file(output_file, vanguard_amounts, prices)


if __name__ == '__main__':
    main()
