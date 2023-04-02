#!/usr/bin/env python3
"""Calculate ETF values."""

import csv

import common

ETFS_PATH = common.PREFIX + 'etfs_amounts.csv'
OUTPUT_PATH = common.PREFIX + 'etfs_values.csv'


def write_output_file(csvfile, etfs, tickers):
    """Write etf values csv file."""
    fieldnames = ['ticker', 'shares', 'current_price', 'value']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    for ticker in sorted(etfs.keys()):
        current_price = tickers[ticker]
        writer.writerow(
            {
                'ticker': ticker,
                'shares': etfs[ticker],
                'current_price': current_price,
                'value': current_price*etfs[ticker]
            })


def main():
    """Main."""
    etfs = {}
    with open(ETFS_PATH, 'r', newline='', encoding='utf-8') as amounts:
        csv_amounts = csv.DictReader(amounts)
        for row in csv_amounts:
            etfs[row['ticker']] = float(row['shares'])
    tickers = common.get_tickers(list(etfs.keys()))
    with common.temporary_file_move(OUTPUT_PATH) as csvfile:
        write_output_file(csvfile, etfs, tickers)


if __name__ == '__main__':
    main()
