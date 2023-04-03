#!/usr/bin/env python3
"""Calculate ETF values."""

import sys

import pandas as pd
import progressbar

import common

ETFS_PATH = common.PREFIX + 'etfs_values.csv'


def trade(etfs_df, amount, original_amount, total, progress):
    """Simulate a trade."""
    progress.update(amount)
    etfs_df['usd_to_reconcile'] = (
        amount * (etfs_df['wanted_percent'] / 100)) + ((
            (etfs_df['wanted_percent'] / 100) * total) - etfs_df['value'])
    etfs_df['shares_to_trade'] = (etfs_df['usd_to_reconcile'] /
                                  etfs_df['current_price'])
    etfs_df = etfs_df.round(2)
    etfs_df['shares_to_trade'] = etfs_df['shares_to_trade'].round(0)
    cost = (etfs_df['shares_to_trade'] * etfs_df['current_price']).sum()
    if round(cost, 0) > original_amount:
        return trade(etfs_df, amount - 1, original_amount, total, progress)
    return etfs_df


def main():
    """Main."""
    amount = 0
    if len(sys.argv) > 1:
        amount = float(sys.argv[1])
    etfs_df = pd.read_csv(ETFS_PATH, index_col=0)
    data = {
        'wanted_percent':
        pd.Series(
            [30, 20, 15, 10, 10, 10, 5],
            index=['ETF6', 'ETF4', 'ETF5', 'ETF2', 'ETF7', 'ETF1', 'ETF3'])
    }
    total = etfs_df['value'].sum()
    etfs_df['current_percent'] = (etfs_df['value'] / total) * 100
    etfs_df = pd.merge(etfs_df,
                       pd.DataFrame(data),
                       left_index=True,
                       right_index=True)

    progress = progressbar.ProgressBar(max_value=progressbar.UnknownLength,
                                       widgets=[
                                           'Working: ',
                                           progressbar.AnimatedMarker(), ' ',
                                           progressbar.Timer()
                                       ])
    etfs_df = trade(etfs_df, amount, amount, total, progress)
    cost = (etfs_df['shares_to_trade'] * etfs_df['current_price']).sum()
    print(etfs_df)
    print(f'Sum of trades: {round(cost, 2)}')


if __name__ == '__main__':
    main()
