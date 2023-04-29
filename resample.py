#!/usr/bin/env python3
"""Run daily resampling."""

import pandas as pd
import common


def resample_file(filename, precision=2, header='infer'):
    """Resample file and round to number of decimal places."""
    filename = f'{common.PREFIX}/{filename}.csv'
    dataframe = pd.read_csv(filename,
                            index_col=0,
                            parse_dates=True,
                            infer_datetime_format=True,
                            header=header)
    dataframe = dataframe.resample('1d').mean().ffill()
    with common.temporary_file_move(filename) as output_file:
        dataframe.to_csv(output_file, float_format=f'%.{precision}f')


def main():
    """Resample."""
    resample_file('history')
    resample_file('account_history', header=[0, 1, 2, 3])
    resample_file('forex', precision=7)


if __name__ == '__main__':
    main()
