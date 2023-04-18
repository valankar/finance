#!/usr/bin/env python3
"""Get estimated home values."""

import shutil
from datetime import datetime, date
from pathlib import Path

import numpy as np
import pandas as pd

import common

URL_PREFIX = 'https://www.redfin.com'
URLS = {
    'property3.txt': '/some/redfin/path/123',
    'property4.txt': '/some/redfin/path/123',
    'property5.txt': '/some/redfin/path/123',
}

PURCHASE_PRICES = {
    'property3.txt': ('2012-07-09', 438000),
    'property4.txt': ('2014-04-16', 532500),
    'property5.txt': ('2013-04-15', 475000),
}

HOME_COLUMN_MAP = {
    'property1.txt': 'Property 1',
    'property2.txt': 'Property 2',
    'property3.txt': 'Property 3',
    'property4.txt': 'Property 4',
    'property5.txt': 'Property 5',
}

OUTPUT_DIR = f'{Path.home()}/bin/accounts/historical/'


def get_redfin_estimate(url_path):
    """Get home value from Redfin."""
    return int(
        common.find_xpath_via_browser(
            URL_PREFIX + url_path,
            # pylint: disable-next=line-too-long
            '//*[@id="content"]/div[12]/div[2]/div[1]/div/div[1]/div/div[1]/div/div/div/div/div/div[1]/div/span'
        ).translate(str.maketrans('', '', '$,')))


def create_csv():
    """Create CSV files of homes with purchase and current price."""
    today = date.today()
    for filename, url_path in URLS.items():
        current_price = get_redfin_estimate(url_path)
        purchase_date = datetime.strptime(PURCHASE_PRICES[filename][0],
                                          '%Y-%m-%d').date()
        purchase_price = PURCHASE_PRICES[filename][1]
        home_df = pd.DataFrame({
            'value': [purchase_price, current_price]
        },
                               index=[purchase_date,
                                      today]).rename_axis('date')
        home_df.to_csv(f'{OUTPUT_DIR}{filename}.csv')


def merge_redfin():
    """"Merge Redfin data into account history."""
    accounts_df = pd.read_csv(f'{common.PREFIX}account_history.csv',
                              index_col=0,
                              parse_dates=True,
                              infer_datetime_format=True,
                              header=[0, 1, 2, 3])
    for home, column in HOME_COLUMN_MAP.items():
        home_df = pd.read_csv(f'{OUTPUT_DIR}{home}.csv',
                              index_col=0,
                              parse_dates=True,
                              infer_datetime_format=True)
        home_df = home_df.resample('D').mean().interpolate()
        column = ('USD', 'Real Estate', column, 'nan')
        accounts_df[column] = np.nan
        new_df = pd.merge_asof(accounts_df[[column]].droplevel([1, 2, 3],
                                                               axis=1),
                               home_df,
                               left_index=True,
                               right_index=True,
                               tolerance=pd.Timedelta('1d'))
        accounts_df[column] = new_df['value']
    # Sales
    accounts_df.loc['2019-10-16':,
                    ('USD', 'Real Estate', 'Property 2', 'nan')] = 0
    accounts_df.loc['2014-03-01':,
                    ('USD', 'Real Estate', 'Property 1', 'nan')] = 0
    accounts_df = accounts_df.sort_index().interpolate()

    shutil.copy(f'{common.PREFIX}account_history.csv',
                f'{common.PREFIX}account_history.csv.old')
    with common.temporary_file_move(
            f'{common.PREFIX}account_history.csv') as output_file:
        accounts_df.to_csv(output_file, float_format='%.2f')
    all_df = pd.read_csv(f'{common.PREFIX}history.csv',
                         index_col=0,
                         parse_dates=True,
                         infer_datetime_format=True)
    sum_df = accounts_df['USD']['Real Estate'].sum(axis=1)
    sum_df.name = 'new_total_real_estate'
    all_merged = pd.merge_asof(all_df,
                               sum_df,
                               left_index=True,
                               right_index=True)
    all_merged['total_real_estate'] = all_merged['new_total_real_estate']
    all_merged = all_merged.drop(columns='new_total_real_estate')

    all_merged['total'] = all_merged['total_investing'] + all_merged['total_retirement'] + \
        all_merged['total_real_estate'] + all_merged['total_liquid']
    all_merged['total_no_homes'] = all_merged['total_investing'] + \
        all_merged['total_retirement'] + all_merged['total_liquid']
    shutil.copy(f'{common.PREFIX}history.csv',
                f'{common.PREFIX}history.csv.old')
    with common.temporary_file_move(
            f'{common.PREFIX}history.csv') as output_file:
        all_merged.to_csv(output_file, float_format='%.2f')


def main():
    """Main."""
    create_csv()
    merge_redfin()


if __name__ == '__main__':
    main()
