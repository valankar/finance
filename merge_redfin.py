#!/usr/bin/env python3
"""Get estimated home values."""

import shutil
from datetime import datetime
from pathlib import Path

import backoff
import numpy as np
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from redfin import Redfin

import common

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


@backoff.on_exception(
    backoff.expo, (TypeError, KeyError, requests.exceptions.RequestException),
    max_time=300)
def get_redfin_estimate(url):
    """Get home value from Redfin."""
    client = Redfin()
    initial_info = client.initial_info(url)
    property_id = initial_info['payload']['propertyId']
    listing_id = initial_info['payload']['listingId']
    avm_details = client.avm_details(property_id, listing_id)
    value = float(avm_details['payload']['predictedValue'])
    return value


def get_redfin():
    """Get data from Redfin and create CSV files."""
    client = Redfin()
    now = datetime.now()
    for filename, url in URLS.items():
        csv = []
        initial_info = client.initial_info(url)
        property_id = initial_info['payload']['propertyId']
        listing_id = 1
        if 'listingId' in initial_info['payload']:
            listing_id = initial_info['payload']['listingId']
        avm_details = client.avm_historical(property_id, listing_id)
        for i, value in enumerate(
                reversed(avm_details['payload']['propertyTimeSeries'])):
            timestamp = now + relativedelta(months=-i)
            timestamp_str = timestamp.strftime('%Y-%m-%d')
            csv.append(f'{timestamp_str},{value}\n')
        csv.append(
            f'{PURCHASE_PRICES[filename][0]},{PURCHASE_PRICES[filename][1]}\n')
        csv.append('date,value\n')
        with open(f'{OUTPUT_DIR}{filename}.csv', 'w',
                  encoding='utf-8') as csv_file:
            csv_file.writelines(reversed(csv))


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
        # Only keep first and last values, i.e. purchase price and current estimate.
        home_df = pd.concat([home_df[:1],
                             home_df[-1:]]).resample('D').mean().interpolate()
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
    get_redfin()
    merge_redfin()


if __name__ == '__main__':
    main()
