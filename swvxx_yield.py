#!/usr/bin/env python3
"""Store Schwab SWVXX 7-day yield history."""

from datetime import date
import pandas as pd

import common

SWVXX_CSV = common.PREFIX + 'swvxx_yield.csv'


def get_yield():
    """Get 7 day yield with Selenium."""
    return float(
        common.find_xpath_via_browser(
            'https://www.schwabassetmanagement.com/products/swvxx',
            '//*[@id="sfm-table--yields"]/table/tbody/tr[1]/td[2]').strip('%'))


def main():
    """Writes 7 day yield history to CSV file."""
    new_df = pd.DataFrame({'percent': get_yield()}, index=[date.today()])
    new_df.to_csv(SWVXX_CSV, mode='a', header=False, float_format='%.2f')


if __name__ == '__main__':
    main()
