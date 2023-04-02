#!/usr/bin/env python3
"""Download fedfunds data."""

import pandas as pd
from fredapi import Fred

import authorization
import common


def get_fedfunds():
    """Download fedfunds rate data."""
    fred = Fred(api_key=authorization.FREDAPI_KEY)
    fedfunds_df = pd.DataFrame({'percent': fred.get_series('FEDFUNDS')})
    fedfunds_df.index.name = 'date'
    fedfunds_df.to_csv(common.PREFIX + 'fedfunds.csv')


if __name__ == '__main__':
    get_fedfunds()
