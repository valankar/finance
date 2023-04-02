#!/usr/bin/env python3
"""Add an account for account history. Be sure to update history.py."""

import pandas as pd
import numpy as np

import common

accounts_df = pd.read_csv(f'{common.PREFIX}account_history.csv',
                          index_col=0,
                          parse_dates=True,
                          infer_datetime_format=True,
                          header=[0, 1, 2, 3])

accounts_df.insert(0, ('USD', 'Bank', 'Account', 'nan'), np.nan)
accounts_df = accounts_df.sort_index(axis=1)
accounts_df.to_csv(f'{common.PREFIX}account_history.csv.new',
                   float_format='%.2f')
