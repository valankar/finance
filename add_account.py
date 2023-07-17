#!/usr/bin/env python3
"""Add an account for account history. Be sure to update history.py."""

import numpy as np

import common

accounts_df = common.read_sql_table("account_history")
# Update account here.
accounts_df.insert(0, "USD_Charles Schwab_IRA_SWYGX", np.nan)
accounts_df = accounts_df.sort_index(axis=1)
common.to_sql(accounts_df, "account_history", if_exists="replace")
