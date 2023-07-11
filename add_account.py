#!/usr/bin/env python3
"""Add an account for account history. Be sure to update history.py."""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

import common

with create_engine(common.SQLITE_URI).connect() as conn:
    accounts_df = pd.read_sql_table("account_history", conn, index_col="date")
    # Update account here.
    accounts_df.insert(0, "USD_Charles Schwab_IRA_SWYGX", np.nan)
    accounts_df = accounts_df.sort_index(axis=1)
    accounts_df.to_sql("account_history", conn, if_exists="replace", index_label="date")
    conn.commit()
