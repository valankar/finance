#!/usr/bin/env python3
"""Write brokerage balance history to database."""

from typing import cast

import pandas as pd
from loguru import logger

import common
import margin_loan

TABLE_NAME = "brokerage_totals"


def load_df() -> pd.DataFrame:
    """Load the total brokerage dataframe."""
    df = (
        common.read_sql_table(TABLE_NAME).pivot(columns="Brokerage").xs("Total", axis=1)
    )[[x.name for x in margin_loan.LOAN_BROKERAGES]]
    return cast(pd.DataFrame, df)


def main():
    dfs = []
    now = pd.Timestamp.now()
    for brokerage in margin_loan.LOAN_BROKERAGES:
        df = margin_loan.get_balances_broker(brokerage)
        df["Brokerage"] = brokerage.name
        df["date"] = now
        df = df.set_index("date")
        dfs.append(df)
    if not dfs:
        logger.error("No brokerage data found.")
        return
    common.to_sql(pd.concat(dfs), TABLE_NAME)


if __name__ == "__main__":
    main()
