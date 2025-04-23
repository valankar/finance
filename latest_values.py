#!/usr/bin/env python3

from datetime import timedelta

import pandas as pd

import common


def difference_df(df: pd.DataFrame):
    yesterday = df.index[-1] - timedelta(days=1)
    closest_idx = df.index.get_indexer([yesterday], method="nearest")
    df = pd.concat([df.iloc[closest_idx], df.iloc[-1:]])
    return df


def main():
    history = common.read_sql_table("history")
    df = difference_df(history)
    print(df)
    print("\nDifference:")
    print(df.diff().iloc[-1:])


if __name__ == "__main__":
    main()
