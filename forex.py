#!/usr/bin/env python3
"""Write forex history."""

import common

TICKERS = ("CHFUSD", "SGDUSD")


def main():
    """Get and update forex data."""
    q = common.get_tickers(TICKERS)
    common.insert_sql("forex", {k: v for k, v in q.items() if k in TICKERS})


if __name__ == "__main__":
    main()
