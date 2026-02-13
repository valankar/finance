#!/usr/bin/env python3
"""Write forex history."""

import common


def main():
    """Get and update forex data."""
    ts = ("CHFUSD", "SGDUSD")
    q = common.get_tickers(ts)
    common.insert_sql("forex", {k: v for k, v in q.items() if k in ts})


if __name__ == "__main__":
    main()
