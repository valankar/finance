#!/usr/bin/env python3
"""Store index prices."""

import common


def main():
    """Main."""
    common.write_ticker_sql("index_prices", "index_prices")


if __name__ == "__main__":
    main()
