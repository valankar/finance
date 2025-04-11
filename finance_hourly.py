#!/usr/bin/env python3
"""Run hourly finance functions."""

import argparse
import pickle

import brokerages
import common
import etfs
import forex
import history
import index_prices
import ledger_amounts
import ledger_prices_db
import main_graphs
import push_web
import schwab_ira
import stock_options_ui


def run_all(graphs_only: bool = False):
    if not graphs_only:
        common.cache_ticker_prices()
        ledger_amounts.main()
        etfs.main()
        index_prices.main()
        forex.main()
        schwab_ira.main()
        ledger_prices_db.main()
        history.main()
        brokerages.main()
        push_web.main()
    common.WalrusDb().db[main_graphs.MainGraphs.REDIS_KEY] = pickle.dumps(
        main_graphs.generate_all_graphs()
    )
    common.WalrusDb().db[stock_options_ui.StockOptionsPage.REDIS_KEY] = pickle.dumps(
        stock_options_ui.generate_options_data()
    )


def main():
    """Main."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--graphs-only", default=False, action=argparse.BooleanOptionalAction
    )
    args = parser.parse_args()
    with common.WalrusDb().db.lock(
        common.SCRIPT_LOCK_NAME, ttl=common.LOCK_TTL_SECONDS * 1000
    ):
        run_all(graphs_only=args.graphs_only)


if __name__ == "__main__":
    main()
