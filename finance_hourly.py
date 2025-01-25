#!/usr/bin/env python3
"""Run hourly finance functions."""

import argparse

import portalocker

import brokerages
import common
import etfs
import forex
import graph_generator
import history
import index_prices
import ledger_amounts
import ledger_prices_db
import push_web
import schwab_ira
from app import MainGraphs


@common.cache_half_hourly_decorator
def run_all(graphs_only: bool = False):
    if not graphs_only:
        ledger_amounts.main()
        etfs.main()
        index_prices.main()
        forex.main()
        schwab_ira.main()
        ledger_prices_db.main()
        history.main()
        brokerages.main()
        push_web.main()
    graph_generator.clear_and_generate(MainGraphs.CACHE_CALL_ARGS)


def main():
    """Main."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--graphs-only", default=False, action=argparse.BooleanOptionalAction
    )
    args = parser.parse_args()
    with portalocker.Lock(common.LOCKFILE, timeout=common.LOCKFILE_TIMEOUT):
        run_all(graphs_only=args.graphs_only)


if __name__ == "__main__":
    main()
