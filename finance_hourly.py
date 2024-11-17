#!/usr/bin/env python3
"""Run hourly finance functions."""

import portalocker

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


def main():
    """Main."""
    with portalocker.Lock(common.LOCKFILE, timeout=common.LOCKFILE_TIMEOUT):
        ledger_amounts.main()
        etfs.main()
        index_prices.main()
        forex.main()
        schwab_ira.main()
        ledger_prices_db.main()
        history.main()
        push_web.main()
        graph_generator.generate_all_graphs.clear()
        graph_generator.generate_all_graphs(*MainGraphs.CACHE_CALL_ARGS)


if __name__ == "__main__":
    main()
