#!/usr/bin/env python3
"""Run hourly finance functions."""

import os
from concurrent.futures import ThreadPoolExecutor

import redis
from cyclopts import App

import brokerages
import common
import etfs
import forex
import history
import index_prices
import ledger_amounts
import ledger_prices_db
import main_graphs
import main_matplot
import push_web
import schwab_ira
import stock_options

app = App()


@app.default
def run_all(
    calculate: bool = True,
    matplot: bool = True,
    plotly: bool = True,
):
    with common.WalrusDb().db.lock(
        common.SCRIPT_LOCK_NAME, ttl=common.LOCK_TTL_SECONDS * 1000
    ):
        if calculate:
            common.cache_ticker_prices()
            stock_options.generate_options_data()
            ledger_amounts.main()
            etfs.main()
            index_prices.main()
            forex.main()
            schwab_ira.main()
            ledger_prices_db.main()
            history.main()
            brokerages.main()
            push_web.main()
        db = common.WalrusDb().db
        m = main_matplot.Matplots(db)
        p = main_graphs.MainGraphs(db)
        with ThreadPoolExecutor() as e:
            if matplot:
                e.submit(m.generate)
            if plotly:
                e.submit(p.generate)
        redis.Redis(host=os.environ.get("REDIS_HOST", "localhost")).bgsave()


if __name__ == "__main__":
    app()
