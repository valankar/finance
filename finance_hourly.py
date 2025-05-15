#!/usr/bin/env python3
"""Run hourly finance functions."""

import os
from concurrent.futures import ProcessPoolExecutor

import valkey
from cyclopts import App
from loguru import logger

import brokerages
import common
import etfs
import forex
import history
import index_prices
import ledger_prices_db
import main_graphs
import main_matplot
import push_web
import stock_options
import stock_options_ui

app = App()


@app.default
async def run_all(
    calculate: bool = True,
    matplot: bool = True,
    plotly: bool = True,
):
    with common.WalrusDb().db.lock(
        common.SCRIPT_LOCK_NAME, ttl=common.LOCK_TTL_SECONDS * 1000
    ):
        if calculate:
            stock_options.generate_options_data()
            etfs.main()
            index_prices.main()
            forex.main()
            ledger_prices_db.main()
            history.main()
            brokerages.main()
            push_web.main()
        with ProcessPoolExecutor() as e:
            results = []
            if plotly:
                results.append(e.submit(main_graphs.main))
            if matplot:
                results.append(e.submit(main_matplot.main))
                results.append(e.submit(stock_options_ui.main))
        for r in results:
            if o := r.result():
                logger.info(o)
        valkey.Valkey(host=os.environ.get("REDIS_HOST", "localhost")).bgsave()


if __name__ == "__main__":
    app()
