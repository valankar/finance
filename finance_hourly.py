#!/usr/bin/env python3
"""Run hourly finance functions."""

import os
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime

import humanize
import valkey
from cyclopts import App
from loguru import logger

import brokerages
import common
import etfs
import finance_daily
import forex
import history
import index_prices
import ledger_prices_db
import main_graphs
import main_matplot

app = App()


@app.default
def run_all(
    calculate: bool = True,
    daily: bool = True,
    flush_cache: bool = True,
    matplot: bool = True,
    plotly: bool = True,
):
    start_time = datetime.now()
    with common.walrus_db.db.lock(
        common.SCRIPT_LOCK_NAME, ttl=common.LOCK_TTL_SECONDS * 1000
    ):
        if flush_cache:
            common.walrus_db.cache.flush()
            common.get_tickers(etfs.get_tickers() | {"^SPX", "CHFUSD=X", "SGDUSD=X"})
        if calculate:
            etfs.main()
            index_prices.main()
            forex.main()
            ledger_prices_db.main()
            history.main()
            brokerages.main()
        with ProcessPoolExecutor() as e:
            results = []
            if plotly:
                results.append(e.submit(main_graphs.main))
            if matplot:
                results.append(e.submit(main_matplot.main))
            for r in results:
                if o := r.result():
                    logger.info(o)
        valkey.Valkey(host=os.environ.get("REDIS_HOST", "localhost")).bgsave()
        if daily:
            finance_daily.run_all()
    logger.info(
        f"Total time for run: {humanize.precisedelta(datetime.now() - start_time)}"
    )


if __name__ == "__main__":
    app()
