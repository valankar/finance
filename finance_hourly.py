#!/usr/bin/env python3
"""Run hourly finance functions."""

from concurrent.futures import ProcessPoolExecutor
from datetime import datetime

import humanize
from cyclopts import App
from loguru import logger

import brokerages
import common
import etfs
import finance_daily
import forex
import history
import ledger_prices_db
import main_graphs
import main_matplot


class MethodFailed(Exception):
    """One of the methods failed with exception."""


app = App()


@app.default
def run_all(
    calculate: bool = True,
    daily: bool = True,
    matplot: bool = True,
    plotly: bool = True,
):
    start_time = datetime.now()
    failed = False
    with common.walrus_db.db.lock(
        common.SCRIPT_LOCK_NAME, ttl=common.LOCK_TTL_SECONDS * 1000
    ):
        try:
            if calculate:
                # Cache all tickers
                common.get_tickers(etfs.get_tickers() | set(forex.TICKERS))
                etfs.main()
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
        except Exception:
            logger.exception("Hourly method failed")
            failed = True
        try:
            if daily:
                logger.info("Running daily methods")
                finance_daily.run_all()
        except Exception:
            logger.exception("Daily method failed")
            failed = True
    logger.info(
        f"Total time for run: {humanize.precisedelta(datetime.now() - start_time)}"
    )
    if failed:
        raise MethodFailed("One method failed")


if __name__ == "__main__":
    app()
