#!/usr/bin/env python3
"""Run hourly finance functions."""

import os
import sys
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime

import humanize
import requests
from authlib.integrations.base_client.errors import OAuthError
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

app = App()


def schwab_auth_status(success: bool):
    suffix = "" if success else "/fail"
    if hc := os.getenv("HEALTHCHECKS_IO_SCHWAB_AUTH"):
        requests.get(f"{hc}{suffix}", timeout=10)


@dataclass
class TaskResult:
    failed: bool = False


@contextmanager
def task_guard(task_name: str, check_schwab: bool = False):
    """Context manager to handle task execution with error handling."""
    result = TaskResult()
    try:
        yield result
        if check_schwab:
            schwab_auth_status(True)
    except OAuthError:
        if check_schwab:
            schwab_auth_status(False)
        logger.exception(f"{task_name} - Schwab auth failure")
        result.failed = True
    except Exception:
        if check_schwab:
            schwab_auth_status(True)
        logger.exception(f"{task_name} failed")
        result.failed = True


@app.default
async def run_all(
    calculate: bool = True,
    daily: bool = True,
    matplot: bool = True,
    plotly: bool = True,
):
    start_time = datetime.now()
    logger.configure(
        handlers=[
            {"sink": sys.stderr, "backtrace": False},
            {
                "sink": common.HOURLY_LOGFILE,
                "mode": "w",
                "enqueue": True,
                "backtrace": False,
            },
        ]
    )
    with common.walrus_db.db.lock(
        common.SCRIPT_LOCK_NAME, ttl=common.LOCK_TTL_SECONDS * 1000
    ):
        with task_guard("Hourly calculations", check_schwab=True) as hourly_result:
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

        with task_guard("Daily calculations") as daily_result:
            if daily:
                await finance_daily.run_all()

    logger.info(
        f"Total time for run: {humanize.precisedelta(datetime.now() - start_time)}"
    )
    if any([hourly_result.failed, daily_result.failed]):
        raise SystemExit("One method failed")
    if hc := os.getenv("HEALTHCHECKS_IO_HOURLY"):
        requests.get(hc, timeout=10)


if __name__ == "__main__":
    app()
