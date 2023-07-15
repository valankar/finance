#!/usr/bin/env python3
"""Show last get ticker failures."""

import shelve

import pytz

import common

with shelve.open(common.TICKER_FAILURES_SHELF, flag="r") as ticker_failures:
    for func in sorted(ticker_failures, key=ticker_failures.get, reverse=True):
        date = ticker_failures[func]
        print(
            f'{func}: {date.astimezone(pytz.timezone("Europe/Zurich")).strftime("%c")}'
        )
