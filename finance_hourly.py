#!/usr/bin/env python3
"""Run hourly finance functions."""

from datetime import datetime
from timeit import default_timer as timer

import pytz

import commodities
import common
import etfs
import history
import i_and_e
import plot
import schwab_ira


def main():
    """Main."""
    start_time = timer()
    schwab_ira.main()
    commodities.main()
    etfs.main()
    history.main()
    plot.main()
    i_and_e.main()
    end_time = timer()
    for output_file in [plot.INDEX_HTML, plot.STATIC_HTML]:
        elapsed = round(end_time - start_time, 3)
        now = datetime.now().astimezone(pytz.timezone("Europe/Zurich")).strftime("%c")
        with open(f"{common.PREFIX}{output_file}", "a", encoding="utf-8") as index_html:
            index_html.write(
                f"<PRE>Execution time: {elapsed} seconds. Last updated: {now}</PRE>\n"
            )


if __name__ == "__main__":
    main()
