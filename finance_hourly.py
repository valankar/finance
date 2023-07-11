#!/usr/bin/env python3
"""Run hourly finance functions."""

from timeit import default_timer as timer

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
    with open(f"{common.PREFIX}{plot.INDEX_HTML}", "a", encoding="utf-8") as index_html:
        index_html.write(
            f"<PRE>Execution time: {round(end_time-start_time, 3)} seconds.</PRE>"
        )


if __name__ == "__main__":
    main()
