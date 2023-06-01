#!/usr/bin/env python3
"""Run hourly finance functions."""

from datetime import datetime
from timeit import default_timer as timer

from selenium.common.exceptions import NoSuchElementException

import commodities
import common
import etfs
import history
import i_and_e
import plot
import vanguard_401k
import vanguard_trust


def main():
    """Main."""
    start_time = timer()
    try:
        vanguard_trust.main()
    except NoSuchElementException as ex:
        # Be silent on weekends when this sometimes fails.
        if datetime.today().strftime("%A") not in ["Saturday", "Sunday"]:
            print(f"{ex} Vanguard Trust exception raised but continuing.")

    vanguard_401k.main()
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
