#!/usr/bin/env python3
"""Run hourly finance functions."""

from datetime import datetime

from selenium.common.exceptions import NoSuchElementException

import commodities
import etfs
import history
import i_and_e
import plot
import vanguard_401k
import vanguard_trust


def main():
    """Main."""
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


if __name__ == "__main__":
    main()
