#!/usr/bin/env python3
"""Store holdings of SWYGX."""

import pandas as pd

import common


def save_holdings():
    """Writes SWYGX holdings to swygx_holdings DB table."""
    table = common.find_xpath_via_browser(
        "https://www.schwabassetmanagement.com/allholdings/SWYGX",
        '//*[@id="block-sch-beacon-csim-content"]/div/div/div/div',
    ).split("\n")
    holdings = {}
    for line in table:
        line_split = line.split()
        if "%" in line_split[-2]:
            holdings[line_split[-3]] = float(line_split[-2].strip("%"))
    holdings_df = pd.DataFrame(
        holdings,
        index=[pd.Timestamp.now()],
    )
    common.to_sql(holdings_df, "swygx_holdings")


def main():
    """Main."""
    save_holdings()


if __name__ == "__main__":
    main()
