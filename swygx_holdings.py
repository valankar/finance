#!/usr/bin/env python3
"""Store holdings of SWYGX."""

import pandas as pd

import common


class GetHoldingsError(Exception):
    """Error getting holdings."""


def browser_func(page):
    """Get market cap data from Schwab."""
    table_dict = {}
    common.schwab_browser_page(page)
    table = (
        page.locator('//*[@id="block-sch-beacon-csim-content"]/div/div/div/div/table')
        .get_by_role("row")
        .all()
    )
    for row in table[1:]:
        _, etf, percent, _ = row.inner_text().split("\t")
        table_dict[etf] = float(percent.strip("%"))
    return table_dict


def save_holdings():
    """Writes SWYGX holdings to swygx_holdings DB table."""
    holdings = common.run_in_browser_page(
        "https://www.schwabassetmanagement.com/allholdings/SWYGX", browser_func
    )
    if len(holdings) != 9:
        print(f"Failed to get SWYGX holdings: only {len(holdings)} found")
        raise GetHoldingsError
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
