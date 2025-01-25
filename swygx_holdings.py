#!/usr/bin/env python3
"""Store holdings of SWYGX."""

import pandas as pd
from loguru import logger

import common


class GetHoldingsError(Exception):
    """Error getting holdings."""


def save_holdings():
    """Writes SWYGX holdings to swygx_holdings DB table."""
    with common.run_with_browser_page(
        "https://www.schwabassetmanagement.com/allholdings/SWYGX"
    ) as page:
        holdings = {}
        common.schwab_browser_page(page)
        for row in page.get_by_role("table").get_by_role("row").all()[1:]:
            holdings[row.get_by_role("cell").nth(1).inner_text()] = float(
                row.get_by_role("cell").nth(2).inner_text().strip("%")
            )

    if len(holdings) != 9:
        logger.error(
            f"Failed to get SWYGX holdings: only {len(holdings)} found: {holdings}"
        )
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
