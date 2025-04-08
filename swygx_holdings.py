#!/usr/bin/env python3
"""Store holdings of SWYGX."""

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
        text = page.get_by_role("cell", name="SCHX").inner_text()
        logger.info(f"Found {text=}")
        for row in page.get_by_role("table").get_by_role("row").all()[1:]:
            holdings[row.get_by_role("cell").nth(1).inner_text()] = float(
                row.get_by_role("cell").nth(2).inner_text().strip("%")
            )

    expected_tickers = set(common.read_sql_last("swygx_holdings").columns)
    found_tickers = set(holdings)
    if found_tickers != expected_tickers:
        logger.error(f"Failed: {expected_tickers=} {found_tickers=} {holdings=}")
        logger.error(
            f"Symmetric difference: {expected_tickers.symmetric_difference(found_tickers)}"
        )
        raise GetHoldingsError
    common.insert_sql("swygx_holdings", holdings)


def main():
    """Main."""
    save_holdings()


if __name__ == "__main__":
    main()
