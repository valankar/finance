#!/usr/bin/env python3
"""Get Schwab IRA value."""

import common

TABLE_PREFIX = "schwab_ira"
CSV_OUTPUT_PATH = f"{common.PREFIX}schwab_ira_values.csv"


def get_ticker():
    """Get ticker price from Schwab via Selenium."""
    return float(
        common.find_xpath_via_browser(
            "https://www.schwabassetmanagement.com/products/stir?product=swygx",
            '//*[@id="sfm-table--fundprofile"]/table/tbody/tr[5]/td[2]',
            execute_before=common.schwab_browser_execute_before,
        ).strip("$")
    )


def main():
    """Main."""
    common.write_ticker_csv(
        f"{TABLE_PREFIX}_amounts",
        f"{TABLE_PREFIX}_prices",
        CSV_OUTPUT_PATH,
        ticker_prices={"SWYGX": common.get_ticker("SWYGX")},
    )


if __name__ == "__main__":
    main()
