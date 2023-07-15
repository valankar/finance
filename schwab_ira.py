#!/usr/bin/env python3
"""Get Schwab IRA value."""

import common

TABLE_PREFIX = "schwab_ira"
CSV_OUTPUT_PATH = f"{common.PREFIX}schwab_ira_values.csv"


def main():
    """Main."""
    # '//*[@id="sfm-table--fundprofile"]/table/tbody/tr[5]/td[2]'
    ticker_prices = {
        "SWYGX": float(
            common.find_xpath_via_browser(
                "https://www.schwabassetmanagement.com/products/stir?product=swygx",
                # pylint: disable-next=line-too-long
                '//*[@id="sfm-table--fundprofile"]/table/tbody/tr[5]/td[2]',
            ).strip("$")
        )
    }
    common.write_ticker_csv(
        f"{TABLE_PREFIX}_amounts",
        f"{TABLE_PREFIX}_prices",
        CSV_OUTPUT_PATH,
        ticker_prices=ticker_prices,
    )


if __name__ == "__main__":
    main()
