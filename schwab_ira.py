#!/usr/bin/env python3
"""Get Schwab IRA value."""

import common

TABLE_PREFIX = "schwab_ira"
CSV_OUTPUT_PATH = f"{common.PREFIX}schwab_ira_values.csv"


def main():
    """Main."""
    # Get fund price from Morningstar as it seems updated more frequently than Yahoo.
    ticker_prices = {
        "SWYGX": float(
            common.find_xpath_via_browser(
                "https://www.morningstar.com/funds/XNAS/SWYGX/quote",
                # pylint: disable-next=line-too-long
                '//*[@id="__layout"]/div/div/div[2]/div[3]/div/div/main/div/div/div[1]/section[1]/ul/li[1]/span[2]/span[1]',
            )
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
