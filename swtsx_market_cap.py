#!/usr/bin/env python3
"""Get market caps of SWTSX."""

import math

import common

CAP_MAP = {
    "US_LARGE_CAP": ["> $70,000 M", "$15,000-$70,000 M"],
    "US_SMALL_CAP": ["<$1,000 M", "$1,000-$3,000 M", "$3,000-$15,000 M"],
}


class ToleranceError(Exception):
    """Percents do not add up close to 100."""


def get_market_cap(page):
    """Get market cap data from Schwab."""
    table_dict = {}
    common.schwab_browser_page(page)
    page.get_by_role("heading", name="Portfolio", exact=True).click()
    for _, keys in CAP_MAP.items():
        for cap in keys:
            table_dict[cap] = float(
                page.locator(f'tr:has-text("{cap}")')
                .inner_text()
                .split("\t\n")[1]
                .strip("%")
            )
    return table_dict


def save_market_cap():
    """Writes SWTSX market cap weightings to swtsx_market_cap DB table."""
    with common.run_with_browser_page(
        "https://www.schwabassetmanagement.com/products/swtsx"
    ) as page:
        table_dict = get_market_cap(page)
    if not math.isclose(sum(table_dict.values()), 100, rel_tol=0.01):
        raise ToleranceError()
    market_cap_dict = {}
    # For determining large vs small cap, compare:
    # https://www.schwabassetmanagement.com/products/scha
    # https://www.schwabassetmanagement.com/products/swtsx
    for cap, keys in CAP_MAP.items():
        market_cap_dict[cap] = sum(table_dict[x] for x in keys)
    common.insert_sql("swtsx_market_cap", market_cap_dict)


def main():
    """Main."""
    save_market_cap()


if __name__ == "__main__":
    main()
