#!/usr/bin/env python3
"""Get market caps of SWTSX."""

import pandas as pd

import common


def reduce_sum_percents(string_values):
    """Sum values in list of percentage strings."""
    return sum(map(lambda x: float(x.strip("%")), string_values))


def browser_func(page):
    """Get market cap data from Schwab."""
    table_dict = {}
    common.schwab_browser_page(page)
    page.get_by_role("heading", name="Portfolio", exact=True).click()
    table = page.locator('//*[@id="marketcap"]').get_by_role("row").all()
    for row in table[1:]:
        market_cap, percent = row.inner_text().split("\t\n")
        table_dict[market_cap] = percent
    return table_dict


def save_market_cap():
    """Writes SWTSX market cap weightings to swtsx_market_cap DB table."""
    table_dict = common.run_in_browser_page(
        "https://www.schwabassetmanagement.com/products/swtsx", browser_func
    )
    market_cap_dict = {}
    # For determining large vs small cap, compare:
    # https://www.schwabassetmanagement.com/products/scha
    # https://www.schwabassetmanagement.com/products/swtsx
    market_cap_dict["US_LARGE_CAP"] = reduce_sum_percents(
        [table_dict["> $70,000 M"], table_dict["$15,000-$70,000 M"]]
    )
    market_cap_dict["US_SMALL_CAP"] = reduce_sum_percents(
        [
            table_dict["<$1,000 M"],
            table_dict["$1,000-$3,000 M"],
            table_dict["$3,000-$15,000 M"],
        ]
    )
    market_cap_df = pd.DataFrame(
        market_cap_dict,
        index=[pd.Timestamp.now()],
    )
    common.to_sql(market_cap_df, "swtsx_market_cap")


def main():
    """Main."""
    save_market_cap()


if __name__ == "__main__":
    main()
