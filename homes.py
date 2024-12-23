#!/usr/bin/env python3
"""Get estimated home values."""

import re
import statistics

import pandas as pd
from retry import retry

import common


class ZillowTextSearchError(Exception):
    """Error searching Zillow text."""


def integerize_value(value):
    """Removes $ and , from value."""
    return int(value.translate(str.maketrans("", "", "$,")))


@retry(tries=4, delay=30)
def get_redfin_estimate(url_path):
    """Get home value from Redfin."""
    with common.run_with_browser_page(f"https://www.redfin.com{url_path}") as page:
        return integerize_value(
            page.get_by_text(re.compile(r"^\$[\d,]+$")).all()[0].inner_text()
        )


@retry(tries=4, delay=30)
def get_zillow_estimates(url_path):
    """Get home and rent value from Zillow."""
    with common.run_with_browser_page(f"https://www.zillow.com{url_path}") as page:
        if matches := re.search(
            r"Zestimate.+: \$([\d,]+).*Rent Zestimate.+: \$([\d,]+)",
            page.get_by_test_id("summary").inner_text(),
        ):
            return [integerize_value(matches[1]), integerize_value(matches[2])]
        else:
            raise ZillowTextSearchError()


def write_prices_table(name, redfin, zillow):
    """Write prices to sqlite."""
    home_df = pd.DataFrame(
        {"name": name, "redfin_value": redfin, "zillow_value": zillow},
        index=[pd.Timestamp.now()],
    )
    common.to_sql(home_df, "real_estate_prices", foreign_key=True)


def write_rents_table(name, value):
    """Write rents to sqlite."""
    home_df = pd.DataFrame({"name": name, "value": value}, index=[pd.Timestamp.now()])
    common.to_sql(home_df, "real_estate_rents", foreign_key=True)


def main():
    """Main."""
    for p in common.PROPERTIES:
        redfin = get_redfin_estimate(p.redfin_url)
        zillow, zillow_rent = get_zillow_estimates(p.zillow_url)
        average = round(statistics.mean([redfin, zillow]))
        if not average:
            print(f"Found 0 average price for {p.name}")
            continue
        with common.temporary_file_move(f"{common.PREFIX}{p.file}") as output_file:
            output_file.write(str(average))
        write_prices_table(p.name, redfin, zillow)

        # Home rent estimate
        write_rents_table(
            p.name,
            zillow_rent,
        )


if __name__ == "__main__":
    main()
