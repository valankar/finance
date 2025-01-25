#!/usr/bin/env python3
"""Get estimated home values."""

import re
import statistics
import typing

import pandas as pd
from loguru import logger
from playwright.sync_api import TimeoutError

import common


class Property(typing.NamedTuple):
    name: str
    file: str
    redfin_url: str
    zillow_url: str
    address: str

PROPERTIES = (
    Property(
        name="Some Real Estate",
        file="prop1.txt",
        redfin_url="URL",
        zillow_url="URL",
        address="ADDRESS",
    ),
)


def get_property(name: str) -> Property | None:
    for p in PROPERTIES:
        if p.name == name:
            return p
    return None


def integerize_value(value):
    return int(re.sub(r"\D", "", value))


@common.cache_daily_decorator
def get_redfin_estimate(url_path):
    """Get home value from Redfin."""
    logger.info(f"Getting Redfin estimate for {url_path}")
    with common.run_with_browser_page(f"https://www.redfin.com{url_path}") as page:
        return integerize_value(
            page.get_by_text(re.compile(r"^\$[\d,]+$")).all()[0].inner_text()
        )


@common.cache_daily_decorator
def get_zillow_estimates(url_path):
    """Get home and rent value from Zillow."""
    logger.info(f"Getting Zillow estimate for {url_path}")
    with common.run_with_browser_page(f"https://www.zillow.com{url_path}") as page:
        try:
            price = page.get_by_test_id("price").get_by_text("$").inner_text()
            rent = page.get_by_test_id("rent-zestimate").inner_text()
        except TimeoutError:
            logger.info("Got timeout error, trying with different matching")
            if matches := re.search(
                r"Zestimate.+: \$([\d,]+).*Rent Zestimate.+: \$([\d,]+)",
                page.get_by_test_id("summary").inner_text(),
            ):
                price = matches[1]
                rent = matches[2]
        return [integerize_value(price), integerize_value(rent)]


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


def process_home(p: Property) -> bool:
    redfin = get_redfin_estimate(p.redfin_url)
    zillow, zillow_rent = get_zillow_estimates(p.zillow_url)
    average = round(statistics.mean([redfin, zillow]))
    if not average:
        logger.error(f"Found 0 average price for {p.name}")
        return False
    with common.temporary_file_move(f"{common.PREFIX}{p.file}") as output_file:
        output_file.write(str(average))
    write_prices_table(p.name, redfin, zillow)
    write_rents_table(
        p.name,
        zillow_rent,
    )
    return True


def main():
    """Main."""
    for p in PROPERTIES:
        process_home(p)


if __name__ == "__main__":
    main()
