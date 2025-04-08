#!/usr/bin/env python3
"""Get estimated home values."""

import re
import typing
from datetime import datetime

import pandas as pd
from cyclopts import App, Parameter, Token
from dateutil import parser
from loguru import logger
from playwright.sync_api import TimeoutError

import common


class Property(typing.NamedTuple):
    name: str
    redfin_url: str
    zillow_url: str
    address: str


PROPERTIES = (
    Property(
        name="Some Real Estate",
        redfin_url="URL",
        zillow_url="URL",
        address="ADDRESS",
    ),
)


def get_real_estate_df() -> pd.DataFrame:
    df = common.read_sql_table("real_estate_prices")
    redfin = (
        df.query("site == 'Redfin'")
        .drop(columns="site")
        .pivot_table(index="date", columns="name", values="value")
        .resample("D")
        .last()
        .interpolate()
    )
    zillow = (
        df.query("site == 'Zillow'")
        .drop(columns="site")
        .pivot_table(index="date", columns="name", values="value")
        .resample("D")
        .last()
        .interpolate()
    )
    taxes = (
        df.query("site == 'Taxes'")
        .drop(columns="site")
        .pivot_table(index="date", columns="name", values="value")
        .add_suffix(" Taxes")
        .resample("D")
        .last()
        .interpolate()
    )
    merged = pd.merge_asof(
        redfin,
        zillow,
        left_index=True,
        right_index=True,
        suffixes=(" Redfin", " Zillow"),
    )
    merged = pd.merge_asof(
        merged,
        taxes,
        left_index=True,
        right_index=True,
    ).interpolate()
    for p in PROPERTIES:
        merged[p.name] = merged[
            [f"{p.name} Redfin", f"{p.name} Zillow", f"{p.name} Taxes"]
        ].mean(axis=1)
    price_df = merged[[p.name for p in PROPERTIES]].add_suffix(" Price")
    rent_df = (
        common.read_sql_table("real_estate_rents")
        .pivot_table(index="date", columns="name", values="value")
        .add_suffix(" Rent")
    )
    return (
        common.reduce_merge_asof([price_df, rent_df]).interpolate().sort_index(axis=1)
    )


def get_property(name: str) -> Property | None:
    for p in PROPERTIES:
        if p.name == name:
            return p
    return None


def integerize_value(value):
    return int(re.sub(r"\D", "", value))


def get_redfin_estimate(url_path):
    """Get home value from Redfin."""
    logger.info(f"Getting Redfin estimate for {url_path}")
    with common.run_with_browser_page(f"https://www.redfin.com{url_path}") as page:
        return integerize_value(
            page.get_by_text(re.compile(r"^\$[\d,]+$")).all()[0].inner_text()
        )


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


def write_prices_table(name, value, site, timestamp: typing.Optional[datetime] = None):
    """Write prices to sql."""
    common.insert_sql(
        "real_estate_prices", {"name": name, "value": value, "site": site}, timestamp
    )


def write_rents_table(name, value, site):
    """Write rents to sql."""
    common.insert_sql("real_estate_rents", {"name": name, "value": value, "site": site})


def process_redfin(p: Property):
    redfin = get_redfin_estimate(p.redfin_url)
    write_prices_table(p.name, redfin, "Redfin")


def process_zillow(p: Property):
    zillow, zillow_rent = get_zillow_estimates(p.zillow_url)
    write_prices_table(p.name, zillow, "Zillow")
    write_rents_table(p.name, zillow_rent, "Zillow")


def do_fetch_prices(p: Property):
    process_redfin(p)
    process_zillow(p)


app = App()


def comma_separated(type_, tokens: typing.Sequence[Token]) -> int:
    return type_(tokens[0].value.replace(",", ""))


type CommaInt = typing.Annotated[int, Parameter(converter=comma_separated)]


@app.default
def main(
    name: typing.Optional[str] = None,
    redfin_price: typing.Optional[CommaInt] = None,
    zillow_price: typing.Optional[CommaInt] = None,
    zillow_rent: typing.Optional[CommaInt] = None,
    taxes_price: typing.Optional[CommaInt] = None,
    date: typing.Optional[str] = None,
    fetch_prices: bool = False,
):
    """Main."""
    if name:
        if p := get_property(name):
            if fetch_prices:
                return do_fetch_prices(p)
            timestamp = None
            if date:
                timestamp = parser.parse(date)
            for arg, site in (
                (redfin_price, "Redfin"),
                (zillow_price, "Zillow"),
                (taxes_price, "Taxes"),
            ):
                if arg:
                    write_prices_table(p.name, arg, site, timestamp)
            if zillow_rent:
                write_rents_table(p.name, zillow_rent, "Zillow")
        else:
            print(f"Property {name} is unknown")


if __name__ == "__main__":
    app()
