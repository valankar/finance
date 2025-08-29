#!/usr/bin/env python3
"""Get estimated home values."""

import re
import typing
from datetime import datetime

import pandas as pd
from cyclopts import App, Parameter, Token
from dateutil import parser
from loguru import logger

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
        .resample("D")
        .last()
        .interpolate()
    )
    merged = (
        common.reduce_merge_asof([price_df, rent_df]).interpolate().sort_index(axis=1)
    )
    return merged.rolling("30D").mean()


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
):
    """Main."""
    if name:
        if p := get_property(name):
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
