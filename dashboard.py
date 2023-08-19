#!/usr/bin/env python3
"""Dash app."""

import plotly.io as pio
from cachetools.func import ttl_cache
from dash import (
    Dash,
    Input,
    Output,
    Patch,
    callback,
    ctx,
    dcc,
    html,
    page_container,
    register_page,
)

import common
import i_and_e
import plot

INITIAL_TIMERANGE = "1y"
SUBPLOT_MARGIN = {"l": 0, "r": 50, "b": 0, "t": 50}


def make_range_buttons(name):
    """Buttons for selecting time range."""
    tab_container_style = {"width": "50vw", "margin": "auto"}
    tab_style = {"padding": "0px"}
    return dcc.Tabs(
        id=name,
        value=INITIAL_TIMERANGE,
        children=[
            dcc.Tab(
                label="All",
                value="All",
                style=tab_style,
                selected_style=tab_style,
            ),
            dcc.Tab(
                label="2y",
                value="2y",
                style=tab_style,
                selected_style=tab_style,
            ),
            dcc.Tab(
                label="1y",
                value="1y",
                style=tab_style,
                selected_style=tab_style,
            ),
            dcc.Tab(
                label="YTD",
                value="YTD",
                style=tab_style,
                selected_style=tab_style,
            ),
            dcc.Tab(
                label="6m",
                value="6m",
                style=tab_style,
                selected_style=tab_style,
            ),
            dcc.Tab(
                label="3m",
                value="3m",
                style=tab_style,
                selected_style=tab_style,
            ),
            dcc.Tab(
                label="1m",
                value="1m",
                style=tab_style,
                selected_style=tab_style,
            ),
        ],
        colors={"background": "black", "primary": "white"},
        style=tab_container_style,
    )


@ttl_cache
def load_all_df(frequency):
    """Load all dataframe."""
    return common.load_sqlite_and_rename_col(
        "history", extra_cols=["total", "total_no_homes"], frequency=frequency
    )


@ttl_cache
def load_invret_df(frequency):
    """Load investing & retirement dataframe."""
    return plot.get_investing_retirement_df(load_all_df(frequency))


@ttl_cache
def load_real_estate_df(frequency):
    """Load real estate dataframe."""
    # Fix issue with missing datapoints.
    match frequency:
        case "weekly":
            resample = "W"
        case "daily":
            resample = "D"
    return (
        common.get_real_estate_df(frequency=frequency)
        .resample(resample)
        .last()
        .interpolate()
    )


@ttl_cache
def load_prices_df(frequency):
    """Load prices dataframe."""
    return plot.reduce_merge_asof(
        [
            common.load_sqlite_and_rename_col("forex", frequency=frequency),
            common.load_sqlite_and_rename_col(
                "commodities_prices", frequency=frequency
            ),
        ]
    )


@ttl_cache
def load_interest_rate_df(frequency):
    """Load interest rate dataframe."""
    return plot.get_interest_rate_df(frequency)


def get_xrange(dataframe, selected_range):
    """Determine time range for selected button."""
    xranges = plot.get_xranges(dataframe)
    return (xranges[selected_range][0], xranges[selected_range][1])


def get_frequency(selected_range):
    """Determine frequency from selected range button."""
    frequency = "daily"
    if selected_range in ("All", "2y"):
        frequency = "weekly"
    return frequency


def make_patch(cols, dataframe, selected_range):
    """Make a subplot patch."""
    start, end = get_xrange(dataframe, selected_range)
    dataframe = dataframe[start:end]
    patched_fig = Patch()
    for i, col in enumerate(cols):
        patched_fig["data"][i]["x"] = dataframe[col].index.values
        patched_fig["data"][i]["y"] = dataframe[col].values
    return patched_fig


def make_assets_section(selected_range):
    """Make assets section."""
    all_df = load_all_df(get_frequency(selected_range))
    start, end = get_xrange(all_df, selected_range)
    return plot.make_assets_breakdown_section(all_df[start:end]).update_layout(
        margin=SUBPLOT_MARGIN
    )


def patch_assets_section(selected_range):
    """Patch assets section."""
    return make_patch(
        [
            "total",
            "total_real_estate",
            "total_no_homes",
            "total_retirement",
            "total_investing",
            "total_liquid",
        ],
        load_all_df(get_frequency(selected_range)),
        selected_range,
    )


def make_invret_section(selected_range):
    """Make investing and retirement section."""
    invret_df = load_invret_df(get_frequency(selected_range))
    start, end = get_xrange(invret_df, selected_range)
    return plot.make_investing_retirement_section(invret_df[start:end]).update_layout(
        margin=SUBPLOT_MARGIN
    )


def patch_invret_section(selected_range):
    """Patch investing and retirement section."""
    return make_patch(
        [
            "pillar2",
            "ira",
            "commodities",
            "etfs",
            "ibonds",
        ],
        load_invret_df(get_frequency(selected_range)),
        selected_range,
    )


def make_real_estate_section(selected_range):
    """Make real estate section."""
    real_estate_df = load_real_estate_df(get_frequency(selected_range))
    start, end = get_xrange(real_estate_df, selected_range)
    return plot.make_real_estate_section(real_estate_df[start:end]).update_layout(
        margin=SUBPLOT_MARGIN
    )


def patch_real_estate_section(selected_range):
    """Patch real estate section."""
    return make_patch(
        [
            "Mt Vernon Price",
            "Mt Vernon Rent",
            "Northlake Price",
            "Northlake Rent",
            "Villa Maria Price",
            "Villa Maria Rent",
        ],
        load_real_estate_df(get_frequency(selected_range)),
        selected_range,
    )


def make_allocation_profit_section(selected_range):
    """Make allocation profit section."""
    frequency = get_frequency(selected_range)
    return plot.make_allocation_profit_section(
        load_all_df(frequency),
        load_invret_df(frequency),
        load_real_estate_df(frequency),
    )


def make_change_section(selected_range, col, title):
    """Make change section."""
    return plot.make_change_section(
        load_all_df(get_frequency(selected_range)), col, title
    )


def make_prices_section(selected_range):
    """Make prices section."""
    prices_df = load_prices_df(get_frequency(selected_range))
    start, end = get_xrange(prices_df, selected_range)
    return plot.make_prices_section(prices_df[start:end]).update_layout(
        margin=SUBPLOT_MARGIN
    )


def patch_prices_section(selected_range):
    """Patch prices section."""
    return make_patch(
        [
            "CHFUSD",
            "SGDUSD",
            "GOLD",
            "SILVER",
        ],
        load_prices_df(get_frequency(selected_range)),
        selected_range,
    )


def make_interest_rate_section(selected_range):
    """Make interest rate section."""
    intrate_df = load_interest_rate_df(get_frequency(selected_range))
    start, end = get_xrange(intrate_df, selected_range)
    return plot.make_interest_rate_section(intrate_df[start:end])


def patch_interest_rate_section(selected_range):
    """Patch interest rate section."""
    return make_patch(
        [
            "Fed Funds",
            "SOFR",
            "Schwab SWVXX",
            "Wealthfront Cash",
        ],
        load_interest_rate_df(get_frequency(selected_range)),
        selected_range,
    )


@ttl_cache
def i_and_e_layout():
    """Income & Expenses page layout."""
    ledger_df, ledger_summarized_df = i_and_e.get_ledger_dataframes()
    return html.Div(
        [
            dcc.Graph(
                figure=i_and_e.get_income_expense_yearly_chart(ledger_summarized_df)
            ),
            dcc.Graph(
                figure=i_and_e.get_yearly_chart(
                    ledger_summarized_df, "Income", "Yearly Income"
                )
            ),
            dcc.Graph(
                figure=i_and_e.get_yearly_chart(
                    ledger_summarized_df, "Expenses", "Yearly Expenses"
                )
            ),
            dcc.Graph(
                figure=i_and_e.get_income_expense_monthly_chart(ledger_summarized_df)
            ),
            dcc.Graph(
                figure=i_and_e.get_monthly_chart(
                    ledger_summarized_df, "Income", "Monthly Income"
                )
            ),
            dcc.Graph(
                figure=i_and_e.get_monthly_chart(
                    ledger_summarized_df, "Expenses", "Monthly Expenses"
                )
            ),
            dcc.Graph(
                figure=i_and_e.get_monthly_chart(
                    ledger_df, "Income", "Monthly Income Categorized"
                )
            ),
            dcc.Graph(
                figure=i_and_e.get_monthly_chart(
                    ledger_df, "Expenses", "Monthly Expenses Categorized"
                )
            ),
            dcc.Graph(
                figure=i_and_e.get_average_monthly_income_expenses_chart(ledger_df)
            ),
            dcc.Graph(figure=i_and_e.get_average_monthly_top_expenses(ledger_df)),
        ]
    )


@ttl_cache
def home_layout():
    """Main page layout."""
    graph_style_full = {"width": "98vw", "height": "96vh"}
    graph_style_half = {"width": "98vw", "height": "50vh"}
    graph_style_three_quarters = {"width": "98vw", "height": "75vh"}
    return html.Div(
        [
            dcc.Graph(
                id="assets",
                figure=make_assets_section(INITIAL_TIMERANGE),
                style=graph_style_full,
            ),
            make_range_buttons("timerange_assets"),
            dcc.Graph(
                id="investing_retirement",
                figure=make_invret_section(INITIAL_TIMERANGE),
                style=graph_style_full,
            ),
            make_range_buttons("timerange_invret"),
            dcc.Graph(
                id="real_estate",
                figure=make_real_estate_section(INITIAL_TIMERANGE),
                style=graph_style_full,
            ),
            make_range_buttons("timerange_real_estate"),
            dcc.Graph(
                id="allocation",
                figure=make_allocation_profit_section(INITIAL_TIMERANGE),
                style=graph_style_three_quarters,
            ),
            dcc.Graph(
                id="total_change",
                figure=make_change_section(
                    INITIAL_TIMERANGE, "total", "Total Net Worth Change"
                ),
                style=graph_style_half,
            ),
            dcc.Graph(
                id="total_no_homes_change",
                figure=make_change_section(
                    INITIAL_TIMERANGE, "total_no_homes", "Total Net Worth Change"
                ),
                style=graph_style_half,
            ),
            dcc.Graph(
                id="prices",
                figure=make_prices_section(INITIAL_TIMERANGE),
                style=graph_style_half,
            ),
            dcc.Graph(
                id="yield",
                figure=make_interest_rate_section(INITIAL_TIMERANGE),
                style=graph_style_half,
            ),
            make_range_buttons("timerange_prices"),
            dcc.Interval(id="refresh", interval=10 * 60 * 1000),
        ],
    )


@callback(
    Output("assets", "figure"),
    Output("investing_retirement", "figure"),
    Output("real_estate", "figure"),
    Output("prices", "figure"),
    Output("yield", "figure"),
    Output("timerange_assets", "value"),
    Output("timerange_invret", "value"),
    Output("timerange_real_estate", "value"),
    Output("timerange_prices", "value"),
    Input("timerange_assets", "value"),
    Input("timerange_invret", "value"),
    Input("timerange_real_estate", "value"),
    Input("timerange_prices", "value"),
    Input("refresh", "n_intervals"),
    prevent_initial_call=True,
)
def update_xrange(assets_value, invret_value, real_estate_value, prices_value, _):
    """Update graphs based on time selection."""
    selected_range = assets_value
    match ctx.triggered_id:
        case "timerange_assets":
            selected_range = assets_value
        case "timerange_invret":
            selected_range = invret_value
        case "timerange_real_estate":
            selected_range = real_estate_value
        case "timerange_prices":
            selected_range = prices_value

    return (
        patch_assets_section(selected_range),
        patch_invret_section(selected_range),
        patch_real_estate_section(selected_range),
        patch_prices_section(selected_range),
        patch_interest_rate_section(selected_range),
        *[selected_range] * 4,
    )


pio.templates.default = "plotly_dark"
app = Dash(__name__, url_base_pathname="/accounts/", use_pages=True, pages_folder="")
app.title = "Accounts"
server = app.server
register_page("home", title="Accounts", path="/", layout=home_layout)
register_page(
    "i_and_e", title="Income & Expenses", path="/i_and_e", layout=i_and_e_layout
)
app.layout = page_container


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port="8050")
