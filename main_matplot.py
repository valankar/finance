import io
import itertools
from concurrent.futures import Future, ProcessPoolExecutor
from datetime import datetime, timedelta
from typing import Callable, ClassVar, Literal, NamedTuple, Optional, Sequence, cast

import humanize
import matplotlib.colors as mcolors
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from loguru import logger
from matplotlib.figure import Figure
from matplotlib.typing import ColorType
from nicegui import run, ui

import balance_etfs
import brokerages
import common
import futures
import homes
import margin_loan
import plot
import stock_options
from main_graphs import (
    DEFAULT_RANGE,
    RANGES,
    GraphCommon,
)

plt.style.use("dark_background")


LAYOUT: Literal["constrained", "compressed", "tight"] = "constrained"


class GraphGenerationError(Exception):
    pass


class GraphResult(NamedTuple):
    f: Future[bytes]
    redis_key: str


class BreakdownSection(NamedTuple):
    title: str
    dataframe: Callable[[], pd.DataFrame]
    column_titles: Optional[list[tuple[str, str]]]


class MultilineSection(NamedTuple):
    title: str
    dataframe: Callable[[Optional[str]], pd.DataFrame]


class Matplots(GraphCommon):
    REDIS_SUBKEY: ClassVar[str] = "MatplotGraphs"
    ASSET_PIE: ClassVar[str] = "Asset Allocation Pie"
    INVESTING_ALLOCATION_PIE: ClassVar[str] = "Investing Allocation Pie"
    REBALANCING_BAR: ClassVar[str] = "Rebalancing Required Bar"
    RE_CHANGE: ClassVar[str] = "Real Estate Change Since Purchase"
    RE_YEARLY_CHANGE: ClassVar[str] = "Real Estate Yearly Average Change Since Purchase"
    TOTAL_CHANGE_YOY: ClassVar[str] = "Total Net Worth Change Year Over Year"
    TOTAL_CHANGE_MOM: ClassVar[str] = "Total Net Worth Change Month Over Month"
    TOTAL_NO_RE_CHANGE_YOY: ClassVar[str] = (
        "Total Net Worth Change w/o Real Estate Year Over Year"
    )
    TOTAL_NO_RE_CHANGE_MOM: ClassVar[str] = (
        "Total Net Worth Change w/o Real Estate Month Over Month"
    )
    MARGIN_LOAN: ClassVar[str] = "Margin Loan"
    FUTURES_MARGIN: ClassVar[str] = "Futures Margin"

    def __init__(self):
        self.ui_image_ranged: dict[str, ui.image] = {}
        self.selected_range = DEFAULT_RANGE
        self.image_graphs = common.walrus_db.db.Hash(self.REDIS_KEY)
        self.assets_section = BreakdownSection(
            title="Assets",
            dataframe=lambda: common.read_sql_table("history"),
            column_titles=[
                ("total", "Total"),
                ("total_real_estate", "Real Estate"),
                ("total_no_real_estate", "Total w/o Real Estate"),
                ("total_retirement", "Retirement"),
                ("total_investing", "Investing"),
                ("total_liquid", "Liquid"),
            ],
        )
        self.investing_retirement_section = BreakdownSection(
            title="Retirement",
            dataframe=lambda: common.read_sql_table("history")[["pillar2", "ira"]],
            column_titles=[
                ("pillar2", "Pillar 2"),
                ("ira", "IRA"),
            ],
        )
        self.real_estate_section = BreakdownSection(
            title="Real Estate",
            dataframe=lambda: homes.get_real_estate_df(),
            column_titles=None,
        )
        self.brokerage_values_section = BreakdownSection(
            title="Brokerage Values",
            dataframe=lambda: brokerages.load_df(),
            column_titles=None,
        )
        self.prices_section = MultilineSection(
            title="Prices",
            dataframe=lambda r: self.prices_df(r),
        )
        self.forex_section = BreakdownSection(
            title="Forex",
            dataframe=lambda: common.read_sql_table("forex"),
            column_titles=None,
        )
        self.interest_rate_section = MultilineSection(
            title="Interest Rates",
            dataframe=lambda _: plot.get_interest_rate_df(),
        )

    def create(self):
        with ui.footer().classes("transparent q-py-none"):
            with ui.tabs().classes("w-full") as tabs:
                for timerange in RANGES:
                    ui.tab(timerange)
        tabs.bind_value(self, "selected_range")
        tabs.on_value_change(self.update)

        self.section_breakdown(self.assets_section)
        self.section_breakdown(self.investing_retirement_section)
        self.section_breakdown(self.real_estate_section)

        with ui.grid().classes("w-full gap-0 md:grid-cols-2"):
            self.ui_image(self.RE_CHANGE, props="fit=scale-down")
            self.ui_image(self.RE_YEARLY_CHANGE, props="fit=scale-down")

        self.section_title("Total Net Worth Change")
        with ui.grid().classes("w-full gap-0 md:grid-cols-2"):
            self.ui_image(self.TOTAL_CHANGE_YOY, props="fit=scale-down")
            self.ui_image(self.TOTAL_CHANGE_MOM, props="fit=scale-down")

        self.section_title("Total Net Worth Change w/o Real Estate")
        with ui.grid().classes("w-full gap-0 md:grid-cols-2"):
            self.ui_image(self.TOTAL_NO_RE_CHANGE_YOY, props="fit=scale-down")
            self.ui_image(self.TOTAL_NO_RE_CHANGE_MOM, props="fit=scale-down")

        self.section_title("Asset Allocation")
        with ui.grid().classes("w-full gap-0 md:grid-cols-3"):
            self.ui_image(self.ASSET_PIE)
            self.ui_image(self.INVESTING_ALLOCATION_PIE)
            self.ui_image(self.REBALANCING_BAR)

        self.section_multiline(self.prices_section)
        self.section_breakdown(self.forex_section)
        self.section_multiline(self.interest_rate_section)

        num_brokerages = len(margin_loan.LOAN_BROKERAGES)
        self.section_title("Brokerage Leverage")
        with ui.grid().classes(f"w-full gap-0 md:grid-cols-{num_brokerages + 1}"):
            for broker in margin_loan.LOAN_BROKERAGES:
                self.ui_image(
                    self.make_redis_key(self.MARGIN_LOAN, broker.name),
                    make_redis_key=False,
                )
            self.ui_image(
                self.make_redis_key(self.MARGIN_LOAN, "Overall"), make_redis_key=False
            )
        self.section_breakdown(self.brokerage_values_section, grid_cols=num_brokerages)
        self.section_title("Brokerage Futures Margin")
        with ui.grid().classes(f"w-full gap-0 md:grid-cols-{num_brokerages}"):
            for broker in margin_loan.LOAN_BROKERAGES:
                self.ui_image(
                    self.make_redis_key(self.FUTURES_MARGIN, broker.name),
                    make_redis_key=False,
                )

        self.daily_change()
        self.common_links()

    async def update(self) -> None:
        for ui_key, image in self.ui_image_ranged.items():
            name = f"{ui_key}:{self.selected_range}"
            if graph := self.image_graphs.get(name):
                await run.io_bound(
                    image.set_source,
                    self.encode_png(graph),
                )

    def ui_image(
        self,
        key: str,
        props: str = "fit=contain",
        classes: str = "",
        make_redis_key: bool = True,
    ) -> Optional[ui.image]:
        if make_redis_key:
            key = self.make_redis_key(key)
        if graph := self.image_graphs.get(key):
            return ui.image(self.encode_png(graph)).props(props).classes(classes)
        return None

    def get_column_titles(self, section: BreakdownSection) -> list[tuple[str, str]]:
        if section.column_titles:
            return section.column_titles
        df = section.dataframe()
        return list(zip(df.columns, df.columns))

    def section_multiline(self, section: MultilineSection):
        self.section_title(section.title)
        name = self.make_redis_key(section.title, self.selected_range)
        if uii := self.ui_image(name, make_redis_key=False):
            self.ui_image_ranged[self.make_ui_key(name)] = uii

    def section_breakdown(self, section: BreakdownSection, grid_cols: int = 2):
        self.section_title(section.title)
        cols: list[tuple[str, str]] = self.get_column_titles(section)
        with ui.grid().classes(f"w-full gap-0 md:grid-cols-{grid_cols}"):
            for column, _ in cols:
                name = self.make_redis_key(section.title, column, self.selected_range)
                if uii := self.ui_image(name, make_redis_key=False):
                    self.ui_image_ranged[self.make_ui_key(name)] = uii

    def generate(self, executor: ProcessPoolExecutor):
        logger.info("Generating Matplot graphs")
        start_time = datetime.now()
        results: list[GraphResult] = []
        for r in RANGES:
            for section in [
                self.assets_section,
                self.investing_retirement_section,
                self.real_estate_section,
                self.forex_section,
            ]:
                color = iter(itertools.cycle(mcolors.TABLEAU_COLORS.values()))
                cols: list[tuple[str, str]] = self.get_column_titles(section)
                for column, title in cols:
                    results.append(
                        GraphResult(
                            executor.submit(
                                make_dataframe_line_graph,
                                self.limit_and_resample_df(section.dataframe(), r),
                                column,
                                title,
                                next(color),
                            ),
                            self.make_redis_key(section.title, column, r),
                        )
                    )

            for section in [
                self.brokerage_values_section,
            ]:
                color = iter(itertools.cycle(mcolors.TABLEAU_COLORS.values()))
                cols: list[tuple[str, str]] = self.get_column_titles(section)
                for column, title in cols:
                    results.append(
                        GraphResult(
                            executor.submit(
                                make_dataframe_line_graph,
                                self.limit_and_resample_df(section.dataframe(), r),
                                column,
                                title,
                                next(color),
                                figsize=None,
                            ),
                            self.make_redis_key(section.title, column, r),
                        )
                    )

            for section in [self.interest_rate_section, self.prices_section]:
                results.append(
                    GraphResult(
                        executor.submit(
                            make_dataframe_multiline_graph,
                            self.limit_and_resample_df(section.dataframe(r), r),
                        ),
                        self.make_redis_key(section.title, r),
                    )
                )

        results.append(
            GraphResult(
                executor.submit(make_investing_allocation_pie),
                self.make_redis_key(self.INVESTING_ALLOCATION_PIE),
            )
        )
        results.append(
            GraphResult(
                executor.submit(make_investing_allocation_bar),
                self.make_redis_key(self.REBALANCING_BAR),
            )
        )
        results.append(
            GraphResult(
                executor.submit(make_asset_allocation_pie),
                self.make_redis_key(self.ASSET_PIE),
            )
        )
        results.append(
            GraphResult(
                executor.submit(make_real_estate_change_bar),
                self.make_redis_key(self.RE_CHANGE),
            )
        )
        results.append(
            GraphResult(
                executor.submit(make_real_estate_change_bar_yearly),
                self.make_redis_key(self.RE_YEARLY_CHANGE),
            )
        )
        for broker in margin_loan.LOAN_BROKERAGES:
            results.append(
                GraphResult(
                    executor.submit(make_loan_graph, broker),
                    self.make_redis_key(self.MARGIN_LOAN, broker.name),
                )
            )
            results.append(
                GraphResult(
                    executor.submit(make_loan_graph),
                    self.make_redis_key(self.MARGIN_LOAN, "Overall"),
                )
            )
            results.append(
                GraphResult(
                    executor.submit(make_futures_margin_graph, broker),
                    self.make_redis_key(self.FUTURES_MARGIN, broker.name),
                )
            )
        total_changes = {
            "total": (self.TOTAL_CHANGE_YOY, self.TOTAL_CHANGE_MOM),
            "total_no_real_estate": (
                self.TOTAL_NO_RE_CHANGE_YOY,
                self.TOTAL_NO_RE_CHANGE_MOM,
            ),
        }
        for column, keys in total_changes.items():
            results.append(
                GraphResult(
                    executor.submit(make_total_bar_yoy, column),
                    self.make_redis_key(keys[0]),
                )
            )
            results.append(
                GraphResult(
                    executor.submit(make_total_bar_mom, column),
                    self.make_redis_key(keys[1]),
                )
            )

        for r in results:
            if image := r.f.result():
                self.image_graphs[r.redis_key] = image
            elif r.redis_key in self.image_graphs:
                del self.image_graphs[r.redis_key]
        logger.info(
            f"Graph generation time for Matplot: {humanize.precisedelta(datetime.now() - start_time)}"
        )


def make_dataframe_line_graph(
    df: pd.DataFrame,
    column: str,
    title: str,
    line_color: ColorType,
    figsize: Optional[tuple[float, float]] = (15, 5),
) -> bytes:
    fig = Figure(figsize=figsize, layout=LAYOUT)
    ax = fig.subplots()
    locator = mdates.AutoDateLocator()
    formatter = mdates.ConciseDateFormatter(locator, show_offset=False)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    ax.plot(
        df.index,
        df[column],
        color=line_color,
        linewidth=2,
    )
    ax.set_title(title)
    first_value = df[column].loc[df[column].first_valid_index()]
    last_value = df[column].loc[df[column].last_valid_index()]
    ax.axhline(
        y=last_value,
        color="gray",
        linestyle="--",
    )
    if abs(last_value) < 100:
        annotation = f"{last_value:.2f}"
    else:
        annotation = f"{last_value:,.0f}"
    if first_value != 0:
        percent_change = (last_value - first_value) / first_value
        if first_value < 0:
            percent_change *= -1
        diff_format = ",.0f"
        diff = last_value - first_value
        if abs(diff) < 1:
            diff_format = ".2f"
        annotation += f" {percent_change:+.1%} ({diff:{diff_format}})"
    ax.annotate(
        annotation,
        xy=(0.5, 0.5),
        xycoords="figure fraction",
        ha="center",
        va="center",
        fontsize=18,
    )
    return make_image_graph(fig)


def make_dataframe_multiline_graph(df: pd.DataFrame) -> bytes:
    fig = Figure(figsize=(15, 5), layout=LAYOUT)
    ax = fig.subplots()
    locator = mdates.AutoDateLocator()
    formatter = mdates.ConciseDateFormatter(locator, show_offset=False)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    for column in df.columns:
        ax.plot(
            df.index,
            df[column],
            label=column,
        )
    ax.legend()
    return make_image_graph(fig)


def make_image_graph(fig: Figure) -> bytes:
    data = io.BytesIO()
    fig.savefig(data, format="png")
    return data.getvalue()


def make_pie_graph(labels: Sequence[str], values: Sequence[float]) -> bytes:
    fig = Figure(layout=LAYOUT)
    ax = fig.subplots()
    color = list(mcolors.TABLEAU_COLORS.values())
    ax.pie(values, labels=labels, autopct="%1.1f%%", colors=color)
    return make_image_graph(fig)


def make_bar_graph(
    title: str,
    x: Sequence[str] | pd.DatetimeIndex | pd.Index,
    y: Sequence[float] | pd.Series,
    labels: Optional[Sequence[str]] = None,
    rotate_x_labels: bool = False,
    width: Optional[Sequence | float] = 0.8,
) -> bytes:
    fig = Figure(layout=LAYOUT)
    ax = fig.subplots()
    colors = ["tab:green" if v > 0 else "tab:red" for v in y]
    p = ax.bar(x, y, color=colors, width=width)  # type: ignore
    if labels:
        ax.bar_label(p, labels=labels, label_type="center")
    ax.set_title(title)
    if rotate_x_labels:
        fig.autofmt_xdate()
    return make_image_graph(fig)


INVESTING_ALLOCATION_LABEL_COL = (
    ("US Large Cap", "US_LARGE_CAP"),
    ("US Small Cap", "US_SMALL_CAP"),
    ("US Bonds", "US_BONDS"),
    ("International Developed", "INTERNATIONAL_DEVELOPED"),
    ("International Emerging", "INTERNATIONAL_EMERGING"),
    ("Gold", "COMMODITIES_GOLD"),
    ("Silver", "COMMODITIES_SILVER"),
    ("Crypto", "COMMODITIES_CRYPTO"),
)


def make_investing_allocation_pie() -> bytes:
    df = balance_etfs.get_rebalancing_df(0)
    labels = [name for name, _ in INVESTING_ALLOCATION_LABEL_COL]
    values = [
        cast(float, df.loc[col]["value"]) for _, col in INVESTING_ALLOCATION_LABEL_COL
    ]
    values = [x if x >= 0 else 0 for x in values]
    pie_current = make_pie_graph(labels, values)
    return pie_current


def make_investing_allocation_bar() -> bytes:
    df = balance_etfs.get_rebalancing_df(0)
    labels = [name for name, _ in INVESTING_ALLOCATION_LABEL_COL]
    values = [
        cast(float, df.loc[col]["usd_to_reconcile"])
        for _, col in INVESTING_ALLOCATION_LABEL_COL
    ]
    bar_labels = [f"{v:,.0f}" for v in values]
    rebalancing = make_bar_graph(
        "Rebalancing Required",
        labels,
        values,
        labels=bar_labels,
        rotate_x_labels=True,
    )
    return rebalancing


def make_asset_allocation_pie() -> bytes:
    latest_df = common.read_sql_last("history")
    labels = ["Investing", "Real Estate"]
    values = [
        latest_df["total_investing"].iloc[-1],
        latest_df["total_real_estate"].iloc[-1],
    ]
    if (liquid := latest_df["total_liquid"].iloc[-1]) > 0:
        labels.append("Liquid")
        values.append(liquid)
    return make_pie_graph(labels, values)


def get_real_estate_change_df() -> pd.DataFrame:
    real_estate_df = homes.get_real_estate_df()
    cols = [f"{home.name} Price" for home in homes.PROPERTIES]
    for home in cols:
        real_estate_df[f"{home} Percent Change"] = (
            (
                real_estate_df[home]
                - real_estate_df.loc[real_estate_df[home].first_valid_index(), home]  # type: ignore
            )
            / real_estate_df.loc[real_estate_df[home].first_valid_index(), home]  # type: ignore
        )
    return real_estate_df


def make_real_estate_change_bar() -> bytes:
    real_estate_df = get_real_estate_change_df()
    cols = [f"{home.name} Price" for home in homes.PROPERTIES]
    values = []
    percent = []
    for home in cols:
        v = (
            real_estate_df.iloc[-1][home]
            - real_estate_df.loc[real_estate_df[home].first_valid_index(), home]  # type: ignore
        )
        values.append(v)
        p = real_estate_df.iloc[-1][f"{home} Percent Change"]
        percent.append(f"{v:,.0f}\n{p:.1%}")
    return make_bar_graph("Real Estate Change Since Purchase", cols, values, percent)


def make_real_estate_change_bar_yearly() -> bytes:
    real_estate_df = get_real_estate_change_df()
    cols = [f"{home.name} Price" for home in homes.PROPERTIES]
    values = []
    percent = []
    for home in cols:
        time_diff = (
            real_estate_df[home].index[-1] - real_estate_df[home].first_valid_index()
        )  # type: ignore
        value_diff = (
            real_estate_df.iloc[-1][home]
            - real_estate_df.loc[real_estate_df[home].first_valid_index(), home]  # type: ignore
        )
        percent_diff = real_estate_df.iloc[-1][f"{home} Percent Change"]
        v = (value_diff / time_diff.days) * 365
        values.append(v)
        p = (percent_diff / time_diff.days) * 365
        percent.append(f"{v:,.0f}\n{p:.1%}")
    return make_bar_graph(
        "Real Estate Yearly Average Change Since Purchase", cols, values, percent
    )


def make_total_bar_yoy(column: str) -> bytes:
    df = common.read_sql_table("history")
    df = df.resample("YE").last().interpolate().diff().dropna().iloc[-6:]
    # Re-align at beginning of year.
    df.index = pd.DatetimeIndex(df.index.strftime("%Y-01-01"))  # type: ignore
    x = df.index
    y = df[column]
    return make_bar_graph(
        "Year Over Year",
        x,
        y,
        labels=[f"{v:,.0f}" for v in y],
        width=[timedelta(330)] * len(x),
    )


def make_total_bar_mom(column: str) -> bytes:
    df = common.read_sql_table("history")
    df = df.resample("ME").last().interpolate().diff().dropna().iloc[-36:]
    x = df.index
    return make_bar_graph(
        "Month Over Month",
        x,
        df[column],
        rotate_x_labels=True,
        width=[timedelta(25)] * len(x),
    )


def make_loan_graph(broker: Optional[margin_loan.LoanBrokerage] = None) -> bytes:
    b = margin_loan.get_balances_broker()
    if broker is None:
        df = margin_loan.get_balances_all(b)
        name = "Overall"
    else:
        name = broker.name
        df = b[name]
    categories = ["Equity", "Loan", "Equity - Loan"]
    total = df.iloc[-1]["Total"]
    equity_balance = df.iloc[-1]["Equity Balance"]
    loan_balance = df.iloc[-1]["Loan Balance"]
    amounts = [
        equity_balance,
        loan_balance,
        total,
    ]
    bottom = [0, amounts[0], 0]
    labels = [f"{x:,.0f}" for x in amounts]
    fig = Figure(layout=LAYOUT)
    ax = fig.subplots()
    colors = ["tab:green" if v > 0 else "tab:red" for v in amounts]
    p = ax.bar(categories, amounts, bottom=bottom, color=colors)
    ax.bar_label(p, labels=labels, label_type="center")
    i = 1
    for i, leverage in enumerate((5.0, 2.0, 1.5), start=1):
        leverage_balance = equity_balance / leverage
        ax.axhline(leverage_balance, color="gray", linestyle="--")
        distance = f"Distance to {leverage}"
        remaining = total - leverage_balance
        ax.annotate(
            f"{distance}: {remaining:,.0f}",
            xy=(0.5, i / 10),
            xycoords="axes fraction",
            ha="center",
            va="center",
        )
    ax.annotate(
        f"Leverage ratio: {df.iloc[-1]['Leverage Ratio']:.2f}",
        xy=(0.5, (i + 1) / 10),
        xycoords="axes fraction",
        ha="center",
        va="center",
    )
    ax.set_title(name)
    return make_image_graph(fig)


def make_futures_margin_graph(broker: margin_loan.LoanBrokerage) -> bytes:
    futures_df = futures.Futures().futures_df
    opts = stock_options.get_options_and_spreads()
    margin_by_account = futures_df.groupby(level="account")["margin_requirement"].sum()
    if broker.name not in margin_by_account:
        return b""
    b = margin_loan.get_balances_broker()
    df = b[broker.name]
    csp = 0
    if broker.name == common.Brokerage.SCHWAB:
        csp = stock_options.short_put_exposure(opts.pruned_options, broker.name)
    cash_balance = df.iloc[-1]["Cash Balance"]
    money_market = df.iloc[-1]["Money Market"]
    margin_requirement = margin_by_account.get(broker.name, 0)
    total = cash_balance - margin_requirement - money_market
    if abs(money_market) > 100:
        if csp < 0:
            categories = [
                "Cash",
                "Money Market",
                "CSP Req",
                "Futures Margin",
                "Excess",
            ]
            amounts = [
                cash_balance,
                -(money_market + csp),
                csp,
                -margin_requirement,
                total,
            ]
            bottom = [
                0,
                cash_balance,
                cash_balance - (money_market + csp),
                cash_balance - money_market,
                0,
            ]
        else:
            categories = ["Cash", "Money Market", "Futures Margin", "Excess"]
            amounts = [
                cash_balance,
                -money_market,
                -margin_requirement,
                total,
            ]
            bottom = [0, cash_balance, cash_balance - money_market, 0]
    else:
        categories = ["Cash", "Futures Margin", "Excess"]
        amounts = [
            cash_balance,
            -margin_requirement,
            total,
        ]
        bottom = [0, cash_balance, 0]
    labels = [f"{x:,.0f}" for x in amounts]
    fig = Figure(layout=LAYOUT)
    ax = fig.subplots()
    colors = ["tab:green" if v > 0 else "tab:red" for v in amounts]
    if amounts[-1] < 0:
        colors[-1] = "darkred"
    p = ax.bar(categories, amounts, bottom=bottom, color=colors)
    ax.bar_label(p, labels=labels, label_type="center")
    ax.set_title(broker.name)
    return make_image_graph(fig)


def main():
    with ProcessPoolExecutor() as executor:
        Matplots().generate(executor)


if __name__ == "__main__":
    main()
