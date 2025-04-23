import io
import itertools
from datetime import datetime
from typing import Callable, ClassVar, Literal, NamedTuple, Optional, Sequence, cast

import matplotlib.colors as mcolors
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
import walrus
from loguru import logger
from matplotlib.figure import Figure
from matplotlib.typing import ColorType
from nicegui import run, ui

import balance_etfs
import brokerages
import common
import homes
import margin_loan
import plot
import stock_options
from main_graphs import (
    DEFAULT_RANGE,
    RANGES,
    GraphCommon,
    MainGraphsImageOnly,
)

plt.style.use("dark_background")


class BreakdownSection(NamedTuple):
    title: str
    dataframe: Callable[[], pd.DataFrame]
    column_titles: Optional[list[tuple[str, str]]]


class MultilineSection(NamedTuple):
    title: str
    dataframe: Callable[[], pd.DataFrame]


class Matplots(GraphCommon):
    REDIS_SUBKEY: ClassVar[str] = "MatplotGraphs"
    ASSET_PIE: ClassVar[str] = "Asset Allocation Pie"
    INVESTING_ALLOCATION_PIE: ClassVar[str] = "Investing Allocation Pie"
    REBALANCING_BAR: ClassVar[str] = "Rebalancing Required Bar"
    RE_CHANGE: ClassVar[str] = "Real Estate Change Since Purchase"
    RE_YEARLY_CHANGE: ClassVar[str] = "Real Estate Yearly Average Change Since Purchase"
    MARGIN_LOAN: ClassVar[str] = "Margin Loan"
    LAYOUT: ClassVar[Literal["constrained", "compressed", "tight"]] = "constrained"

    def __init__(self, db: walrus.Database):
        self.ui_image_ranged: dict[str, ui.image] = {}
        self.selected_range = DEFAULT_RANGE
        self.image_graphs = db.Hash(self.REDIS_KEY)
        self.assets_section = BreakdownSection(
            title="Assets",
            dataframe=lambda: common.read_sql_table("history"),
            column_titles=[
                ("total", "Total"),
                ("total_real_estate", "Real Estate"),
                ("total_no_homes", "Total w/o Real Estate"),
                ("total_retirement", "Retirement"),
                ("total_investing", "Investing"),
                ("total_liquid", "Liquid"),
            ],
        )
        self.investing_retirement_section = BreakdownSection(
            title="Investing & Retirement",
            dataframe=lambda: common.read_sql_table("history"),
            column_titles=[
                ("pillar2", "Pillar 2"),
                ("ira", "IRA"),
                ("commodities", "Commodities"),
                ("etfs", "ETFs"),
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
            dataframe=lambda: common.read_sql_table("schwab_etfs_prices"),
        )
        self.forex_section = BreakdownSection(
            title="Forex",
            dataframe=lambda: common.read_sql_table("forex"),
            column_titles=None,
        )
        self.interest_rate_section = MultilineSection(
            title="Interest Rates",
            dataframe=lambda: plot.get_interest_rate_df(),
        )

    def create(self, mgio: MainGraphsImageOnly):
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

        self.section_title("Asset Allocation")
        with ui.grid().classes("w-full gap-0 md:grid-cols-3"):
            self.ui_image(self.ASSET_PIE)
            self.ui_image(self.INVESTING_ALLOCATION_PIE)
            self.ui_image(self.REBALANCING_BAR)

        self.section_multiline(self.prices_section)
        self.section_breakdown(self.forex_section)
        self.section_multiline(self.interest_rate_section)

        num_brokerages = len(margin_loan.LOAN_BROKERAGES)
        self.section_title("Margin/Box Loans")
        with ui.grid().classes(f"w-full gap-0 md:grid-cols-{num_brokerages}"):
            for broker in margin_loan.LOAN_BROKERAGES:
                name = self.make_redis_key(self.MARGIN_LOAN, broker.name)
                self.ui_image(name, make_redis_key=False)

        self.section_breakdown(self.brokerage_values_section, grid_cols=num_brokerages)
        # Use plotly daily indicator
        if graph := mgio.image_graphs.get(mgio.make_redis_key("daily_indicator")):
            ui.image(self.encode_png(graph))
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

    def section_title(self, title: str):
        with ui.column(align_items="center").classes("w-full"):
            ui.label(title)

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

    def make_image_graph(self, fig: Figure) -> bytes:
        data = io.BytesIO()
        fig.savefig(data, format="png")
        return data.getvalue()

    def get_real_estate_change_df(self) -> pd.DataFrame:
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

    def make_bar_graph(
        self,
        title: str,
        x: Sequence[str],
        y: Sequence[float],
        labels: Optional[Sequence[str]] = None,
        rotate_x_labels: bool = False,
    ) -> bytes:
        fig = Figure(layout=self.LAYOUT)
        ax = fig.subplots()
        colors = ["tab:green" if v > 0 else "tab:red" for v in y]
        p = ax.bar(x, y, color=colors)
        if labels:
            ax.bar_label(p, labels=labels, label_type="center")
        ax.set_title(title)
        if rotate_x_labels:
            fig.autofmt_xdate()
        return self.make_image_graph(fig)

    def make_real_estate_change_bar_yearly(self) -> bytes:
        real_estate_df = self.get_real_estate_change_df()
        cols = [f"{home.name} Price" for home in homes.PROPERTIES]
        values = []
        percent = []
        for home in cols:
            time_diff = (
                real_estate_df[home].index[-1]
                - real_estate_df[home].first_valid_index()
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
        return self.make_bar_graph(
            "Real Estate Yearly Average Change Since Purchase", cols, values, percent
        )

    def make_real_estate_change_bar(self) -> bytes:
        real_estate_df = self.get_real_estate_change_df()
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
        return self.make_bar_graph(
            "Real Estate Change Since Purchase", cols, values, percent
        )

    def make_asset_allocation_pie(self) -> bytes:
        latest_df = common.read_sql_last("history")
        labels = ["Investing", "Real Estate", "Retirement"]
        values = [
            latest_df["total_investing"].iloc[-1],
            latest_df["total_real_estate"].iloc[-1],
            latest_df["total_retirement"].iloc[-1],
        ]
        if (liquid := latest_df["total_liquid"].iloc[-1]) > 0:
            labels.append("Liquid")
            values.append(liquid)
        return self.make_pie_graph(labels, values)

    def make_investing_allocation_section(self) -> Optional[tuple[bytes, bytes]]:
        if (df := balance_etfs.get_rebalancing_df(0)) is None:
            return None
        label_col = (
            ("US Large Cap", "US_LARGE_CAP"),
            ("US Small Cap", "US_SMALL_CAP"),
            ("US Bonds", "US_BONDS"),
            ("International Developed", "INTERNATIONAL_DEVELOPED"),
            ("International Emerging", "INTERNATIONAL_EMERGING"),
            ("Gold", "COMMODITIES_GOLD"),
            ("Silver", "COMMODITIES_SILVER"),
            ("Crypto", "COMMODITIES_CRYPTO"),
        )
        labels = [name for name, _ in label_col]
        values = [cast(float, df.loc[col]["value"]) for _, col in label_col]
        pie_current = self.make_pie_graph(labels, values)
        values = [cast(float, df.loc[col]["usd_to_reconcile"]) for _, col in label_col]
        bar_labels = [f"{v:,.0f}" for v in values]
        rebalancing = self.make_bar_graph(
            "Rebalancing Required",
            labels,
            values,
            labels=bar_labels,
            rotate_x_labels=True,
        )
        return pie_current, rebalancing

    def make_pie_graph(self, labels: Sequence[str], values: Sequence[float]) -> bytes:
        fig = Figure(layout=self.LAYOUT)
        ax = fig.subplots()
        color = list(mcolors.TABLEAU_COLORS.values())
        ax.pie(values, labels=labels, autopct="%1.1f%%", colors=color)
        return self.make_image_graph(fig)

    def make_loan_section(self) -> list[tuple[str, bytes]]:
        graphs = []
        if (od := stock_options.get_options_data()) is None:
            return graphs
        for broker in margin_loan.LOAN_BROKERAGES:
            if (
                df := margin_loan.get_balances_broker(
                    broker, od.opts.options_value_by_brokerage
                )
            ) is None:
                continue
            categories = ["Equity", "Loan", "Equity - Loan"]
            amounts = [
                df["Equity Balance"].iloc[-1],
                df["Loan Balance"].iloc[-1],
                df["Total"].iloc[-1],
            ]
            bottom = [0, amounts[0], 0]
            labels = [f"{x:,.0f}" for x in amounts]
            fig = Figure(layout=self.LAYOUT)
            ax = fig.subplots()
            colors = ["tab:green" if v > 0 else "tab:red" for v in amounts]
            p = ax.bar(categories, amounts, bottom=bottom, color=colors)
            ax.bar_label(p, labels=labels, label_type="center")
            percent_balance = (
                df["Equity Balance"].iloc[-1] - df["30% Equity Balance"].iloc[-1]
            )
            ax.axhline(percent_balance, color="yellow", linestyle="--")
            percent_balance = (
                df["Equity Balance"].iloc[-1] - df["50% Equity Balance"].iloc[-1]
            )
            ax.axhline(percent_balance, color="red", linestyle="--")
            ax.axhline(df["50% Equity Balance"].iloc[-1], color="red", linestyle="--")
            for percent, y in ((30, 0.2), (50, 0.1)):
                distance = f"Distance to {percent}%"
                remaining = df[distance].iloc[-1]
                ax.annotate(
                    f"{distance}: {remaining:,.0f}",
                    xy=(0.5, y),
                    xycoords="axes fraction",
                    ha="center",
                    va="center",
                )
            ax.set_title(broker.name)
            graphs.append((broker.name, self.make_image_graph(fig)))
        return graphs

    def make_dataframe_multiline_graph(self, df: pd.DataFrame) -> bytes:
        fig = Figure(figsize=(15, 5), layout=self.LAYOUT)
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
        return self.make_image_graph(fig)

    def make_dataframe_line_graph(
        self,
        df: pd.DataFrame,
        column: str,
        title: str,
        line_color: ColorType,
        figsize: Optional[tuple[float, float]] = (15, 5),
    ) -> bytes:
        fig = Figure(figsize=figsize, layout=self.LAYOUT)
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
            annotation += f" {percent_change:+.1%}"
        ax.annotate(
            annotation,
            xy=(0.5, 0.5),
            xycoords="figure fraction",
            ha="center",
            va="center",
            fontsize=18,
        )
        return self.make_image_graph(fig)

    def generate(self):
        logger.info("Generating Matplot graphs")
        start_time = datetime.now()
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
                    graph = self.make_dataframe_line_graph(
                        self.limit_and_resample_df(section.dataframe(), r),
                        column,
                        title,
                        next(color),
                    )
                    redis_key = self.make_redis_key(section.title, column, r)
                    self.image_graphs[redis_key] = graph

            for section in [
                self.brokerage_values_section,
            ]:
                color = iter(itertools.cycle(mcolors.TABLEAU_COLORS.values()))
                cols: list[tuple[str, str]] = self.get_column_titles(section)
                for column, title in cols:
                    graph = self.make_dataframe_line_graph(
                        self.limit_and_resample_df(section.dataframe(), r),
                        column,
                        title,
                        next(color),
                        figsize=None,
                    )
                    redis_key = self.make_redis_key(section.title, column, r)
                    self.image_graphs[redis_key] = graph

            for section in [self.interest_rate_section, self.prices_section]:
                graph = self.make_dataframe_multiline_graph(
                    self.limit_and_resample_df(section.dataframe(), r)
                )
                redis_key = self.make_redis_key(section.title, r)
                self.image_graphs[redis_key] = graph

        self.image_graphs[self.make_redis_key(self.ASSET_PIE)] = (
            self.make_asset_allocation_pie()
        )
        if ia := self.make_investing_allocation_section():
            self.image_graphs[self.make_redis_key(self.INVESTING_ALLOCATION_PIE)] = ia[
                0
            ]
            self.image_graphs[self.make_redis_key(self.REBALANCING_BAR)] = ia[1]
        self.image_graphs[self.make_redis_key(self.RE_CHANGE)] = (
            self.make_real_estate_change_bar()
        )
        self.image_graphs[self.make_redis_key(self.RE_YEARLY_CHANGE)] = (
            self.make_real_estate_change_bar_yearly()
        )
        for broker, graph in self.make_loan_section():
            self.image_graphs[self.make_redis_key(self.MARGIN_LOAN, broker)] = graph

        end_time = datetime.now()
        last_generation_duration = end_time - start_time
        logger.info(f"Graph generation time for Matplot: {last_generation_duration}")


def main():
    Matplots(common.WalrusDb().db).generate()


if __name__ == "__main__":
    main()
