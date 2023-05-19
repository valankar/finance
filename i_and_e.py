#!/usr/bin/env python3
"""Create income and expense graphs."""

import io
import subprocess
from pathlib import Path

import pandas as pd
import plotly.express as px
from dateutil.relativedelta import relativedelta

import common

HTML_PREFIX = common.PREFIX + "i_and_e/"
LEDGER_BIN = f"{Path.home()}/bin/ledger"
LEDGER_DIR = f"{Path.home()}/code/ledger"
# pylint: disable-next=line-too-long
LEDGER_CSV_CMD = f"{LEDGER_BIN} -f {LEDGER_DIR}/ledger.ledger --price-db {LEDGER_DIR}/prices.db -X '$' -c --no-revalued csv ^Expenses ^Income"


def get_dataframe(ledger_df, category_prefix):
    """Get income or expense dataframe."""
    dataframe = ledger_df[ledger_df["category"].str.startswith(f"{category_prefix}:")]
    dataframe = dataframe.assign(amount=dataframe["amount"].abs())
    dataframe["category"] = dataframe["category"].str.removeprefix(
        f"{category_prefix}:"
    )
    return dataframe


def configure_yearly_chart(chart):
    """Set some defaults for yearly charts."""
    chart.update_traces(xbins_size="M12")
    chart.update_yaxes(title_text="USD")
    chart.update_xaxes(
        title_text="",
        ticklabelmode="period",
        dtick="M12",
        tickformat="%Y",
        showgrid=True,
    )
    chart.update_layout(bargap=0.1)


def configure_monthly_chart(chart):
    """Set some defaults for monthly charts."""
    chart.update_traces(xbins_size="M1")
    chart.update_yaxes(title_text="USD")
    chart.update_xaxes(
        title_text="",
        ticklabelmode="period",
        dtick="M1",
        tickformat="%b\n%Y",
        showgrid=True,
    )
    chart.update_layout(bargap=0.1)


def get_income_expense_df(ledger_df):
    """Get income and expense totals dataframe."""
    income_df = get_dataframe(ledger_df, "Income")
    income_df = (
        income_df.groupby(by=income_df.index)
        .sum(numeric_only=True)
        .rename(columns={"amount": "income"})
    )
    expense_df = get_dataframe(ledger_df, "Expenses")
    expense_df = (
        expense_df.groupby(by=expense_df.index)
        .sum(numeric_only=True)
        .rename(columns={"amount": "expenses"})
    )
    return income_df.join(expense_df, how="outer")


def get_average_monthly_top_expenses(ledger_df):
    """Get average monthly top expenses."""
    expense_df = get_dataframe(ledger_df, "Expenses")
    latest_time = expense_df[-1:].index.item()
    latest_month = latest_time.strftime("%B %Y")
    latest_month_slice = latest_time.strftime("%Y-%m")
    categories = []
    expenses = []
    for i in (6, 3, 1):
        i -= 1
        start_month_slice = (latest_time + relativedelta(months=-i)).strftime("%Y-%m")
        exp_max_df = (
            expense_df.loc[start_month_slice:latest_month_slice]
            .groupby("category")
            .mean(numeric_only=True)
            .agg(["idxmax", "max"])
        )
        categories.append(exp_max_df.iloc[0].values.item())
        expenses.append(exp_max_df.iloc[1].values.item())
    xaxis = [
        "Last 6 months",
        "Last 3 months",
        latest_month,
    ]
    top_expenses_df = pd.DataFrame(
        {"category": categories, "expense": expenses}, index=xaxis
    )
    chart = px.bar(
        top_expenses_df,
        x=top_expenses_df.index,
        y="expense",
        color="category",
        text_auto=",.0f",
        title="Average Monthly Top Expenses",
    )
    chart.update_xaxes(title_text="", categoryarray=xaxis, categoryorder="array")
    chart.update_yaxes(title_text="USD")
    return chart


def get_average_monthly_income_expenses_chart(ledger_df):
    """Get average income and expenses chart."""
    joined_df = get_income_expense_df(ledger_df).resample("M").sum()
    incomes = []
    expenses = []
    for i in (6, 3, 1):
        incomes.append(joined_df[-i:].mean()["income"])
        expenses.append(joined_df[-i:].mean()["expenses"])
    latest_month = joined_df[-1:].index.item().strftime("%B %Y")
    i_and_e_avg_df = pd.DataFrame(
        {"income": incomes, "expense": expenses},
        index=[
            "Last 6 months",
            "Last 3 months",
            latest_month,
        ],
    )
    chart = px.bar(
        i_and_e_avg_df,
        x=i_and_e_avg_df.index,
        y=i_and_e_avg_df.columns,
        title="Average Monthly Income and Expenses",
        barmode="group",
        text_auto=",.0f",
    )
    chart.update_xaxes(title_text="")
    chart.update_yaxes(title_text="USD")
    return chart


def get_income_expense_yearly_chart(ledger_df):
    """Get yearly income and expense totals chart."""
    joined_df = get_income_expense_df(ledger_df)
    chart = px.histogram(
        joined_df,
        x=joined_df.index,
        y=joined_df.columns,
        barmode="group",
        title="Yearly Income and Expenses",
        histfunc="sum",
        text_auto=",.0f",
    )
    configure_yearly_chart(chart)
    return chart


def get_income_expense_monthly_chart(ledger_df):
    """Get monthly income and expense totals chart."""
    joined_df = get_income_expense_df(ledger_df)
    chart = px.histogram(
        joined_df,
        x=joined_df.index,
        y=joined_df.columns,
        barmode="group",
        title="Monthly Income and Expenses",
        histfunc="sum",
        text_auto=",.0f",
    )
    configure_monthly_chart(chart)
    return chart


def get_yearly_chart(ledger_df, category_prefix, title):
    """Get yearly income or expense bar chart."""
    dataframe = get_dataframe(ledger_df, category_prefix)
    chart = px.histogram(
        dataframe,
        x=dataframe.index,
        y="amount",
        color="category",
        title=title,
        histfunc="sum",
        category_orders={"category": sorted(dataframe["category"].unique())},
    )
    configure_yearly_chart(chart)
    return chart


def get_monthly_chart(ledger_df, category_prefix, title):
    """Get monthly income or expense bar chart."""
    dataframe = get_dataframe(ledger_df, category_prefix)
    chart = px.histogram(
        dataframe,
        x=dataframe.index,
        y="amount",
        color="category",
        title=title,
        histfunc="sum",
        category_orders={"category": sorted(dataframe["category"].unique())},
    )
    configure_monthly_chart(chart)
    # Add a trendline that is shifted left to the middle of the month.
    line_df = dataframe.resample("M").sum(numeric_only=True)
    line_df.index = line_df.index.map(lambda x: pd.to_datetime(x.strftime("%Y-%m-15")))
    line_chart = px.scatter(line_df, x=line_df.index, y="amount", trendline="lowess")
    line_chart.update_traces(showlegend=True)
    for trace in line_chart.data:
        if trace.mode == "lines":
            chart.add_trace(trace)
    return chart


def write_plots(output_file, plots):
    """Write out html file with plots."""
    output_file.write(
        plots[0].to_html(full_html=False, include_plotlyjs="cdn", default_height="50%")
    )
    for plot in plots[1:]:
        output_file.write(
            plot.to_html(full_html=False, include_plotlyjs=False, default_height="50%")
        )


def get_ledger_csv():
    """Get income/expense ledger csv as a StringIO."""
    return io.StringIO(subprocess.check_output(LEDGER_CSV_CMD, shell=True, text=True))


def main():
    """Main."""
    ledger_df = pd.read_csv(
        get_ledger_csv(),
        index_col=0,
        parse_dates=True,
        infer_datetime_format=True,
        names=[
            "date",
            "skip",
            "payee",
            "category",
            "currency",
            "amount",
            "skip2",
            "tag",
        ],
        usecols=["date", "payee", "category", "amount"],
    )["2023":]
    # Make virtual transactions real.
    ledger_df["category"] = ledger_df["category"].replace(r"[()]", "", regex=True)
    ledger_summarized_df = ledger_df.copy()
    ledger_summarized_df["category"] = ledger_summarized_df["category"].replace(
        r"(.+:.+):.+", r"\1", regex=True
    )
    get_average_monthly_top_expenses(ledger_df)

    with common.temporary_file_move(f"{HTML_PREFIX}/index.html") as output_file:
        write_plots(
            output_file,
            [
                get_income_expense_yearly_chart(ledger_summarized_df),
                get_yearly_chart(ledger_summarized_df, "Income", "Yearly Income"),
                get_yearly_chart(ledger_summarized_df, "Expenses", "Yearly Expenses"),
                get_income_expense_monthly_chart(ledger_summarized_df),
                get_monthly_chart(ledger_summarized_df, "Income", "Monthly Income"),
                get_monthly_chart(ledger_summarized_df, "Expenses", "Monthly Expenses"),
                get_monthly_chart(ledger_df, "Income", "Monthly Income Categorized"),
                get_monthly_chart(
                    ledger_df, "Expenses", "Monthly Expenses Categorized"
                ),
                get_average_monthly_income_expenses_chart(ledger_df),
                get_average_monthly_top_expenses(ledger_df),
            ],
        )


if __name__ == "__main__":
    main()
