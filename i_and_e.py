#!/usr/bin/env python3
"""Create income and expense graphs."""

import io
import subprocess
from datetime import date

import pandas as pd
import plotly.express as px
from dateutil.relativedelta import relativedelta

import common

HTML_PREFIX = f"{common.PREFIX}i_and_e/"
LEDGER_CSV_CMD = f"{common.LEDGER_PREFIX} csv ^Expenses ^Income"
TOSHL_INCOME_TABLE = "toshl_income_export_2023-01-01"
TOSHL_EXPENSES_TABLE = "toshl_expenses_export_2023-01-01"


def get_ledger_csv():
    """Get income/expense ledger csv as a StringIO."""
    return io.StringIO(subprocess.check_output(LEDGER_CSV_CMD, shell=True, text=True))


def convert_toshl_usd(dataframe):
    """Change CHF to USD."""
    dataframe = dataframe.rename(
        columns={"Category": "category", "In main currency": "amount_chf"}
    ).rename_axis("date")
    dataframe = dataframe[:"2022"]
    forex_df = common.read_sql_table_resampled_last("forex")["CHFUSD"]
    dataframe = pd.merge_asof(dataframe, forex_df, left_index=True, right_index=True)
    dataframe["amount"] = dataframe["amount_chf"] * dataframe["CHFUSD"]
    dataframe = dataframe.drop(
        columns=[
            "Tags",
            "Currency",
            "Main currency",
            "Description",
            "Expense amount",
            "Income amount",
            "amount_chf",
            "CHFUSD",
        ],
        errors="ignore",
    )
    return dataframe


def get_toshl_expenses_dataframe():
    """Get historical data from Toshl export."""
    dataframe = common.read_sql_table(TOSHL_EXPENSES_TABLE, index_col="Date")
    # Remove unnecessary transactions.
    dataframe = dataframe[~dataframe["Category"].isin(["Reconciliation", "Transfer"])]
    # Remove things that are not expenses.
    for category, tag in (("Banking", "equity purchase"), ("Other", "equity purchase")):
        dataframe = dataframe[
            ~((dataframe["Category"] == category) & (dataframe["Tags"] == tag))
        ]
    for category, tag, new_category in (
        ("Food & Drinks", "groceries", "Food:Groceries"),
        ("Food & Drinks", "alcohol", "Food:Alcohol"),
        ("Food & Drinks", "restaurants", "Food:Restaurants"),
        ("Food & Drinks", None, "Food:Groceries"),
        ("Gifts", None, "Gifts"),
        ("Music", "accessories", "Music:Accessories"),
        ("Music", "massage", "Entertainment:Massage"),
        ("Music", ["subscription", "apps"], "Music:Apps"),
        ("Music", None, "Music:Apps"),
        ("Health & Personal Care", "massage", "Entertainment:Massage"),
        ("Health & Personal Care", "insurance", "Health:Insurance"),
        ("Health & Personal Care", "medicine", "Health:Medicine"),
        ("Health & Personal Care", "medical services", "Health:Doctor"),
        ("Health & Personal Care", "gym", "Health:Exercise"),
        (
            "Health & Personal Care",
            ["accessories", "cosmetics", "devices"],
            "Health:Accessories",
        ),
        ("Health & Personal Care", None, "Health:Other"),
        ("Computer", "apps", "Home:Computer:Apps"),
        ("Computer", "games", "Entertainment:Games"),
        ("Computer", ["accessories", "devices", "music"], "Home:Computer:Accessories"),
        ("Computer", ["subscription", "books", "publications"], "Home:Computer:Apps"),
        ("Computer", "hosting", "Internet Hosting"),
        ("Computer", "internet", "Home:Internet"),
        ("Computer", "mobile phone", "Home:Phone"),
        ("Loans", None, "Rental Property:Mortgage"),
        ("Home & Utilities", "cleaning", "Home:Cleaning"),
        ("Home & Utilities", "hosting", "Internet Hosting"),
        ("Home & Utilities", "internet", "Home:Internet"),
        ("Home & Utilities", "rent", "Home:Rent"),
        ("Home & Utilities", "electricity", "Home:Electricity"),
        ("Home & Utilities", "groceries", "Food:Groceries"),
        ("Home & Utilities", "subscription", "Home:Other"),
        ("Home & Utilities", "furniture", "Home:Furniture"),
        ("Home & Utilities", "hoa", "Rental Property:HOA"),
        ("Home & Utilities", ["mobile phone", "landline phone"], "Home:Phone"),
        ("Home & Utilities", ["water", "heating"], "Home:Water & Heating"),
        (
            "Home & Utilities",
            ["accessories", "Tools", "devices", "movies & TV"],
            "Home:Accessories",
        ),
        ("Home & Utilities", ["legal", "lawyer"], "Home:Legal"),
        ("Taxes", "income tax", "Taxes:Income"),
        ("Taxes", "servicing", "Taxes:Preparation"),
        ("Taxes", "movies & TV", "Home:Television"),
        ("Taxes", "membership fees", "Home:Television"),
        ("Taxes", "Immigration", "Taxes:Immigration"),
        ("Taxes", "property tax", "Rental Property:Taxes"),
        ("Banking", None, "Banking:Fees"),
        ("Transport", ["train", "subscription", "bus"], "Transportation:Public"),
        ("Transport", "taxi", "Transportation:Taxi"),
        ("Transport", ["car", "fuel", "toll", "parking"], "Transportation:Car"),
        ("Transport", "airplane", "Transportation:Air"),
        ("Transport", "bicycle", "Transportation:Bike"),
        ("Transport", None, "Transportation:Public"),
        ("Rental Property Management", None, "Rental Property:Management"),
        ("Charity", None, "Gifts"),
        ("Leisure", "airplane", "Transportation:Air"),
        ("Leisure", "travel", "Transportation:Air"),
        ("Leisure", "accommodation", "Transportation:Hotel"),
        ("Leisure", "events", "Entertainment:Concerts"),
        ("Leisure", "massage", "Entertainment:Massage"),
        ("Leisure", "adult fun", "Entertainment:Massage"),
        ("Leisure", "sightseeing", "Entertainment:Sightseeing"),
        ("Leisure", "movies & TV", "Entertainment:Movies"),
        ("Leisure", "books", "Entertainment:Books"),
        ("Leisure", "games", "Entertainment:Games"),
        ("Leisure", None, "Entertainment:Massage"),
        ("Other", "travel", "Transportation:Air"),
        ("Education", "tuition", "Education:Language"),
        ("Education", "apps", "Education:Language"),
        ("Education", "books", "Entertainment:Books"),
        ("Clothing & Footwear", None, "Home:Clothing"),
    ):
        if isinstance(tag, list):
            mask = dataframe["Tags"].isin(tag)
        elif isinstance(tag, str):
            mask = dataframe["Tags"] == tag
        else:
            mask = True
        dataframe.loc[
            (dataframe["Category"] == category) & mask,
            "Category",
        ] = f"Expenses:{new_category}"

    dataframe.loc[
        (dataframe["Category"] == "Home & Utilities") & (dataframe["Tags"].isna()),
        "Category",
    ] = "Expenses:Home:Accessories"
    dataframe.loc[
        (dataframe["Category"] == "Home & Utilities")
        & (dataframe["Tags"] == "insurance")
        & (dataframe["Currency"] == "CHF"),
        "Category",
    ] = "Expenses:Home:Insurance"
    dataframe.loc[
        (dataframe["Category"] == "Home & Utilities")
        & (dataframe["Tags"] == "insurance")
        & (dataframe["Currency"] == "USD"),
        "Category",
    ] = "Expenses:Rental Property:Insurance"
    dataframe.loc[
        (dataframe["Category"] == "Home & Utilities")
        & (dataframe["Tags"].isin(["home improvement", "building upkeep"]))
        & (dataframe["Description"].str.contains("coral", case=False)),
        "Category",
    ] = "Expenses:Home:Repairs"
    dataframe.loc[
        (dataframe["Category"] == "Home & Utilities")
        & (dataframe["Tags"] == "home improvement"),
        "Category",
    ] = "Expenses:Home:Repairs"
    dataframe.loc[
        (dataframe["Category"] == "Home & Utilities")
        & (dataframe["Tags"] == "building upkeep"),
        "Category",
    ] = "Expenses:Rental Property:Repairs"
    dataframe.loc[
        (dataframe["Category"] == "Taxes")
        & (dataframe["Tags"].isna())
        & (dataframe["Description"].str.contains("swiss", case=False)),
        "Category",
    ] = "Expenses:Taxes:Preparation"
    dataframe.loc[
        (dataframe["Category"] == "Taxes")
        & (dataframe["Tags"].isna())
        & (
            dataframe["Description"].isna()
            | dataframe["Description"].str.contains("customs", case=False)
        ),
        "Category",
    ] = "Expenses:Taxes:Customs"
    dataframe.loc[
        (dataframe["Category"] == "Computer")
        & (dataframe["Tags"].isna())
        & (dataframe["Description"].str.contains("riccardo", case=False)),
        "Category",
    ] = "Expenses:Sales:Fees"
    dataframe.loc[
        (dataframe["Category"] == "Computer") & (dataframe["Tags"].isna()),
        "Category",
    ] = "Expenses:Home:Computer:Accessories"
    dataframe.loc[
        (dataframe["Category"] == "Other")
        & (dataframe["Tags"].isna())
        & (dataframe["Description"].str.contains("sale", case=False)),
        "Category",
    ] = "Expenses:Sales:Fees"
    dataframe.loc[
        (dataframe["Category"] == "Other")
        & (dataframe["Tags"].isna())
        & (dataframe["Description"].str.contains("gold", case=False)),
        "Category",
    ] = "Expenses:Precious Metals:Purchase"
    dataframe.loc[
        (dataframe["Category"] == "Other") & (dataframe["Tags"].isna()),
        "Category",
    ] = "Expenses:Gifts"
    dataframe = convert_toshl_usd(dataframe)
    return dataframe


def get_toshl_income_dataframe():
    """Get historical data from Toshl export."""
    dataframe = common.read_sql_table(TOSHL_INCOME_TABLE, index_col="Date")
    # Remove unnecessary transactions.
    dataframe = dataframe[~dataframe["Category"].isin(["Reconciliation", "Transfer"])]
    # Make dataframe like ledger.
    dataframe.loc[
        dataframe["Category"] == "Rental", "Category"
    ] = "Income:Rental Property:Rent"
    dataframe.loc[
        dataframe["Category"] == "Property", "Category"
    ] = "Income:Sales:Property"
    dataframe.loc[
        (dataframe["Category"] == "Other") & (dataframe["Currency"] == "CHF"),
        "Category",
    ] = "Income:Sales"
    dataframe.loc[
        dataframe["Tags"] == "cryptocurrency", "Category"
    ] = "Income:Cryptocurrency"
    for category in (
        "Dividends",
        "Interest",
        "Other",
        "Salary",
        "Sales",
        "Grants",
        "Reimbursements",
    ):
        dataframe.loc[
            dataframe["Category"] == category, "Category"
        ] = f"Income:{category}"
    dataframe = convert_toshl_usd(dataframe)
    return dataframe


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


def get_historical_average_labels():
    """Get labels for historical averages."""
    return (36, 24, 12, 6, 3, 1), (
        "Last 3 years",
        "Last 2 years",
        "Last year",
        "Last 6 months",
        "Last 3 months",
        "Last month",
    )


def get_average_monthly_top_expenses(ledger_df):
    """Get average monthly top expenses."""
    expense_df = get_dataframe(ledger_df, "Expenses")
    months_back, labels = get_historical_average_labels()
    categories = []
    expenses = []
    for i in months_back:
        start = date.today() + relativedelta(months=-i)
        exp_max_df = (
            expense_df.loc[start:]
            .groupby("category")
            .mean(numeric_only=True)
            .agg(["idxmax", "max"])
        )
        categories.append(exp_max_df.iloc[0].values.item())
        expenses.append(exp_max_df.iloc[1].values.item())
    top_expenses_df = pd.DataFrame(
        {"category": categories, "expense": expenses}, index=labels
    )
    chart = px.bar(
        top_expenses_df,
        x=top_expenses_df.index,
        y="expense",
        color="category",
        text_auto=",.0f",
        title="Average Monthly Top Expenses",
    )
    chart.update_xaxes(title_text="", categoryarray=labels, categoryorder="array")
    chart.update_yaxes(title_text="USD")
    return chart


def get_average_monthly_income_expenses_chart(ledger_df):
    """Get average income and expenses chart."""
    joined_df = get_income_expense_df(ledger_df)
    months_back, labels = get_historical_average_labels()
    incomes = []
    expenses = []
    for i in months_back:
        start = date.today() + relativedelta(months=-i)
        incomes.append(joined_df[start:].sum()["income"] / i)
        expenses.append(joined_df[start:].sum()["expenses"] / i)
    i_and_e_avg_df = pd.DataFrame(
        {"income": incomes, "expense": expenses},
        index=labels,
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
    dataframe = get_income_expense_df(ledger_df)
    # Only keep last 12 months.
    dataframe = dataframe[
        dataframe.resample("M")
        .sum(numeric_only=True)
        .iloc[-12]
        .name.strftime("%Y-%m") :
    ]
    chart = px.histogram(
        dataframe,
        x=dataframe.index,
        y=dataframe.columns,
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
    # Only keep last 12 months.
    dataframe = dataframe[
        dataframe.resample("M")
        .sum(numeric_only=True)
        .iloc[-12]
        .name.strftime("%Y-%m") :
    ]
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


def get_ledger_dataframes():
    """Get ledger and ledger summarized dataframes."""
    ledger_df = pd.read_csv(
        get_ledger_csv(),
        index_col=0,
        parse_dates=True,
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
        usecols=["date", "category", "amount"],
    )["2023":]
    ledger_df = pd.concat(
        [get_toshl_income_dataframe(), get_toshl_expenses_dataframe(), ledger_df]
    ).sort_index()
    # Make virtual transactions real.
    ledger_df["category"] = ledger_df["category"].replace(r"[()]", "", regex=True)
    ledger_summarized_df = ledger_df.copy()
    ledger_summarized_df["category"] = ledger_summarized_df["category"].replace(
        r"([^:]+:[^:]+):.+", r"\1", regex=True
    )
    return ledger_df, ledger_summarized_df
