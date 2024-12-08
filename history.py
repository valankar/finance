#!/usr/bin/env python3
"""Write finance history."""

import pandas as pd

import common
import stock_options
from common import get_ledger_balance

LEDGER_LIQUID_CMD = (
    f"""{common.LEDGER_PREFIX} --limit 'commodity=~/^(SWVXX|\\\\$|CHF|GBP|SGD|"SPX)/' """
    "--limit 'not(account=~/(Retirement|Precious Metals|Zurcher)/)' -J "
    "-n bal \\(^assets or ^liabilities\\)"
)
COMMODITIES_REGEX = "(GLD|SGOL|SIVR|COIN|BITX|MSTR)"
LEDGER_COMMODITIES_CMD = (
    f"""{common.LEDGER_PREFIX} -J -n --limit 'commodity=~/^{COMMODITIES_REGEX}/' bal """
    '^"Assets:Investments"'
)
ETFS_REGEX = "(SCH|SW[AIT]|IBKR|SGOV)"
LEDGER_ETFS_CMD = (
    f"""{common.LEDGER_PREFIX} --limit 'commodity=~/^{ETFS_REGEX}/' -J -n bal """
    '^"Assets:Investments:.*Broker.*"'
)
LEDGER_IRA_CMD = (
    f"""{common.LEDGER_PREFIX} --limit 'commodity=~/^SWYGX/' -J -n bal """
    '^"Assets:Investments:Retirement:Charles Schwab IRA"'
)
LEDGER_REAL_ESTATE_CMD = f'{common.LEDGER_PREFIX} -J -n bal ^"Assets:Real Estate"'
LEDGER_UBS_PILLAR_CMD = (
    f"{common.LEDGER_PREFIX} -J -n bal "
    '^"Assets:Investments:Retirement:UBS Vested Benefits"'
)
LEDGER_ZURCHER_CMD = f'{common.LEDGER_PREFIX} -J -n bal ^"Assets:Zurcher Kantonal"'


def main():
    """Main."""
    options_df = stock_options.options_df_with_value()
    commodities_options = options_df[options_df["ticker"].str.match(COMMODITIES_REGEX)][
        "value"
    ].sum()
    etfs_options = options_df[options_df["ticker"].str.match(ETFS_REGEX)]["value"].sum()
    commodities = get_ledger_balance(LEDGER_COMMODITIES_CMD) + commodities_options
    etfs = get_ledger_balance(LEDGER_ETFS_CMD) + etfs_options
    total_investing = commodities + etfs
    total_real_estate = get_ledger_balance(LEDGER_REAL_ESTATE_CMD)

    # Retirement
    schwab_ira = get_ledger_balance(LEDGER_IRA_CMD)
    pillar2 = get_ledger_balance(LEDGER_UBS_PILLAR_CMD)
    zurcher = get_ledger_balance(LEDGER_ZURCHER_CMD)
    total_retirement = pillar2 + zurcher + schwab_ira

    history_df_data = {
        "total_real_estate": total_real_estate,
        "total_liquid": get_ledger_balance(LEDGER_LIQUID_CMD),
        "total_investing": total_investing,
        "total_retirement": total_retirement,
        "etfs": etfs,
        "commodities": commodities,
        "ira": schwab_ira,
        "pillar2": pillar2,
    }
    history_df = pd.DataFrame(
        history_df_data,
        index=[pd.Timestamp.now()],
        columns=list(history_df_data.keys()),
    )
    common.to_sql(history_df, "history")


if __name__ == "__main__":
    main()
