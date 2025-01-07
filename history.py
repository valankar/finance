#!/usr/bin/env python3
"""Write finance history."""

import pandas as pd
from loguru import logger

import common
import ledger_amounts
import stock_options
from common import get_ledger_balance

LEDGER_LIQUID_CMD = (
    f"{common.LEDGER_PREFIX} --limit 'commodity=~/{common.CURRENCIES_REGEX}/ or commodity=~/{common.OPTIONS_LOAN_REGEX}/' "
    "--limit 'not(account=~/(Retirement|Precious Metals|Zurcher)/)' -J "
    "-n bal ^assets ^liabilities"
)
COMMODITIES_REGEX = "^(GLDM|SGOL|SIVR|COIN|BITX|MSTR)$"
LEDGER_COMMODITIES_CMD = (
    f"""{common.LEDGER_PREFIX} -J -n --limit 'commodity=~/{COMMODITIES_REGEX}/' bal """
    '^"Assets:Investments"'
)
LEDGER_ETFS_CMD = (
    f"""{common.LEDGER_PREFIX} {ledger_amounts.LEDGER_LIMIT_ETFS} --limit 'commodity!~/{COMMODITIES_REGEX}/' -J -n bal """
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
    options_df = stock_options.options_df(with_value=True)
    commodities_options = options_df.query(
        f"ticker.str.fullmatch('{COMMODITIES_REGEX}')"
    )["value"].sum()
    etfs_options = options_df.query(
        f"not ticker.str.fullmatch('{COMMODITIES_REGEX}') and not ticker.str.fullmatch('SMI|SPX')"
    )["value"].sum()
    logger.info(f"Commodities options: {commodities_options}")
    logger.info(f"ETFs options: {etfs_options}")
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
    diff_df = (
        pd.concat([common.read_sql_last("history"), history_df], join="inner")
        .diff()
        .dropna()
    )
    if diff_df.sum(axis=1).sum():
        with common.pandas_options():
            logger.info(f"History difference:\n{diff_df}")
            logger.info(f"Writing history:\n{history_df}")
        common.to_sql(history_df, "history")
    else:
        logger.info("History hot changed. Not writing new entry.")


if __name__ == "__main__":
    main()
