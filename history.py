#!/usr/bin/env python3
"""Write finance history."""

from loguru import logger

import common
import ledger_amounts
import stock_options
from ledger_ops import get_ledger_balance

LEDGER_LIQUID_CMD = f"{common.LEDGER_CURRENCIES_OPTIONS_CMD} --limit 'not(account=~/(Retirement|Precious Metals|Zurcher)/)' -J -n bal ^assets ^liabilities"
LEDGER_COMMODITIES_CMD = (
    f"""{common.LEDGER_PREFIX} -J -n --limit 'commodity=~/{common.COMMODITIES_REGEX}/' bal """
    '^"Assets:Investments"'
)
LEDGER_ETFS_CMD = (
    f"{common.LEDGER_PREFIX} {ledger_amounts.LEDGER_LIMIT_ETFS} --limit 'commodity!~/{common.COMMODITIES_REGEX}/' -J -n bal "
    '^"Assets:Investments:.*Broker.*"'
)
LEDGER_IRA_CMD = (
    f"{common.LEDGER_PREFIX} {ledger_amounts.LEDGER_LIMIT_ETFS} -J -n bal "
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
    if (options_data := stock_options.get_options_data()) is None:
        raise ValueError("No options data available")
    options_df = options_data.opts.all_options
    commodities_options = options_df.query(
        f"ticker.str.fullmatch('{common.COMMODITIES_REGEX}')"
    )["value"].sum()
    etfs_options = options_df.query(
        f"not ticker.str.fullmatch('{common.COMMODITIES_REGEX}') and not ticker.str.fullmatch('SMI|SPX')"
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
    common.insert_sql("history", history_df_data)


if __name__ == "__main__":
    main()
