#!/usr/bin/env python3
"""Write finance history."""

import common
from ledger_ops import get_ledger_balance

LEDGER_TOTAL_CMD = f"""{common.LEDGER_PREFIX} -J -n bal \\(^assets ^liabilities\\)"""
LEDGER_TOTAL_NO_RE_CMD = f"""{common.LEDGER_PREFIX} -J -n bal \\(^assets ^liabilities\\) and not\\("Real Estate" "Rental Property"\\)"""
LEDGER_LIQUID_CMD = f"""{common.LEDGER_CURRENCIES_CMD} -J -n bal \\(^assets ^liabilities\\) and not\\(Futures Retirement "Real Estate" "Rental Property"\\)"""
LEDGER_INVESTMENTS_CMD = f'{common.LEDGER_PREFIX} -J -n bal ^"Assets:Investments"'
LEDGER_RETIREMENT_CMD = (
    f'{common.LEDGER_PREFIX} -J -n bal ^"Assets:Investments:Retirement"'
)
LEDGER_IRA_CMD = f'{common.LEDGER_PREFIX} -J -n bal ^"Assets:Investments:Retirement:Charles Schwab IRA"'
LEDGER_UBS_PILLAR_CMD = f'{common.LEDGER_PREFIX} -J -n bal ^"Assets:Investments:Retirement:UBS Vested Benefits"'
LEDGER_REAL_ESTATE_CMD = (
    f'{common.LEDGER_PREFIX} -J -n bal ^"Assets:Real Estate" ^"Assets:Rental Property"'
)


def main():
    """Main."""
    total = get_ledger_balance(LEDGER_TOTAL_CMD)
    total_liquid = get_ledger_balance(LEDGER_LIQUID_CMD)
    total_no_real_estate = get_ledger_balance(LEDGER_TOTAL_NO_RE_CMD)
    total_investing = get_ledger_balance(LEDGER_INVESTMENTS_CMD)
    total_real_estate = get_ledger_balance(LEDGER_REAL_ESTATE_CMD)

    # Retirement
    schwab_ira = get_ledger_balance(LEDGER_IRA_CMD)
    pillar2 = get_ledger_balance(LEDGER_UBS_PILLAR_CMD)
    total_retirement = pillar2 + schwab_ira

    history_df_data = {
        "total": total,
        "total_no_real_estate": total_no_real_estate,
        "total_real_estate": total_real_estate,
        "total_liquid": total_liquid,
        "total_investing": total_investing,
        "total_retirement": total_retirement,
        "ira": schwab_ira,
        "pillar2": pillar2,
    }
    common.insert_sql("history", history_df_data)


if __name__ == "__main__":
    main()
