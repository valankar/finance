#!/usr/bin/env python3
"""Write finance history."""

import common
import ledger_amounts
import margin_loan
from ledger_ops import get_ledger_balance

LEDGER_LIQUID_CMD = f"{common.LEDGER_CURRENCIES_OPTIONS_CMD} --limit 'not(account=~/(Investments|Precious Metals)/)' -J -n bal ^assets ^liabilities"
LEDGER_IRA_CMD = (
    f"{common.LEDGER_PREFIX} {ledger_amounts.LEDGER_LIMIT_ETFS} -J -n bal "
    '^"Assets:Investments:Retirement:Charles Schwab IRA"'
)
LEDGER_REAL_ESTATE_CMD = f'{common.LEDGER_PREFIX} -J -n bal ^"Assets:Real Estate"'
LEDGER_UBS_PILLAR_CMD = (
    f"{common.LEDGER_PREFIX} -J -n bal "
    '^"Assets:Investments:Retirement:UBS Vested Benefits"'
)


def main():
    """Main."""
    total_liquid = get_ledger_balance(LEDGER_LIQUID_CMD)
    total_investing = 0
    for broker in margin_loan.LOAN_BROKERAGES:
        df = margin_loan.get_balances_broker(broker)
        total_investing += df["Equity Balance"].sum()
        total_liquid += df["Loan Balance"].sum()
    total_real_estate = get_ledger_balance(LEDGER_REAL_ESTATE_CMD)

    # Retirement
    schwab_ira = get_ledger_balance(LEDGER_IRA_CMD)
    pillar2 = get_ledger_balance(LEDGER_UBS_PILLAR_CMD)
    total_retirement = pillar2 + schwab_ira

    history_df_data = {
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
