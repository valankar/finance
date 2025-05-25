import io
import subprocess

import pandas as pd

import common


class Futures:
    def future_quote(self, ticker: str) -> pd.Series:
        fq = common.get_future_quote(ticker)
        return pd.Series([fq.mark, fq.multiplier])

    @property
    def ledger_df(self) -> pd.DataFrame:
        cmd = (
            f"""{common.LEDGER_BIN} -f {common.LEDGER_DAT} --limit 'commodity=~/^"\\//' """
            + 'bal --no-total --flat --balance-format "%(partial_account)\n%(strip(T))\n"'
        )
        entries = []
        account = ""
        for line in io.StringIO(subprocess.check_output(cmd, shell=True, text=True)):
            if line[0].isalpha():
                account = line.strip().split(":")[-1]
                continue
            count = int(line.split(maxsplit=1)[0])
            future_name = line.split(maxsplit=1)[1].strip().strip('"')
            commodity = future_name.split()[0]
            trade_price = float(future_name.split()[-1])
            if account:
                current_price, multiplier = self.future_quote(commodity)
                entries.append(
                    {
                        "account": account,
                        "commodity": commodity,
                        "count": count,
                        "value": (current_price - trade_price) * multiplier * count,
                        "notional_value": current_price * multiplier * count,
                        "current_price": current_price,
                        "trade_price": trade_price,
                        "multiplier": multiplier,
                        "future": future_name,
                    }
                )
        return pd.DataFrame(entries)

    @property
    def futures_df(self) -> pd.DataFrame:
        df = self.ledger_df.groupby(["account", "commodity"]).agg(
            {
                "count": "sum",
                "value": "sum",
                "notional_value": "sum",
                "current_price": "first",
                "trade_price": "mean",
                "multiplier": "first",
            }
        )
        return df

    @property
    def notional_values_df(self) -> pd.DataFrame:
        df = self.futures_df.reset_index()[["commodity", "notional_value"]]
        df["ticker"] = df["commodity"].str[:-3]
        df = (
            df.drop(columns="commodity")
            .groupby("ticker")
            .sum()
            .rename(columns={"notional_value": "value"})
        )
        return df
