CREATE TABLE history (
	date DATETIME, 
	total_liquid FLOAT, 
	total_real_estate FLOAT, 
	total_retirement FLOAT, 
	total_investing FLOAT, 
	etfs FLOAT, 
	commodities FLOAT, 
	"ira" FLOAT, 
	pillar2 FLOAT
, total_no_homes float generated always as (total_liquid+total_retirement+total_investing), total float generated always as (total_no_homes+total_real_estate));
CREATE TABLE forex (
	date DATETIME, 
	"CHFUSD" FLOAT, 
	"SGDUSD" FLOAT
);
CREATE TABLE wealthfront_cash_yield (
	date DATETIME, 
	percent FLOAT
);
CREATE TABLE swvxx_yield (
	date DATETIME, 
	percent FLOAT
);
CREATE TABLE "schwab_etfs_amounts" (
	date DATETIME, 
	"SCHA" FLOAT, 
	"SCHF" FLOAT, 
	"SCHR" FLOAT, 
	"SCHX" FLOAT
, [SWTSX] FLOAT, [SWISX] FLOAT, [SWAGX] FLOAT, [SCHZ] FLOAT, [IBKR] FLOAT, [SCHB] FLOAT, [GLDM] FLOAT, [SGOL] FLOAT, [SIVR] FLOAT, [SCHE] FLOAT, [SCHO] FLOAT, [COIN] FLOAT, [BITX] FLOAT, [MSTR] FLOAT, [SGOV] FLOAT, [VV] FLOAT, [PLTR] FLOAT);
CREATE TABLE schwab_etfs_prices (
	date DATETIME, 
	"SCHA" FLOAT, 
	"SCHF" FLOAT, 
	"SCHR" FLOAT, 
	"SCHX" FLOAT
, [SWTSX] FLOAT, [SWISX] FLOAT, [SWAGX] FLOAT, [SCHZ] FLOAT, [IBKR] FLOAT, [SCHB] FLOAT, [GLDM] FLOAT, [SGOL] FLOAT, [SIVR] FLOAT, [SCHE] FLOAT, [SCHO] FLOAT, [COIN] FLOAT, [BITX] FLOAT, [MSTR] FLOAT, [SGOV] FLOAT, [VV] FLOAT, [PLTR] FLOAT);
CREATE TABLE schwab_ira_amounts (
	date DATETIME, 
	"SWYGX" FLOAT
);
CREATE TABLE schwab_ira_prices (
	date DATETIME, 
	"SWYGX" FLOAT
);
CREATE TABLE "toshl_income_export_2023-01-01" (
	"Date" DATETIME, 
	"Category" TEXT, 
	"Tags" TEXT, 
	"Income amount" FLOAT, 
	"Currency" TEXT, 
	"In main currency" FLOAT, 
	"Main currency" TEXT, 
	"Description" TEXT
);
CREATE TABLE "toshl_expenses_export_2023-01-01" (
	"Date" DATETIME, 
	"Category" TEXT, 
	"Tags" TEXT, 
	"Expense amount" FLOAT, 
	"Currency" TEXT, 
	"In main currency" FLOAT, 
	"Main currency" TEXT, 
	"Description" TEXT
);
CREATE TABLE sqlite_stat1(tbl,idx,stat);
CREATE TABLE swygx_holdings (
	date DATETIME, 
	"SCHX" FLOAT, 
	"SCHF" FLOAT, 
	"SCHZ" FLOAT, 
	"SCHH" FLOAT, 
	"SCHA" FLOAT, 
	"SCHE" FLOAT, 
	"USD" FLOAT, 
	"SCHO" FLOAT, 
	"SVUXX" FLOAT
, SGUXX FLOAT);
CREATE TABLE swtsx_market_cap (
	date DATETIME, 
	"US_LARGE_CAP" FLOAT, 
	"US_SMALL_CAP" FLOAT
);
CREATE TABLE interactive_brokers_margin_rates (
	date DATETIME, 
	"USD" FLOAT, 
	"CHF" FLOAT
);
CREATE TABLE index_prices (
	date DATETIME, 
	"^SPX" FLOAT, [^SSMI] FLOAT);
CREATE TABLE brokerage_totals (
	date DATETIME, 
	"Equity Balance" FLOAT, 
	"30% Equity Balance" FLOAT, 
	"50% Equity Balance" FLOAT, 
	"Loan Balance" FLOAT, 
	"Total" FLOAT, 
	"Distance to 30%" FLOAT, 
	"Distance to 50%" FLOAT, 
	"Brokerage" TEXT
);
CREATE TABLE real_estate_prices (
	date DATETIME, 
	name TEXT, 
	value BIGINT, 
	site TEXT
);
CREATE TABLE real_estate_rents (
	date DATETIME, 
	name TEXT, 
	value BIGINT, 
	site TEXT
);
CREATE TABLE fedfunds (
	date DATETIME, 
	percent FLOAT
);
CREATE TABLE sofr (
	date DATETIME, 
	percent FLOAT
);
CREATE INDEX ix_history_date ON history (date);
CREATE INDEX ix_forex_date ON forex (date);
CREATE INDEX ix_wealthfront_cash_yield_date ON wealthfront_cash_yield (date);
CREATE INDEX ix_swvxx_yield_date ON swvxx_yield (date);
CREATE INDEX ix_schwab_etfs_prices_date ON schwab_etfs_prices (date);
CREATE INDEX ix_schwab_etfs_amounts_date ON schwab_etfs_amounts (date);
CREATE INDEX ix_schwab_ira_amounts_date ON schwab_ira_amounts (date);
CREATE INDEX ix_schwab_ira_prices_date ON schwab_ira_prices (date);
CREATE INDEX "ix_toshl_income_export_2023-01-01_Date" ON "toshl_income_export_2023-01-01" ("Date");
CREATE INDEX "ix_toshl_expenses_export_2023-01-01_Date" ON "toshl_expenses_export_2023-01-01" ("Date");
CREATE INDEX ix_swygx_holdings_date ON swygx_holdings (date);
CREATE INDEX ix_swtsx_market_cap_date ON swtsx_market_cap (date);
CREATE INDEX ix_interactive_brokers_margin_rates_date ON interactive_brokers_margin_rates (date);
CREATE INDEX ix_index_prices_date ON index_prices (date);
CREATE INDEX ix_brokerage_totals_date ON brokerage_totals (date);
CREATE INDEX ix_real_estate_prices_date ON real_estate_prices (date);
CREATE INDEX ix_real_estate_rents_date ON real_estate_rents (date);
CREATE INDEX ix_fedfunds_date ON fedfunds (date);
CREATE INDEX ix_sofr_date ON sofr (date);
