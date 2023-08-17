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
);
CREATE TABLE "commodities_amounts" (
	date DATETIME, 
	"GOLD" FLOAT, 
	"SILVER" FLOAT
);
CREATE TABLE schwab_etfs_prices (
	date DATETIME, 
	"SCHA" FLOAT, 
	"SCHF" FLOAT, 
	"SCHR" FLOAT, 
	"SCHX" FLOAT
);
CREATE TABLE commodities_prices (
	date TIMESTAMP, 
	"GOLD" FLOAT, 
	"SILVER" FLOAT
);
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
CREATE TABLE function_result (
	date DATETIME, 
	name TEXT, 
	success BOOLEAN, 
	error TEXT
);
CREATE TABLE real_estate(name TEXT PRIMARY KEY NOT NULL);
CREATE TABLE real_estate_prices(date DATETIME, name TEXT NOT NULL REFERENCES real_estate(name), redfin_value BIGINT, zillow_value BIGINT, value BIGINT generated always as ((redfin_value+zillow_value)/2));
CREATE TABLE real_estate_rents(date DATETIME, name TEXT NOT NULL REFERENCES real_estate(name), value BIGINT);
CREATE TABLE sqlite_stat1(tbl,idx,stat);
CREATE INDEX ix_history_date ON history (date);
CREATE INDEX ix_forex_date ON forex (date);
CREATE INDEX ix_wealthfront_cash_yield_date ON wealthfront_cash_yield (date);
CREATE INDEX ix_swvxx_yield_date ON swvxx_yield (date);
CREATE INDEX ix_schwab_etfs_prices_date ON schwab_etfs_prices (date);
CREATE INDEX ix_schwab_etfs_amounts_date ON schwab_etfs_amounts (date);
CREATE INDEX ix_commodities_amounts_date ON commodities_amounts (date);
CREATE INDEX ix_commodities_prices_date ON commodities_prices (date);
CREATE INDEX ix_schwab_ira_amounts_date ON schwab_ira_amounts (date);
CREATE INDEX ix_schwab_ira_prices_date ON schwab_ira_prices (date);
CREATE INDEX "ix_toshl_income_export_2023-01-01_Date" ON "toshl_income_export_2023-01-01" ("Date");
CREATE INDEX "ix_toshl_expenses_export_2023-01-01_Date" ON "toshl_expenses_export_2023-01-01" ("Date");
CREATE INDEX ix_function_result_date ON function_result (date);
CREATE INDEX ix_real_estate_prices_date ON real_estate_prices (date);
CREATE INDEX ix_real_estate_rents_date ON real_estate_rents (date);
CREATE TABLE fedfunds (
	date DATETIME, 
	percent FLOAT
);
CREATE INDEX ix_fedfunds_date ON fedfunds (date);
CREATE TABLE sofr (
	date DATETIME, 
	percent FLOAT
);
CREATE INDEX ix_sofr_date ON sofr (date);
