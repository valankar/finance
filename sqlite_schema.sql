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
CREATE INDEX ix_history_date ON history (date);
CREATE TABLE account_history (
	date DATETIME, 
	"CHF_Cash" FLOAT, 
	"CHF_UBS_Pillar 2" FLOAT, 
	"CHF_UBS_Primary" FLOAT, 
	"CHF_UBS_Visa" FLOAT, 
	"CHF_Wise" FLOAT, 
	"CHF_Zurcher" FLOAT, 
	"SGD_Wise" FLOAT, 
	"USD_Ally" FLOAT, 
	"USD_Apple_Card" FLOAT, 
	"USD_Apple_Cash" FLOAT, 
	"USD_Bank of America_Cash Rewards Visa" FLOAT, 
	"USD_Bank of America_Checking" FLOAT, 
	"USD_Bank of America_Travel Rewards Visa" FLOAT, 
	"USD_Charles Schwab_Brokerage_Cash" FLOAT, 
	"USD_Charles Schwab_Brokerage_SCHA" FLOAT, 
	"USD_Charles Schwab_Brokerage_SCHB" FLOAT, 
	"USD_Charles Schwab_Brokerage_SCHE" FLOAT, 
	"USD_Charles Schwab_Brokerage_SCHF" FLOAT, 
	"USD_Charles Schwab_Brokerage_SCHO" FLOAT, 
	"USD_Charles Schwab_Brokerage_SCHR" FLOAT, 
	"USD_Charles Schwab_Brokerage_SCHX" FLOAT, 
	"USD_Charles Schwab_Brokerage_SCHZ" FLOAT, 
	"USD_Charles Schwab_Checking" FLOAT, 
	"USD_Charles Schwab_IRA_Cash" FLOAT, 
	"USD_Charles Schwab_Pledged Asset Line" FLOAT, 
	"USD_Commodities_Gold" FLOAT, 
	"USD_Commodities_Silver" FLOAT, 
	"USD_Healthequity HSA" FLOAT, 
	"USD_Real Estate_California St" FLOAT, 
	"USD_Real Estate_Coral Lake" FLOAT, 
	"USD_Real Estate_Mt Vernon" FLOAT, 
	"USD_Real Estate_Northlake" FLOAT, 
	"USD_Real Estate_Villa Maria" FLOAT, 
	"USD_Treasury Direct" FLOAT, 
	"USD_Vanguard 401k_VWIAX" FLOAT, 
	"USD_Vanguard 401k_Vanguard Target Retirement 2040 Trust" FLOAT, 
	"USD_Wealthfront_Cash" FLOAT, 
	"USD_Wise" FLOAT
);
CREATE INDEX ix_account_history_date ON account_history (date);
CREATE TABLE forex (
	date DATETIME, 
	"CHFUSD" FLOAT, 
	"SGDUSD" FLOAT
);
CREATE INDEX ix_forex_date ON forex (date);
CREATE TABLE wealthfront_cash_yield (
	date DATETIME, 
	percent FLOAT
);
CREATE INDEX ix_wealthfront_cash_yield_date ON wealthfront_cash_yield (date);
CREATE TABLE swvxx_yield (
	date DATETIME, 
	percent FLOAT
);
CREATE INDEX ix_swvxx_yield_date ON swvxx_yield (date);
CREATE TABLE mtvernon (
	date DATETIME, 
	value BIGINT
);
CREATE INDEX ix_mtvernon_date ON mtvernon (date);
CREATE TABLE northlake (
	date DATETIME, 
	value BIGINT
);
CREATE INDEX ix_northlake_date ON northlake (date);
CREATE TABLE villamaria (
	date DATETIME, 
	value BIGINT
);
CREATE INDEX ix_villamaria_date ON villamaria (date);
CREATE TABLE mtvernon_rent (
	date DATETIME, 
	value BIGINT
);
CREATE INDEX ix_mtvernon_rent_date ON mtvernon_rent (date);
CREATE TABLE northlake_rent (
	date DATETIME, 
	value BIGINT
);
CREATE INDEX ix_northlake_rent_date ON northlake_rent (date);
CREATE TABLE villamaria_rent (
	date DATETIME, 
	value BIGINT
);
CREATE INDEX ix_villamaria_rent_date ON villamaria_rent (date);
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
CREATE TABLE IF NOT EXISTS "schwab_etfs_amounts" (
	date DATETIME, 
	"SCHA" FLOAT, 
	"SCHF" FLOAT, 
	"SCHR" FLOAT, 
	"SCHX" FLOAT
);
CREATE TABLE IF NOT EXISTS "commodities_amounts" (
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
CREATE INDEX ix_schwab_etfs_prices_date ON schwab_etfs_prices (date);
CREATE TABLE commodities_prices (
	date DATETIME, 
	"GOLD" FLOAT, 
	"SILVER" FLOAT
);
CREATE INDEX ix_commodities_prices_date ON commodities_prices (date);
CREATE INDEX ix_schwab_etfs_amounts_date ON schwab_etfs_amounts (date);
CREATE INDEX ix_commodities_amounts_date ON commodities_amounts (date);
