import os
import csv
import boto3
import psycopg2
from io import StringIO
from datetime import datetime


# ---------------------------------------------------------------------------
# Column mapping: (db_column_name, csv_column_name, type)
# type: 'd' = decimal/float, 's' = string, 'dt' = date
# ---------------------------------------------------------------------------
COLUMNS = [
    # Identity
    ('position_id',              'P:P:Position ID',                        's'),
    ('internal_position_id',     'P:P:_PositionID',                        's'),
    ('position_date',            'P:P:Date',                               'dt'),
    ('start_date',               'P:P:Start Date',                         'dt'),
    ('end_date',                 'P:P:End Date',                           'dt'),
    # Fund
    ('fund_id',                  'P:P:Fund ID',                            's'),
    ('internal_fund_id',         'P:P:_FundID',                            's'),
    ('fund_code',                'P:P:Fund Code',                          's'),
    ('fund_name',                'P:P:Fund Name',                          's'),
    ('fund_group_code',          'P:P:Fund Group Code',                    's'),
    ('fund_group_name',          'P:P:Fund Group Name',                    's'),
    ('fund_currency',            'P:P:Fund Currency',                      's'),
    ('fund_nav',                 'P:P:Fund NAV',                           'd'),
    ('fund_nav_base',            'P:P:Fund NAV Base',                      'd'),
    ('fund_official_nav',        'P:P:Fund Official NAV',                  'd'),
    ('fund_official_nav_date',   'P:P:Fund Official NAV Date',             'dt'),
    ('fund_shares',              'P:P:Fund Shares',                        'd'),
    ('fund_nav_per_share',       'P:P:Fund NAV per Share',                 'd'),
    ('fund_pct_of_nav',          'P:P:Fund % of NAV',                      'd'),
    # Strategy
    ('strategy_id',              'P:P:Strategy ID',                        's'),
    ('strategy_code',            'P:P:Strategy Code',                      's'),
    ('strategy_name',            'P:P:Strategy Name',                      's'),
    ('strategy_pm_name',         'P:P:Strategy Portfolio Manager Name',    's'),
    # Custodian
    ('custodian_id',             'P:P:Custodian ID',                       's'),
    ('custodian_code',           'P:P:Custodian Code',                     's'),
    ('custodian_name',           'P:P:Custodian Name',                     's'),
    # Security identifiers
    ('security_id',              'P:P:Security ID',                        's'),
    ('security_asset_id',        'P:P:Security Asset ID',                  's'),
    ('internal_security_id',     'P:P:_SecurityID',                        's'),
    ('security_sec_master_id',   'P:P:Security Sec Master ID',             's'),
    ('security_symbol',          'P:P:Security Symbol',                    's'),
    ('security_name',            'P:P:Security Name',                      's'),
    ('security_ticker',          'P:P:Security Ticker',                    's'),
    ('security_cusip',           'P:P:Security Cusip',                     's'),
    ('security_isin',            'P:P:Security Isin',                      's'),
    ('security_sedol',           'P:P:Security Sedol',                     's'),
    ('security_ric',             'P:P:Security Ric',                       's'),
    ('security_currency',        'P:P:Security Currency',                  's'),
    ('asset_class',              'P:P:Asset Class',                        's'),
    ('security_type_code',       'P:P:Security Type Code',                 's'),
    ('security_type_name',       'P:P:Security Type Name',                 's'),
    ('security_maturity_date',   'P:P:Security Maturity Date',             'dt'),
    ('security_pricing_factor',  'P:P:Security Pricing Factor',            'd'),
    ('security_industry_code',   'P:P:Security Industry Code',             's'),
    ('security_industry_name',   'P:P:Security Industry Name',             's'),
    ('security_industry_l1_code','P:P:Security Industry Level1 Code',      's'),
    ('security_industry_l1_name','P:P:Industry Level 1 Name',              's'),
    ('security_industry_l2_code','P:P:Security Industry Level2 Code',      's'),
    ('security_industry_l2_name','P:P:Security Industry Level2 Name',      's'),
    ('security_industry_l3_code','P:P:Security Industry Level3 Code',      's'),
    ('security_industry_l3_name','P:P:Security Industry Level3 Name',      's'),
    ('security_issuer_code',     'P:P:Security Issuer Code',               's'),
    ('issuer_name',              'P:P:Issuer Name',                        's'),
    ('country_name',             'P:P:Country Name',                       's'),
    ('currency_name',            'P:P:Currency Name',                      's'),
    ('risk_category_id',         'P:P:Risk Category ID',                   's'),
    ('risk_group_id',            'P:P:Risk Group ID',                      's'),
    ('conversion_ratio',         'P:P:Conversion Ratio',                   'd'),
    ('security_conversion_ratio','P:P:Security Conversion Ratio',          'd'),
    ('shares_outstanding',       'P:P:Shares Outstanding',                 'd'),
    # Underlying
    ('underlying_symbol',        'P:P:Underlying Symbol',                  's'),
    ('underlying_name',          'P:P:Underlying Name',                    's'),
    ('underlying_isin',          'P:P:Underlying Isin',                    's'),
    # Position details
    ('direction',                'P:P:Position Direction',                  's'),
    ('ls_flag',                  'P:P:L/S',                                's'),
    ('box_type',                 'P:P:Box Type',                           's'),
    ('box_type_id',              'P:P:Box Type ID',                        's'),
    ('position_qty',             'P:P:Position',                           'd'),
    ('settled_position',         'P:P:Settled Position',                   'd'),
    ('adjusted_position',        'P:P:Adjusted Position',                  'd'),
    ('position_adjustment',      'P:P:Position Adjustment',                'd'),
    ('unsettled_position',       'P:P:Unsettled Position',                 'd'),
    ('original_face',            'P:P:Original Face',                      'd'),
    # Pricing
    ('end_price',                'P:P:End Price',                          'd'),
    ('end_fx',                   'P:P:End FX',                             'd'),
    ('end_fund_fx',              'P:P:End Fund FX',                        'd'),
    ('end_fx_in_fund_ccy',       'P:P:End FX in Fund Ccy',                 'd'),
    ('end_price_date',           'P:P:End Price Date',                     'dt'),
    ('end_fx_date',              'P:P:End FX Date',                        'dt'),
    ('end_fund_fx_date',         'P:P:End Fund FX Date',                   'dt'),
    ('end_factor',               'P:P:End Factor',                         'd'),
    ('dirty_price',              'P:P:Dirty Price',                        'd'),
    ('open_price',               'P:P:Open Price',                         'd'),
    ('open_date',                'P:P:Open Date',                          'dt'),
    ('open_fx',                  'P:P:Open FX',                            'd'),
    ('open_fund_fx',             'P:P:Open Fund FX',                       'd'),
    ('open_fx_in_fund_ccy',      'P:P:Open FX in Fund Ccy',                'd'),
    ('wash_sale_warning',        'P:P:Wash Sale Warning',                  's'),
    # Market value
    ('market_value_base',        'P:P:Market Value Base',                  'd'),
    ('market_value_local',       'P:P:Market Value Local',                 'd'),
    ('market_value_fund',        'P:P:Market Value Fund',                  'd'),
    # Cost basis
    ('unit_cost_fund',           'P:P:Unit Cost Fund',                     'd'),
    ('unit_cost_base',           'P:P:Unit Cost Base',                     'd'),
    ('unit_cost_fund_fx',        'P:P:Unit Cost Fund FX',                  'd'),
    ('unit_cost_fx',             'P:P:Unit Cost FX',                       'd'),
    ('unit_cost_local',          'P:P:Unit Cost Local',                    'd'),
    ('average_price_base',       'P:P:Average Price Base',                 'd'),
    ('average_price_fund',       'P:P:Average Price Fund',                 'd'),
    ('average_price_local',      'P:P:Average Price Local',                'd'),
    ('total_cost_local',         'P:P:Total Cost Local',                   'd'),
    ('total_cost_fund',          'P:P:Total Cost Fund',                    'd'),
    ('total_cost_base',          'P:P:Total Cost Base',                    'd'),
    # Accrued
    ('end_accrued_local',        'P:P:End Accrued Local',                  'd'),
    ('end_accrued_base',         'P:P:End Accrued Base',                   'd'),
    ('end_accrued_fund',         'P:P:End Accrued Fund',                   'd'),
    ('end_paying_accrued',       'P:P:End Paying Accrued',                 'd'),
    ('end_receiving_accrued',    'P:P:End Receiving Accrued',              'd'),
    ('end_borrow_accrued',       'P:P:End Borrow Accrued',                 'd'),
    ('end_borrow_accrued_base',  'P:P:End Borrow Accrued Base',            'd'),
    ('end_borrow_accrued_fund',  'P:P:End Borrow Accrued Fund',            'd'),
    ('end_payable_rec_accrued',  'P:P:End Payable/Receivable Accrued',     'd'),
    ('end_dividend_accrued',     'P:P:End Dividend Accrued',               'd'),
    ('end_dividend_acc_base',    'P:P:End Dividend Accrued Base',          'd'),
    ('end_dividend_acc_fund',    'P:P:End Dividend Accrued Fund',          'd'),
    # Position NAV
    ('position_nav_base',        'P:P:Position NAV Base',                  'd'),
    ('position_nav_fund',        'P:P:Position NAV Fund',                  'd'),
    ('position_nav_local',       'P:P:Position NAV Local',                 'd'),
    # Exposure
    ('notional_exp_base',        'P:P:Notional Exposure Base',             'd'),
    ('notional_exp_fund',        'P:P:Notional Exposure Fund',             'd'),
    ('notional_exp_local',       'P:P:Notional Exposure Local',            'd'),
    ('exposure_pct_nav',         'P:P:Exposure % NAV',                     'd'),
    ('gross_exposure_pct_nav',   'P:P:Gross Exposure % NAV',               'd'),
    ('notional_gross_exp_base',  'P:P:Notional Gross Exposure Base',       'd'),
    ('notional_gross_exp_fund',  'P:P:Notional Gross Exposure Fund',       'd'),
    ('notional_gross_exp_local', 'P:P:Notional Gross Exposure Local',      'd'),
    ('notional_long_exp_base',   'P:P:Notional Long Exposure Base',        'd'),
    ('notional_long_exp_fund',   'P:P:Notional Long Exposure Fund',        'd'),
    ('notional_long_exp_local',  'P:P:Notional Long Exposure Local',       'd'),
    ('notional_short_exp_base',  'P:P:Notional Short Exposure Base',       'd'),
    ('notional_short_exp_fund',  'P:P:Notional Short Exposure Fund',       'd'),
    ('notional_short_exp_local', 'P:P:Notional Short Exposure Local',      'd'),
    # Greeks & risk
    ('beta',                     'P:P:Beta',                               'd'),
    ('gamma',                    'P:P:Gamma',                              'd'),
    ('delta',                    'P:P:Delta',                              'd'),
    ('futures_conv_risk',        'P:P:Futures Conventional Risk',          'd'),
    ('cr01_price',               'P:P:CR01 Price',                         'd'),
    ('dv01',                     'P:P:DV01',                               'd'),
    ('beta_adj_exp_base',        'P:P:Beta Adjusted Exposure Base',        'd'),
    ('beta_adj_exp_fund',        'P:P:Beta Adjusted Exposure Fund',        'd'),
    ('beta_adj_exp_local',       'P:P:Beta Adjusted Exposure Local',       'd'),
    ('beta_adj_gross_exp_fund',  'P:P:Beta Adjusted Gross Exposure Fund',  'd'),
    ('beta_adj_exp_pct_nav',     'P:P:Beta Adjusted Exposure % NAV',       'd'),
    ('beta_adj_gross_pct_nav',   'P:P:Beta Adjusted Gross Exposure % NAV', 'd'),
    ('cr01_exp_base',            'P:P:CR01 Exposure Base',                 'd'),
    ('cr01_exp_fund',            'P:P:CR01 Exposure Fund',                 'd'),
    ('cr01_exp_local',           'P:P:CR01 Exposure Local',                'd'),
    ('cr01_exp_pct_nav',         'P:P:CR01 Exposure % NAV',                'd'),
    ('delta_adj_exp_base',       'P:P:Delta Adjusted Exposure Base',       'd'),
    ('delta_adj_exp_fund',       'P:P:Delta Adjusted Exposure Fund',       'd'),
    ('delta_adj_exp_local',      'P:P:Delta Adjusted Exposure Local',      'd'),
    ('delta_adj_gross_exp_fund', 'P:P:Delta Adjusted Gross Exposure Fund', 'd'),
    ('delta_adj_exp_pct_nav',    'P:P:Delta Adjusted Exposure % NAV',      'd'),
    ('delta_adj_gross_pct_nav',  'P:P:Delta Adjusted Gross Exposure % NAV','d'),
    ('dv01_exp_base',            'P:P:DV01 Exposure Base',                 'd'),
    ('dv01_exp_fund',            'P:P:DV01 Exposure Fund',                 'd'),
    ('dv01_exp_local',           'P:P:DV01 Exposure Local',                'd'),
    ('dv01_exp_pct_nav',         'P:P:DV01 Exposure % NAV',                'd'),
    ('gamma_adj_exp_base',       'P:P:Gamma Adjusted Exposure Base',       'd'),
    ('gamma_adj_exp_fund',       'P:P:Gamma Adjusted Exposure Fund',       'd'),
    ('gamma_adj_exp_local',      'P:P:Gamma Adjusted Exposure Local',      'd'),
    ('gamma_adj_exp_pct_nav',    'P:P:Gamma Adjusted Exposure % NAV',      'd'),
    ('rho_exp_base',             'P:P:Rho Exposure Base',                  'd'),
    ('rho_exp_fund',             'P:P:Rho Exposure Fund',                  'd'),
    ('rho_exp_local',            'P:P:Rho Exposure Local',                 'd'),
    ('rho_exp_pct_nav',          'P:P:Rho Exposure % NAV',                 'd'),
    ('theta_exp_base',           'P:P:Theta Exposure Base',                'd'),
    ('theta_exp_fund',           'P:P:Theta Exposure Fund',                'd'),
    ('theta_exp_local',          'P:P:Theta Exposure Local',               'd'),
    ('theta_exp_pct_nav',        'P:P:Theta Exposure % NAV',               'd'),
    ('vega_exp_base',            'P:P:Vega Exposure Base',                 'd'),
    ('vega_exp_fund',            'P:P:Vega Exposure Fund',                 'd'),
    ('vega_exp_local',           'P:P:Vega Exposure Local',                'd'),
    ('vega_exp_pct_nav',         'P:P:Vega Exposure % NAV',                'd'),
    # DTD P&L
    ('dtd_pl',                   'P:P:DTD P/L',                            'd'),
    ('dtd_realized',             'P:P:DTD Realized',                       'd'),
    ('dtd_unrealized',           'P:P:DTD Unrealized',                     'd'),
    ('dtd_unrealized_price_gl',  'P:P:DTD Unrealized Price G/L',           'd'),
    ('dtd_unrealized_fx_gl',     'P:P:DTD Unrealized FX G/L',              'd'),
    ('dtd_carry',                'P:P:DTD Carry',                          'd'),
    ('dtd_carry_dividend',       'P:P:DTD Carry Dividend',                 'd'),
    ('dtd_carry_paid',           'P:P:DTD Carry Paid',                     'd'),
    ('dtd_carry_accrued',        'P:P:DTD Carry Accrued',                  'd'),
    ('dtd_trading_pl',           'P:P:DTD Trading P/L',                    'd'),
    ('dtd_pl_fx_gl',             'P:P:DTD P/L FX G/L',                     'd'),
    ('dtd_realized_fx_gl',       'P:P:DTD Realized FX G/L',                'd'),
    ('dtd_carry_fx_gl',          'P:P:DTD Carry FX G/L',                   'd'),
    ('dtd_trading_pl_fx_gl',     'P:P:DTD Trading P/L FX G/L',             'd'),
    ('dtd_pl_price_gl',          'P:P:DTD P/L Price G/L',                  'd'),
    ('dtd_realized_price_gl',    'P:P:DTD Realized Price G/L',             'd'),
    ('dtd_carry_price_gl',       'P:P:DTD Carry Price G/L',                'd'),
    ('dtd_trading_pl_price_gl',  'P:P:DTD Trading P/L Price G/L',          'd'),
    ('dtd_local_pl',             'P:P:DTD Local P/L',                      'd'),
    ('dtd_local_realized',       'P:P:DTD Local Realized',                 'd'),
    ('dtd_local_unrealized',     'P:P:DTD Local Unrealized',               'd'),
    ('dtd_local_carry',          'P:P:DTD Local Carry',                    'd'),
    ('dtd_local_carry_accrued',  'P:P:DTD Local Carry Accrued',            'd'),
    ('dtd_local_carry_dividend', 'P:P:DTD Local Carry Dividend',           'd'),
    ('dtd_local_carry_paid',     'P:P:DTD Local Carry Paid',               'd'),
    ('dtd_local_trading_pl',     'P:P:DTD Local Trading P/L',              'd'),
    ('fund_dtd_pl',              'P:P:Fund DTD P/L',                       'd'),
    ('fund_dtd_realized',        'P:P:Fund DTD Realized',                  'd'),
    ('fund_dtd_unrealized',      'P:P:Fund DTD Unrealized',                'd'),
    ('fund_dtd_carry',           'P:P:Fund DTD Carry',                     'd'),
    ('fund_dtd_pct',             'P:P:Fund DTD %',                         'd'),
    ('dtd_begin_position',       'P:P:DTD Begin Position',                 'd'),
    ('dtd_begin_price',          'P:P:DTD Begin Price',                    'd'),
    ('dtd_begin_mv_base',        'P:P:DTD Begin Market Value Base',        'd'),
    ('dtd_begin_mv_fund',        'P:P:DTD Begin Market Value Fund',        'd'),
    ('dtd_begin_mv_local',       'P:P:DTD Begin Market Value Local',       'd'),
    ('dtd_begin_fx',             'P:P:DTD Begin FX',                       'd'),
    ('dtd_begin_fund_fx',        'P:P:DTD Begin Fund FX',                  'd'),
    ('dtd_begin_fx_fund_ccy',    'P:P:DTD Begin FX in Fund Ccy',           'd'),
    ('dtd_begin_factor',         'P:P:DTD Begin Factor',                   'd'),
    ('dtd_begin_acc_base',       'P:P:DTD Begin Accrued Base',             'd'),
    ('dtd_begin_acc_fund',       'P:P:DTD Begin Accrued Fund',             'd'),
    ('dtd_begin_acc_local',      'P:P:DTD Begin Accrued Local',            'd'),
    # MTD P&L
    ('mtd_pl',                   'P:P:MTD P/L',                            'd'),
    ('mtd_realized',             'P:P:MTD Realized',                       'd'),
    ('mtd_unrealized',           'P:P:MTD Unrealized',                     'd'),
    ('mtd_unrealized_price_gl',  'P:P:MTD Unrealized Price G/L',           'd'),
    ('mtd_unrealized_fx_gl',     'P:P:MTD Unrealized FX G/L',              'd'),
    ('mtd_carry',                'P:P:MTD Carry',                          'd'),
    ('mtd_carry_dividend',       'P:P:MTD Carry Dividend',                 'd'),
    ('mtd_carry_paid',           'P:P:MTD Carry Paid',                     'd'),
    ('mtd_carry_accrued',        'P:P:MTD Carry Accrued',                  'd'),
    ('mtd_trading_pl',           'P:P:MTD Trading P/L',                    'd'),
    ('mtd_pl_fx_gl',             'P:P:MTD P/L FX G/L',                     'd'),
    ('mtd_realized_fx_gl',       'P:P:MTD Realized FX G/L',                'd'),
    ('mtd_carry_fx_gl',          'P:P:MTD Carry FX G/L',                   'd'),
    ('mtd_trading_pl_fx_gl',     'P:P:MTD Trading P/L FX G/L',             'd'),
    ('mtd_pl_price_gl',          'P:P:MTD P/L Price G/L',                  'd'),
    ('mtd_realized_price_gl',    'P:P:MTD Realized Price G/L',             'd'),
    ('mtd_carry_price_gl',       'P:P:MTD Carry Price G/L',                'd'),
    ('mtd_trading_pl_price_gl',  'P:P:MTD Trading P/L Price G/L',          'd'),
    ('mtd_local_pl',             'P:P:MTD Local P/L',                      'd'),
    ('mtd_local_realized',       'P:P:MTD Local Realized',                 'd'),
    ('mtd_local_unrealized',     'P:P:MTD Local Unrealized',               'd'),
    ('mtd_local_carry',          'P:P:MTD Local Carry',                    'd'),
    ('mtd_local_carry_accrued',  'P:P:MTD Local Carry Accrued',            'd'),
    ('mtd_local_carry_dividend', 'P:P:MTD Local Carry Dividend',           'd'),
    ('mtd_local_carry_paid',     'P:P:MTD Local Carry Paid',               'd'),
    ('mtd_local_trading_pl',     'P:P:MTD Local Trading P/L',              'd'),
    ('fund_mtd_pl',              'P:P:Fund MTD P/L',                       'd'),
    ('fund_mtd_realized',        'P:P:Fund MTD Realized',                  'd'),
    ('fund_mtd_unrealized',      'P:P:Fund MTD Unrealized',                'd'),
    ('fund_mtd_carry',           'P:P:Fund MTD Carry',                     'd'),
    ('fund_mtd_pct',             'P:P:Fund MTD %',                         'd'),
    ('mtd_begin_position',       'P:P:MTD Begin Position',                 'd'),
    ('mtd_begin_price',          'P:P:MTD Begin Price',                    'd'),
    ('mtd_begin_mv_base',        'P:P:MTD Begin Market Value Base',        'd'),
    ('mtd_begin_mv_fund',        'P:P:MTD Begin Market Value Fund',        'd'),
    ('mtd_begin_mv_local',       'P:P:MTD Begin Market Value Local',       'd'),
    ('mtd_begin_fx',             'P:P:MTD Begin FX',                       'd'),
    ('mtd_begin_fund_fx',        'P:P:MTD Begin Fund FX',                  'd'),
    ('mtd_begin_fx_fund_ccy',    'P:P:MTD Begin FX in Fund Ccy',           'd'),
    ('mtd_begin_factor',         'P:P:MTD Begin Factor',                   'd'),
    ('mtd_begin_acc_base',       'P:P:MTD Begin Accrued Base',             'd'),
    ('mtd_begin_acc_fund',       'P:P:MTD Begin Accrued Fund',             'd'),
    ('mtd_begin_acc_local',      'P:P:MTD Begin Accrued Local',            'd'),
    # YTD P&L
    ('ytd_pl',                   'P:P:YTD P/L',                            'd'),
    ('ytd_realized',             'P:P:YTD Realized',                       'd'),
    ('ytd_unrealized',           'P:P:YTD Unrealized',                     'd'),
    ('ytd_unrealized_price_gl',  'P:P:YTD Unrealized Price G/L',           'd'),
    ('ytd_unrealized_fx_gl',     'P:P:YTD Unrealized FX G/L',              'd'),
    ('ytd_carry',                'P:P:YTD Carry',                          'd'),
    ('ytd_carry_dividend',       'P:P:YTD Carry Dividend',                 'd'),
    ('ytd_carry_paid',           'P:P:YTD Carry Paid',                     'd'),
    ('ytd_carry_accrued',        'P:P:YTD Carry Accrued',                  'd'),
    ('ytd_trading_pl',           'P:P:YTD Trading P/L',                    'd'),
    ('ytd_pl_fx_gl',             'P:P:YTD P/L FX G/L',                     'd'),
    ('ytd_realized_fx_gl',       'P:P:YTD Realized FX G/L',                'd'),
    ('ytd_carry_fx_gl',          'P:P:YTD Carry FX G/L',                   'd'),
    ('ytd_trading_pl_fx_gl',     'P:P:YTD Trading P/L FX G/L',             'd'),
    ('ytd_pl_price_gl',          'P:P:YTD P/L Price G/L',                  'd'),
    ('ytd_realized_price_gl',    'P:P:YTD Realized Price G/L',             'd'),
    ('ytd_carry_price_gl',       'P:P:YTD Carry Price G/L',                'd'),
    ('ytd_trading_pl_price_gl',  'P:P:YTD Trading P/L Price G/L',          'd'),
    ('ytd_local_pl',             'P:P:YTD Local P/L',                      'd'),
    ('ytd_local_realized',       'P:P:YTD Local Realized',                 'd'),
    ('ytd_local_unrealized',     'P:P:YTD Local Unrealized',               'd'),
    ('ytd_local_carry',          'P:P:YTD Local Carry',                    'd'),
    ('ytd_local_carry_accrued',  'P:P:YTD Local Carry Accrued',            'd'),
    ('ytd_local_carry_dividend', 'P:P:YTD Local Carry Dividend',           'd'),
    ('ytd_local_carry_paid',     'P:P:YTD Local Carry Paid',               'd'),
    ('ytd_local_trading_pl',     'P:P:YTD Local Trading P/L',              'd'),
    ('fund_ytd_pl',              'P:P:Fund YTD P/L',                       'd'),
    ('fund_ytd_realized',        'P:P:Fund YTD Realized',                  'd'),
    ('fund_ytd_unrealized',      'P:P:Fund YTD Unrealized',                'd'),
    ('fund_ytd_carry',           'P:P:Fund YTD Carry',                     'd'),
    ('fund_ytd_pct',             'P:P:Fund YTD %',                         'd'),
    ('ytd_begin_position',       'P:P:YTD Begin Position',                 'd'),
    ('ytd_begin_price',          'P:P:YTD Begin Price',                    'd'),
    ('ytd_begin_mv_base',        'P:P:YTD Begin Market Value Base',        'd'),
    ('ytd_begin_mv_fund',        'P:P:YTD Begin Market Value Fund',        'd'),
    ('ytd_begin_mv_local',       'P:P:YTD Begin Market Value Local',       'd'),
    ('ytd_begin_fx',             'P:P:YTD Begin FX',                       'd'),
    ('ytd_begin_fund_fx',        'P:P:YTD Begin Fund FX',                  'd'),
    ('ytd_begin_fx_fund_ccy',    'P:P:YTD Begin FX in Fund Ccy',           'd'),
    ('ytd_begin_factor',         'P:P:YTD Begin Factor',                   'd'),
    ('ytd_begin_acc_base',       'P:P:YTD Begin Accrued Base',             'd'),
    ('ytd_begin_acc_fund',       'P:P:YTD Begin Accrued Fund',             'd'),
    ('ytd_begin_acc_local',      'P:P:YTD Begin Accrued Local',            'd'),
]

# Build DDL dynamically from COLUMNS so schema always matches insert
TYPE_MAP = {'s': 'VARCHAR(300)', 'd': 'DECIMAL(24,6)', 'dt': 'DATE'}

def _build_ddl():
    col_defs = '\n'.join(
        f'    {db_col:<32} {TYPE_MAP[t]},'
        for db_col, _, t in COLUMNS
    )
    return f"""
CREATE TABLE IF NOT EXISTS positions (
    id        SERIAL PRIMARY KEY,
    source    VARCHAR(20)  DEFAULT 'broadridge',
    loaded_at TIMESTAMP    DEFAULT NOW(),
{col_defs.rstrip(',')}
);"""

INDEXES = [
    'CREATE INDEX IF NOT EXISTS idx_pos_date     ON positions (position_date);',
    'CREATE INDEX IF NOT EXISTS idx_pos_symbol   ON positions (security_symbol);',
    'CREATE INDEX IF NOT EXISTS idx_pos_fund     ON positions (fund_code);',
    'CREATE INDEX IF NOT EXISTS idx_pos_strategy ON positions (strategy_name);',
    'CREATE INDEX IF NOT EXISTS idx_pos_asset    ON positions (asset_class);',
    'CREATE INDEX IF NOT EXISTS idx_pos_isin     ON positions (security_isin);',
    'CREATE INDEX IF NOT EXISTS idx_pos_cusip    ON positions (security_cusip);',
    'CREATE INDEX IF NOT EXISTS idx_pos_ls       ON positions (ls_flag);',
]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def handler(event, context):
    s3 = boto3.client('s3')
    db_host     = os.environ['DB_HOST']
    db_name     = os.environ['DB_NAME']
    db_user     = os.environ['DB_USER']
    db_password = os.environ['DB_PASSWORD']

    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key    = record['s3']['object']['key']
        print(f"Processing: s3://{bucket}/{key}")

        response    = s3.get_object(Bucket=bucket, Key=key)
        csv_content = response['Body'].read().decode('utf-8-sig')  # handles BOM

        conn = psycopg2.connect(
            host=db_host, database=db_name,
            user=db_user, password=db_password,
            port=5432, connect_timeout=15
        )
        cursor = conn.cursor()

        try:
            rows = process_csv(cursor, csv_content)
            conn.commit()
            print(f"Committed {rows} rows from {key}")

            dest = key.replace('incoming/', 'processed/')
            s3.copy_object(Bucket=bucket,
                           CopySource={'Bucket': bucket, 'Key': key},
                           Key=dest)
            s3.delete_object(Bucket=bucket, Key=key)

        except Exception as exc:
            conn.rollback()
            dest = key.replace('incoming/', 'failed/')
            s3.copy_object(Bucket=bucket,
                           CopySource={'Bucket': bucket, 'Key': key},
                           Key=dest)
            s3.delete_object(Bucket=bucket, Key=key)
            print(f"ERROR: {exc}")
            raise
        finally:
            cursor.close()
            conn.close()

    return {'statusCode': 200, 'body': 'Done'}


# ---------------------------------------------------------------------------
# CSV processing
# ---------------------------------------------------------------------------
def process_csv(cursor, csv_content):
    cursor.execute(_build_ddl())
    for idx_sql in INDEXES:
        cursor.execute(idx_sql)

    db_cols   = [c[0] for c in COLUMNS]
    csv_cols  = [c[1] for c in COLUMNS]
    col_types = [c[2] for c in COLUMNS]

    placeholders = ', '.join(['%s'] * len(COLUMNS))
    insert_cols  = ', '.join(db_cols)
    INSERT_SQL   = (
        f"INSERT INTO positions ({insert_cols}) "
        f"VALUES ({placeholders})"
    )

    reader = csv.DictReader(StringIO(csv_content))
    count  = 0

    for row in reader:
        if not any(v.strip() for v in row.values() if v):
            continue  # skip blank trailing rows

        values = []
        for csv_col, col_type in zip(csv_cols, col_types):
            raw = row.get(csv_col, '')
            if col_type == 'd':
                values.append(_to_decimal(raw))
            elif col_type == 'dt':
                values.append(_to_date(raw))
            else:
                values.append(str(raw).strip() if raw else None)

        cursor.execute(INSERT_SQL, values)
        count += 1

    return count


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _to_decimal(value):
    try:
        cleaned = str(value).replace(',', '').replace('$', '').strip()
        return float(cleaned) if cleaned else None
    except Exception:
        return None


def _to_date(value):
    if not value or not str(value).strip():
        return None
    v = str(value).strip()
    for fmt in ('%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%d-%m-%Y', '%Y%m%d'):
        try:
            return datetime.strptime(v, fmt).date()
        except ValueError:
            continue
    return None
