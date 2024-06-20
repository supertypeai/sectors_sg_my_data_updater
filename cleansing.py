import numpy as np
import pandas as pd

def clean_daily_foreign_data(foreign_daily_data):
    """
    SGX/KLSE Daily Data Fetching Cleansing.

    Parameters:
    - foreign_daily_data: dataframe, KLSE/SGX daily data from investing.com api hit

    Returns:
    - foreign_daily_data: dataframe, Cleaned KLSE/SGX daily data
    """

    # Replace '-' data with ''
    foreign_daily_data = foreign_daily_data.replace('-',np.nan)

    # Remove percentage and change data to decimal
    for i in ["daily",'weekly','monthly','ytd','one_year','three_year']:
        foreign_daily_data[f"{i}_percentage_change"] = foreign_daily_data[f"{i}_percentage_change"]/100 

    # Rename columns
    foreign_daily_data.rename(columns={"daily_percentage_change":"change_1d", "weekly_percentage_change":'change_7d', 
                        "monthly_percentage_change":"change_1m", 
                        "ytd_percentage_change":"change_ytd",
                        "one_year_percentage_change":"change_1y",
                        "three_year_percentage_change":"change_3y"}, inplace=True)

    # Delete redundant percentage change columns
    foreign_daily_data.drop("percentage_change",axis=1, inplace = True)

    # Change data type to float
    float_columns = ['close', 'market_cap', 'volume','pe', 'revenue', 'beta','change_1d',
       'change_7d', 'change_1m', 'change_ytd', 'change_1y', 'change_3y',]

    foreign_daily_data[float_columns] = foreign_daily_data[float_columns].applymap(lambda x:float(str(x).replace(',', '')))

    return foreign_daily_data

def clean_periodic_foreign_data(foreign_periodic_data, foreign_sectors):
    """
    SGX/KLSE Periodic Data Fetching Cleansing.

    Parameters:
    - foreign_periodic_data: dataframe, periodic data from investing.com data scraping using request
    - foreign_sectors: dataframe, KLSE/SGX sectors mapping to IDX sectors

    Returns:
    - foreign_periodic_data: dataframe, Cleaned KLSE/SGX periodic data
    """

    # Replace '-' data with ''
    foreign_periodic_data = foreign_periodic_data.replace('-',np.nan)

    foreign_periodic_data['dividend_yield'] = foreign_periodic_data['dividend_yield'].apply(lambda x: float(x.strip('%')) / 100 if pd.notnull(x) else np.nan)

    for i in ["gross_margin","operating_margin",'net_profit_margin',"debt_to_equity","five_year_dividend_average",'dividend_growth_rate',"payout_ratio","five_year_eps_growth","five_year_sales_growth","five_year_capital_spending_growth"]:
        foreign_periodic_data[i] = foreign_periodic_data[i].apply(lambda x: float(x.replace('%', '').replace(',', '')) / 100 if pd.notnull(x) else np.nan)

    foreign_periodic_data.rename(columns={"five_year_dividend_average":"dividend_yield_5y_avg"}, inplace=True) 
    
    float_columns = ['eps', 'dividend', 'dividend_yield', 'pe_ttm', 'ps_ttm', 'pcf', 'pcf_ttm', 'pb', 'five_year_eps_growth',
       'five_year_sales_growth', 'five_year_capital_spending_growth',
       'asset_turnover', 'inventory turnover (ttm)', 'receivable_turnover',
       'gross_margin', 'operating_margin', 'net_profit_margin', 'quick_ratio',
       'current_ratio', 'debt_to_equity', 'dividend_yield_5y_avg',
       'dividend_growth_rate', 'payout_ratio']

    foreign_periodic_data[float_columns] = foreign_periodic_data[float_columns].applymap(lambda x:float(str(x).replace(',', '')))

    foreign_periodic_data = foreign_periodic_data.merge(foreign_sectors, on = ["sector",'industry']).drop(["sector",'industry'], axis=1).rename(columns={"sectors_id":"sector","sub_sector_id":"sub_sector"})

    return foreign_periodic_data