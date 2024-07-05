import json
import os
import pandas as pd
import numpy as np


def combine_data (df_db_data):
  cwd = os.getcwd()
  data_dir = os.path.join(cwd, "data")
  data_file_path = [os.path.join(data_dir,f'P{i}_data.json') for i in range(1,5)]

  # Combine data
  all_data_list = list()
  for file_path in data_file_path:
    f = open(file_path)
    data = json.load(f)
    all_data_list = all_data_list + data

  # Make Dataframe
  df_scraped = pd.DataFrame(all_data_list)

  # Drop Column
  df_scraped = df_scraped.drop(['sector', 'name'], axis=1)

  # Rename columns
  df_scraped = df_scraped.rename(columns={"stock_code": "symbol", "industry": "sector", "sub_industry": "sub_sector"})
  
  # Sort df_db_data and df_scraped
  df_db_data = df_db_data.sort_values(['symbol'])
  df_scraped = df_scraped.sort_values(['symbol'])

  indices_list = df_db_data.index.tolist()

  # Reset index
  df_db_data = df_db_data.reset_index(drop= True)
  df_scraped = df_scraped.reset_index(drop= True)

  # Merge the dataframe to the one in the db
  df_db_data.update(df_scraped)

  df_db_data.index = indices_list

  # Replace mp.nan to None
  df_merge = df_db_data.replace({np.nan: None})
  
  return df_merge