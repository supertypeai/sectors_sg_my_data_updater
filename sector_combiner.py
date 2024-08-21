import json
import os
import pandas as pd
import numpy as np


def combine_data(df_db_data : pd.DataFrame, type : str, symbol_column: str):
  cwd = os.getcwd()
  data_dir = os.path.join(cwd, "data")
  data_file_path = [os.path.join(data_dir,f'P{i}_data_{type}.json') for i in range(1,5)]

  # Combine data
  all_data_list = list()
  for file_path in data_file_path:
    f = open(file_path)
    data = json.load(f)
    all_data_list = all_data_list + data

  # Make Dataframe
  df_scraped = pd.DataFrame(all_data_list)
  
  # Sort df_db_data and df_scraped
  df_db_data = df_db_data.sort_values([symbol_column])
  df_scraped = df_scraped.sort_values([symbol_column])

  indices_list = df_db_data.index.tolist()

  
  # Save to JSON and CSV
  cwd = os.getcwd()
  data_dir = os.path.join(cwd, "data")
  df_scraped.to_json(os.path.join(data_dir, f"final_data_{type}.json"), orient="records", indent=2)
  # df_scraped.to_csv(os.path.join(data_dir, f"final_data_{type}.csv"), index=False) # Only activated if needed

  # Reset index
  df_db_data = df_db_data.reset_index(drop= True)
  df_scraped = df_scraped.reset_index(drop= True)

  # Merge the dataframe to the one in the db
  df_db_data.update(df_scraped)

  df_db_data.index = indices_list

  # Replace mp.nan to None
  df_merge = df_db_data.replace({np.nan: None})
  
  return df_merge