from sector_scraper_klse import scrap_function_my
from sector_scraper_sgx import scrap_function_sg
from sector_combiner import combine_data
from additional_scrapper_klse import scrap_null_data_my
from additional_scrapper_sgx import scrap_null_data_sg
import pandas as pd
from multiprocessing import Process
import os
import time
from dotenv import load_dotenv
from supabase import create_client
import sys

PARAMS_DICT = {
  "SG": {
    "type_id" : "sgx",
    "db_table" : "sgx_companies",
    "symbol_column" : "symbol",
    "scraping_function" : scrap_function_sg,
    "null_handling_function" : scrap_null_data_sg,
  },
  "MY": {
    "type_id" : "klse",
    "db_table" : "klse_companies",
    "symbol_column" : "investing_symbol",
    "scraping_function" : scrap_function_my,
    "null_handling_function" : scrap_null_data_my
  }
}

if __name__ == "__main__":
  load_dotenv()

  # Connection to Supabase
  url_supabase = os.getenv("SUPABASE_URL")
  key = os.getenv("SUPABASE_KEY")
  supabase = create_client(url_supabase, key)

  # Check the running argument
  if (sys.argv[1] is not None and sys.argv[1].upper() in PARAMS_DICT):

    USED_DICT = PARAMS_DICT[sys.argv[1].upper()]
    DB_TABLE = USED_DICT["db_table"]
    SYMBOL_COLUMN = USED_DICT['symbol_column']
    TYPE_ID = USED_DICT['type_id']
    SCRAPING_FUNCTION = USED_DICT["scraping_function"]
    NULL_HANDLING_FUNCTION = USED_DICT["null_handling_function"]

    # Get the table
    db_data = supabase.table(DB_TABLE).select("").execute()
    df_db_data = pd.DataFrame(db_data.data)

    cols = df_db_data.columns.tolist()

    # Get symbol data
    symbol_list = df_db_data[SYMBOL_COLUMN].tolist()
    print(f"[DATA LENGTH] Got {len(symbol_list)} data to be processed!")

    start = time.time()

    # Divide to processes
    length_list = len(symbol_list)
    i1 = int(length_list / 4)
    i2 = 2 * i1
    i3 = 3 * i1

    p1 = Process(target=SCRAPING_FUNCTION, args=(symbol_list[:i1], 1))
    p2 = Process(target=SCRAPING_FUNCTION, args=(symbol_list[i1:i2], 2))
    p3 = Process(target=SCRAPING_FUNCTION, args=(symbol_list[i2:i3], 3))
    p4 = Process(target=SCRAPING_FUNCTION, args=(symbol_list[i3:], 4))

    p1.start()
    p2.start()
    p3.start()
    p4.start()

    p1.join()
    p2.join()
    p3.join()
    p4.join()

    # Handle null data
    NULL_HANDLING_FUNCTION()

    # Merge data
    df_final = combine_data(df_db_data, TYPE_ID, SYMBOL_COLUMN)

    # Convert to json. Remove the index in dataframe
    records = df_final.to_dict(orient="records")

    # Upsert to db
    try:
      supabase.table(DB_TABLE).upsert(
          records
      ).execute()
      print(
          f"Successfully upserted {len(records)} data to database"
      )
    except Exception as e:
      raise Exception(f"Error upserting to database: {e}")

    # End time
    end = time.time()
    duration = int(end-start)
    print(f"The execution time: {time.strftime('%H:%M:%S', time.gmtime(duration))}")

  else:
    print("[ERROR] Running argument is not complete")