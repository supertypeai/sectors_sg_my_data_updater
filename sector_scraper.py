from scraper import scrap_function
from combiner import combine_data
import pandas as pd
from multiprocessing import Process
import os
import time
from dotenv import load_dotenv
from supabase import create_client

if __name__ == "__main__":

  load_dotenv()

  # Connection to Supabase
  url_supabase = os.getenv("SUPABASE_URL")
  key = os.getenv("SUPABASE_KEY")
  supabase = create_client(url_supabase, key)

  # Get the table
  db_data = supabase.table("sgx_companies").select("").execute()
  df_db_data = pd.DataFrame(db_data.data)

  cols = df_db_data.columns.tolist()

  # Get symbol data
  symbol_list = df_db_data['symbol'].tolist()

  start = time.time()

  # Divide to processes
  length_list = len(symbol_list)
  i1 = int(length_list / 4)
  i2 = 2 * i1
  i3 = 3 * i1

  p1 = Process(target=scrap_function, args=(symbol_list[:i1], 1))
  p2 = Process(target=scrap_function, args=(symbol_list[i1:i2], 2))
  p3 = Process(target=scrap_function, args=(symbol_list[i2:i3], 3))
  p4 = Process(target=scrap_function, args=(symbol_list[i3:], 4))

  p1.start()
  p2.start()
  p3.start()
  p4.start()

  p1.join()
  p2.join()
  p3.join()
  p4.join()

  # Merge data
  df_final = combine_data(df_db_data)

  # # Save to JSON and CSV
  cwd = os.getcwd()
  data_dir = os.path.join(cwd, "data")
  
  df_final.to_json(os.path.join(data_dir, "final_data.json"), orient="records", indent=2)
  df_final.to_csv(os.path.join(data_dir, "final_data.csv"), index=False)

  # Convert to json. Remove the index in dataframe
  records = df_final.to_dict(orient="records")

  # Upsert to db
  try:
    supabase.table("sgx_companies").upsert(
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
