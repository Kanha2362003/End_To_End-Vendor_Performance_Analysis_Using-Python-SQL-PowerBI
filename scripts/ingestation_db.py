import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# ✅ Ensure logs folder exists
os.makedirs('logs', exist_ok=True)

logging.basicConfig(
    filename=r"logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine('sqlite:///inventory.db')  # ✅ 3 slashes → relative to cwd

def ingest_db(df, table_name, engine):
    '''This function will ingest the dataframe into database table'''
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def load_raw_data():
    '''This function will load the CSVs as dataframe and ingest into db'''
    start = time.time()
    
    data = r'C:\Users\kanha\Vendor Analysis\data'  # ✅ make sure this exists

    for file in os.listdir(data):
        if file.endswith('.csv'):
            full_path = os.path.join(data, file)
            chunksize = 100000  # adjust to your memory

            for chunk in pd.read_csv(full_path, chunksize=chunksize):
                logging.info(f'Ingesting {file} in db')
                ingest_db(chunk, file[:-4], engine)
    
    end = time.time()
    total_time = (end - start) / 60 
    logging.info('-----Ingestion Complete------')       
    logging.info(f'Total Time Taken: {total_time:.2f} minutes')

if __name__ == '__main__':
    load_raw_data()
