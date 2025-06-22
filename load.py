from utils.db_conn import db_connection
from utils.read_sql import read_sql_file
from sqlalchemy.orm import sessionmaker
import sqlalchemy
import pandas as pd
import os
import warnings
from dotenv import load_dotenv
warnings.filterwarnings('ignore')

load_dotenv()

def load():

    try:
        
        # Define db connection engine
        _, dwh_engine = db_connection()
        
        # Define DIR
        DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")

        #-------------------------------Load data------------------------
        tables = [
                    ('aircrafts', 'public.aircrafts.csv'),
                    ('airlines', 'public.airlines.csv'),
                    ('customers', 'public.customers.csv'),
                    ('airports', 'public.airports.csv'),
                    ('hotel', 'public.hotel.csv'),
                    ('flight_bookings', 'public.flight_bookings.csv'),
                    ('hotel_bookings', 'public.hotel_bookings.csv')
                ]
        
        # Load to public schema
        for table_name, file_name in tables:
            data = pd.read_csv(f'{DIR_TEMP_DATA}/{file_name}')
            data.to_sql(table_name, con=dwh_engine, if_exists='append', index=False, schema='public')
        
    except Exception as e:
        print(f"Error loading data: {e}")
        
# Execute the functions when the script is run
if __name__ == "__main__":
    load()