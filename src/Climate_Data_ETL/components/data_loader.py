from src.Climate_Data_ETL.logger import logging
from src.Climate_Data_ETL.exception import customexception
from sklearn.model_selection import train_test_split
from dataclasses import dataclass
from mysql.connector import Error
from dotenv import load_dotenv
from pathlib import Path
import mysql.connector
from tqdm import tqdm
import pandas as pd
import os
import sys

# calling all env variable
load_dotenv()

class DataLoaderConfig:
    csv_data_path: str = os.path.join("artifacts", "raw.csv")
    host:str = os.getenv('HOST')
    user:str = os.getenv('USER')
    password:str = os.getenv('PASSWORD')
    database:str = os.getenv('DATABASE')
    table_name:str = os.getenv('TABLE_NAME')


class DataLoader:

    def __init__(self):
        self.data_loader_config = DataLoaderConfig()

    def create_connection(self):
        logging.info("[-] Establishing connection with databse...")

        connection = None
        try:
            connection = mysql.connector.connect(
                host=self.data_loader_config.host,
                user=self.data_loader_config.user,
                password=self.data_loader_config.password,
                database=self.data_loader_config.database
            )
            if connection.is_connected():
                print("Connection to MySQL DB successful")
                logging.info("[*] Established connection successfully!")

            return connection
        
        except Error as e:
            print(f"[!] Exception occured during creating connection with databse stage: {e}")
            raise customexception(e, sys)

    def initiate_data_dataloading(self, year: int, month: int):
        logging.info("[-] Data Loading Started...")

        try:
            # conneting to sql databse
            connection = self.create_connection()

            # initalizing cursor
            cursor = connection.cursor()

            # create table if not exists
            cursor.execute(f"""
                            CREATE TABLE IF NOT EXISTS {self.data_loader_config.table_name} (
                                Day INT PRINERY KEY NOT NULL,
                                Month INT, 
                                Year INT,
                                T REAL,
                                TM REAL,
                                Tm REAL,
                                SLP REAL,
                                H REAL,
                                PP REAL,
                                VV REAL,
                                V REAL,
                                VM REAL,
                                VG REAL,
                                RA REAL,
                                SN REAL,
                                TS REAL,
                                FG REAL
                        );
                        """)

            # Commit the changes
            connection.commit()

            # read data csv file
            data = pd.read_csv(Path(self.data_loader_config.csv_data_path))

            # for loop to insert all data points of csv to database
            for index, row in tqdm(data.iterrows(), total=data.shape[0], desc="Inserting Rows"):
                cursor.execute(f'''
                    INSERT INTO {self.data_loader_config.table_name} (Day, Month, Year, T, TM, Tm, SLP, H, PP, VV, V, VM, VG, RA, SN, TS, FG) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                ''', (row['Day'], row['Month'], row['Year'], row['T'], row['TM'], row['Tm'], row['SLP'], row['H'], row['PP'], row['VV'], row['V'], row['VM'], row['VG'], row['RA'], row['SN'], row['TS'], row['FG']))  # Adjust column names based on your CSV

            # Commit the changes and close the connection
            connection.commit()
            connection.close()

            logging.info(f"[*] Data Loading Completed for {month}-{year} !")

        except Exception as e:
            logging.info(f"[!] Exception occured during Data Loading stage : {e}")
            raise customexception(e, sys)
        
        finally:
            connection.close()