# html to pdf -> pdf to csv
import os
import sys
import pdfkit
import calendar
import pandas as pd
from tabula import read_pdf
from dataclasses import dataclass
from src.Climate_Data_ETL.logger import logging
from src.Climate_Data_ETL.exception import customexception

@dataclass
class DataTransformationConfig:
    html_data_path:str = "artifacts"
    pdf_data_path:str = os.path.join("artifacts", "raw.pdf")
    csv_data_path:str = os.path.join("artifacts", "raw.csv")
    options = {
                'page-size': 'A3',
            }
    columns = ['Day','T','TM','Tm','SLP','H','PP','VV','V','VM','VG','RA','SN','TS','FG']


class DataTranformation:
    
    def __init__(self):
        self.data_transform_config = DataTransformationConfig()

    def days_in_month(self, month: int, year: int) -> int:
        """Return the number of days in a given month and year."""
        try:
            # if month < 1 or month > 12:
                # raise ValueError("Month must be between 1 and 12.")

            return calendar.monthrange(year, month)[1]
        
        except Exception as e:
            logging.info(f"[!] Month must be between 1 and 12.")
            raise customexception(e, sys)

    def covert_html_to_pdf(self, year: int, month: int):
        """ 
        reads html file for given year and month then converts it and saves it as pdf
        """
        logging.info(f"[-] HTML to PDF Conversion Initalized for {month}-{year} ...")
        try:
            # check if directory exists
            os.makedirs(os.path.dirname(self.data_transform_config.pdf_data_path), exist_ok=True)

            # coverting html to pdf
            pdfkit.from_file(os.path.join(self.data_transform_config.html_data_path, f"{year}_{month}.html"), 
                             self.data_transform_config.pdf_data_path, 
                             options=self.data_transform_config.options)
            
            logging.info(f"[*] HTML to PDF Conversion Completed for {month}-{year} !")

        except Exception as e:
            logging.info(f"[!] Exception occured while html to pdf conversion for month:{month} and year: {year}")
            raise customexception(e, sys)
        
    def covert_pdf_to_csv(self, year: int, month: int):
        """ 
        reads html file for given year and month then converts it and saves it as pdf
        """
        logging.info(f"[-] PDF to CSV Conversion Initalized for {month}-{year} ...")
        try:
            # check if directory exists
            os.makedirs(os.path.dirname(self.data_transform_config.csv_data_path), exist_ok=True)

            # coverting pdf to csv
            # read pdf file
            with open(self.data_transform_config.pdf_data_path, 'rb') as pdf_file:
                df = read_pdf(pdf_file, pages=1)

            # get number of days in month
            DAYS_IN_MONTH = self.days_in_month(month, year)

            # get data table only
            data = df[-1].iloc[df[-1].shape[0]-1-DAYS_IN_MONTH:-2]
            data.columns = self.data_transform_config.columns

            print("\n Not got CSV")

            data.to_csv(self.data_transform_config.csv_data_path, index=False)  

            print("\n Got csv")

            logging.info(f"[*] PDF to CSV Conversion Completed for {month}-{year} !")

        except Exception as e:
            logging.info(f"[!] Exception occured while pdf to csv conversion for month:{month} and year: {year}")
            raise customexception(e, sys)
        
        