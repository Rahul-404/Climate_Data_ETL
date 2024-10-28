import os
import sys
import requests
from pathlib import Path
from dataclasses import dataclass
from src.Climate_Data_ETL.logger import logging
from src.Climate_Data_ETL.exception import customexception

@dataclass
class DataScrapeConfig:
    html_data_path:str = "artifacts"


class DataScrapper:

    def __init__(self):
        self.scrape_config = DataScrapeConfig()

    def retrive_html(self, year:int, month:int):
        """ 
        function scrapes data for given month and year
        """
        try:
            logging.info(f"[-] Scrapping Initalized for {month}-{year}...")

            # get appropriate url
            if (month < 10):
                url = "https://en.tutiempo.net/climate/0{}-{}/ws-432950.html".format(month, year)
            else:
                url = "https://en.tutiempo.net/climate/{}-{}/ws-432950.html".format(month, year)

            # request page for url
            texts = requests.get(url,)
            # encode data to a format
            text_utf= texts.text.encode('utf-8')

            # store the file in folder
            os.makedirs(self.scrape_config.html_data_path, exist_ok=True)

            with open(os.path.join(self.scrape_config.html_data_path, f"{year}_{month}.html"), "wb") as output:
                output.write(text_utf)

            logging.info(f"[*] Scrapping Completed for {month}-{year}!")

            sys.stdout.flush()

        except Exception as e:
            logging.info(f"[!] Exception occured while scrapping the data for month:{month} and year: {year}")
            raise customexception(e, sys)