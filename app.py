from src.Climate_Data_ETL.components.data_extractor import DataScrapper
from src.Climate_Data_ETL.components.data_transfromation import DataTranformation


obj = DataScrapper()
trans_obj = DataTranformation()


year = 2010
month = 1

# scrape html
obj.retrive_html(year, month)

# convert html to pdf
trans_obj.covert_html_to_pdf(year, month)

# convert pdf to csv
trans_obj.covert_pdf_to_csv(year, month)
