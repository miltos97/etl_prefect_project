
import pandas as pd
from etl.utils import setup_logger
logger = setup_logger()


def extract_data():
    logger.info("Extracting data from Excel files...")
    transactions = pd.read_excel(r'C:\Users\symeom\OneDrive - Pfizer\Desktop\SrAssociate_Data_Engineer\etl_prefect_project\data\Transaction_Data.xlsx')
    customers = pd.read_excel(r'C:\Users\symeom\OneDrive - Pfizer\Desktop\SrAssociate_Data_Engineer\etl_prefect_project\data\Customer_Data.xlsx')
    products = pd.read_excel(r'C:\Users\symeom\OneDrive - Pfizer\Desktop\SrAssociate_Data_Engineer\etl_prefect_project\data\Products_Data.xlsx')
    logger.info("Data extracted successfully.")
    #return transactions, customers, products
    return {
        "transactions": transactions,
        "customers": customers,
        "products": products
    }



