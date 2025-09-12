
import pandas as pd
from etl.utils import setup_logger
import os
from datetime import datetime
logger = setup_logger()

# Path to store the last processed timestamp
STATE_FILE = r'C:\Users\symeom\OneDrive - Pfizer\Desktop\SrAssociate_Data_Engineer\etl_prefect_project\data\last_transaction_time.txt'

def get_last_transaction_time():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return pd.to_datetime(f.read().strip())
    return pd.to_datetime("1900-01-01")

def update_last_transaction_time(latest_time):
    with open(STATE_FILE, 'w') as f:
        f.write(latest_time.strftime('%Y-%m-%d %H:%M:%S'))

def extract_data():
    logger.info("Extracting data from Excel files...")

    # Load full datasets
    customers = pd.read_excel(r'C:\Users\symeom\OneDrive - Pfizer\Desktop\SrAssociate_Data_Engineer\etl_prefect_project\data\Customer_Data.xlsx')
    products = pd.read_excel(r'C:\Users\symeom\OneDrive - Pfizer\Desktop\SrAssociate_Data_Engineer\etl_prefect_project\data\Products_Data.xlsx')

    # Load transactions and filter by timestamp
    transactions_full = pd.read_excel(r'C:\Users\symeom\OneDrive - Pfizer\Desktop\SrAssociate_Data_Engineer\etl_prefect_project\data\Transaction_Data.xlsx')
    transactions_full['TransactionTime'] = pd.to_datetime(transactions_full['TransactionTime'])

    last_time = get_last_transaction_time()
    new_transactions = transactions_full[transactions_full['TransactionTime'] > last_time]

    logger.info(f"Loaded {len(new_transactions)} new transaction rows since {last_time}.")

    if not new_transactions.empty:
        latest_time = new_transactions['TransactionTime'].max()
        update_last_transaction_time(latest_time)

    return {
        "transactions": new_transactions,
        "customers": customers,
        "products": products
    }

