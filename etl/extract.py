
import pandas as pd
from etl.utils import setup_logger
import os
import yaml
from datetime import datetime
logger = setup_logger()


with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

try:
    STATE_FILE = config["state_file"]   # Path to store the last processed timestamp
    CUSTOMER_FILE = config["customer_data"]
    PRODUCTS_FILE = config["products_data"]
    TRANSACTIONS_FILE = config["transaction_data"]
except KeyError as e:
    logger.error(f"Missing key in config.yaml: {e}")
    raise





def get_last_transaction_time():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            return pd.to_datetime(f.read().strip())
    return pd.to_datetime("1900-01-01")

def update_last_transaction_time(latest_time):
    with open(STATE_FILE, 'w') as f:
        f.write(latest_time.strftime('%Y-%m-%d %H:%M:%S'))

def extract_data():
    logger.info("Extracting data from the source...")

    # Load full datasets
    customers = pd.read_excel(CUSTOMER_FILE)
    products = pd.read_excel(PRODUCTS_FILE)

    # Load transactions and filter by timestamp
    transactions_full = pd.read_excel(TRANSACTIONS_FILE)
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

