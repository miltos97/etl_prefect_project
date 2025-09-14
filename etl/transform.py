import pandas as pd
from etl.utils import setup_logger
logger = setup_logger()

def remove_duplicates(transactions, customers, products):
    logger.info("Removing duplicate rows from each dataset.")
    return (
        transactions.drop_duplicates(),
        customers.drop_duplicates(),
        products.drop_duplicates()
    )
    logger.info("Duplicated removed.")


def clean_customers(customers):
    logger.info("Filling missing Age values with median.")
    customers_clean = customers.copy()
    customers_clean['Age'] = pd.to_numeric(customers_clean['Age'])
    customers_clean['Age'] = customers_clean['Age'].fillna(customers_clean['Age'].median())
    logger.info("Customers dataset was cleaned.")
    return customers_clean
def clean_products(products):
     logger.info("Filling missing Category values with 'N/A'.")
     products_clean = products.copy()
     products_clean['Category'] = products_clean['Category'].fillna('N/A')
     logger.info("Products dataset was cleaned.")
     return products_clean
def clean_transactions(transactions):
    logger.info("Filling missing Price values and convert TransactionTime to datetime.")
    transactions_clean = transactions.copy()
    transactions_clean['Price'] = transactions_clean.apply(
        lambda row: row['TotalPrice'] / row['Quantity'] if pd.isna(row['Price']) else row['Price'],
        axis=1
    )
    transactions_clean['TransactionTime'] = pd.to_datetime(transactions_clean['TransactionTime'])
    logger.info("Transactions dataset was cleaned.")
    return transactions_clean
def merge_datasets(transactions_clean, customers_clean, products_clean):
    logger.info("Merging the three datasets into one unified DataFrame.")
    merged = transactions_clean.merge(customers_clean, on='CustomerID', how='left')
    merged = merged.merge(products_clean, on='ProductID', how='left')
    logger.info("Merging completed.")
    return merged
def fill_missing_values(merged):
    logger.info("Fill missing values in merged dataset with 'N/A'.")
    merged_clean = merged.copy()
    merged_clean['Age'] = pd.to_numeric(merged_clean['Age'])
    columns_to_fill = [
        'Gender', 'Age', 'AnnualIncome', 'EducationLevel', 'MaritalStatus',
        'Region', 'LoyaltyPoints', 'PreferredChannel'
    ]
    merged_clean[columns_to_fill] = merged_clean[columns_to_fill].fillna('N/A')
    logger.info("Final dataset was cleaned.")
    return merged_clean

def transform_data(df):
    try:
        logger.info("Transforation started.")
        transactions = df["transactions"]
        customers = df["customers"]
        products = df["products"]
        #Remove duplicates
        transactions, customers, products = remove_duplicates(transactions, customers, products)
        #Clean individual datasets
        customers_clean = clean_customers(customers)
        products_clean = clean_products(products)
        transactions_clean = clean_transactions(transactions)
        #Merge datasets
        merged = merge_datasets(transactions_clean, customers_clean, products_clean)
        #Fill missing values
        final_df = fill_missing_values(merged)
        logger.info("Transforation completed.")
        return final_df
    except Exception as e:
        logger.error(f"Transformation failed: {e}")
        raise

        
    