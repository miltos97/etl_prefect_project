from prefect import flow, task
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.utils import setup_logger
from etl.data_product_creation import create_data_product
from etl.data_product_creation import load_processed_data
from etl.db_utils import load_to_duckdb


logger = setup_logger()

# 1st Prefect Task - extracts the data from the source
@task
def extract_task():
    logger.info("Starting extraction")
    return extract_data()

# 2nd Prefect Task - makes the transformations 
@task
def transform_task(df):
    logger.info("Starting transformation")
    return transform_data(df)

# 3rd Prefect Task - loads the transformed data
@task
def load_task(final_df):
    logger.info("Starting load")
    load_data(final_df)
    logger.info("Load complete")

# Task 1C: Data Product Creation:
# 4th Prefect Task - loads the processed data
@task
def load_proc_data_task(): 
    return load_processed_data()
    logger.info("Loading completed.")

# 5th Prefect Task - craetes and extracts the data product
@task
def data_product_task(processed_data):
    logger.info("Starting data product creation")
    return create_data_product(processed_data)
    logger.info("Data product creation complete")

# 6th Prefect Task - Load the final dataset into duckdb
@task
def load_task_final():
    logger.info("Starting load to duckdb")
    load_to_duckdb()
    logger.info("Load completed to duckdb")


# Prefect flow

@flow(name="ETL Pipeline")
def etl_flow():
    try:
        extracted_data = extract_task()
        new_transactions = extracted_data.get("transactions")
        if new_transactions is None or new_transactions.empty:
            logger.info("No new transactions found. Skipping transformation and load.")
            return

        transformed_data = transform_task(df=extracted_data)
        load_task(final_df=transformed_data)
        
        processed_data = load_proc_data_task()
        data_product_task(processed_data)

        load_task_final()
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise

if __name__ == "__main__":
    etl_flow()

