from prefect import flow, task
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.utils import setup_logger


logger = setup_logger()

@task
def extract_task():
    logger.info("Starting extraction")
    return extract_data()

@task
def transform_task(df):
    logger.info("Starting transformation")
    return transform_data(df)

@task
def load_task(final_df):
    logger.info("Starting load")
    load_data(final_df)
    logger.info("Load complete")

@flow(name="ETL Pipeline")
def etl_flow():
    try:
        raw_data = extract_task()
        transformed_data = transform_task(df=raw_data)
        load_task(final_df=transformed_data)
    except Exception as e:
        logger.error(f"ETL failed: {e}")
        raise

if __name__ == "__main__":
    etl_flow()

