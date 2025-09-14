
import os
import pandas as pd
import yaml
from etl.utils import setup_logger
logger = setup_logger()

def load_data(final_df):
    logger.info("Loading processed data...")
    try:
        with open("config.yaml") as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logger.error("Configuration file 'config.yaml' not found.")
        return
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file: {e}")
        return

    try: 
        output_path = config["output_path"]
        incremental_col = config["incremental_column"]
    except KeyError as e:
        logger.error(f"Missing key in config.yaml: {e}")
        return

    try:
        if os.path.exists(output_path):
            existing_df = pd.read_csv(output_path)
            final_df[incremental_col] = pd.to_datetime(final_df[incremental_col])
            existing_df[incremental_col] = pd.to_datetime(existing_df[incremental_col])
            final_df = final_df[~final_df[incremental_col].isin(existing_df[incremental_col])]
            dfs_to_concat = [df for df in [existing_df, final_df] if not df.empty and not df.isna().all().all()]
            final_df = pd.concat(dfs_to_concat, ignore_index=True)
        final_df.to_csv(output_path, index=False, na_rep='N/A')
        logger.info("Load completed successfully.")
    except Exception as e:
        logger.error(f"Error during data saving process: {e}")

