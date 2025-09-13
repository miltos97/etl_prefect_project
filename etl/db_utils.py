import duckdb
import pandas as pd
from etl.utils import setup_logger
import yaml
logger = setup_logger()

def load_to_duckdb(table_name: str = "final_dataset"):
    with open("config.yaml") as f:
        config = yaml.safe_load(f)
    duckdb_path = config["db_path"]
    logger.info(f"Loading data to DuckDB at {duckdb_path}, table: {table_name}")
    con = duckdb.connect(database=str(duckdb_path))
    data_prod = pd.read_csv(r'C:\Users\symeom\OneDrive - Pfizer\Desktop\SrAssociate_Data_Engineer\etl_prefect_project\d2\Aanalytical_Ready_Data.csv')
    con.register("df_view", data_prod)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df_view")
    con.close()
    logger.info("Data successfully loaded to DuckDB")
