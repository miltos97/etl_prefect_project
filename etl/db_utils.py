import duckdb
import pandas as pd
from etl.utils import setup_logger
import yaml
logger = setup_logger()

def load_to_duckdb():
    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    duckdb_path = config["db_path"]
    table_name = "final_dataset"

    logger.info(f"Loading data to DuckDB at {duckdb_path}, table: {table_name}")
    con = duckdb.connect(database=str(duckdb_path))

    Aanalytical_Ready_Data = config["output_path_fin"]
    data_prod = pd.read_csv(Aanalytical_Ready_Data)
    con.register("df_view", data_prod)

    con.execute(f"DROP TABLE IF EXISTS {table_name}")
    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM df_view")
    con.close()

    logger.info("Data successfully loaded to DuckDB")
