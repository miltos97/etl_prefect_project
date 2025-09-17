# **How to run the project**


## **A) Prerequisites:**


## 1. Install Python
```plaintext
https://www.python.org/downloads/

(version 3.11.1)
```


## 2. Virtual Environment
Create a Virtual environment, then activate it and install the requirements
```plaintext
python -m venv etl_env   #Virtual Environment creation
etl_env\Scripts\activate #Virtual Environment activation
pip install requirements.txt  #requirements installation
```



## 3. Download DuckDB  
```plaintext
Go to the DuckDB Releases Page:
https://duckdb.org/docs/installation

Version: Stable release
Environment: Command line
Platform: Windows
Download method: Direct download
Architecture: arm64

Link: https://github.com/duckdb/duckdb/releases/download/v1.3.2/duckdb_cli-windows-arm64.zip

Download, extract and run the .exe file
```

## 4. Create DuckDB connection to a database tool  
```plaintext
For this project I used DBeaver.
Please feel free to use any database tool.

First open DBeaver.

In the menu bar: Go to Database -> New Database Connection -> SQL (left side-bar) -> find and select DuckDB -> Next

If you already have a DuckDB file in your pc, click on "Browse" button and select it.
If you do not have a  DuckDB file, click on  "Create" button 
or just fill in the path field with a location in your pc and at the end add a name for your DuckDB file and the file extension ".duckdb"

example: C:\Users\mydb.duckdb

Then click on the "Test connection" button and if everything is ok click "OK".
```



## 5. Update the config.yaml file
Update the paths below with the coresponding path of your pc.
```plaintext
incremental_column: TransactionTime
state_file: "C:/........./etl_prefect_project/state_file/last_transaction_time.txt"
customer_data: "C:/........./etl_prefect_project/data/Customer_Data.xlsx"
products_data: "C:/........./etl_prefect_project/data/Products_Data.xlsx"
transaction_data: "C:/........./etl_prefect_project/data/Transaction_Data.xlsx"
output_path: d1/processed_data.csv
output_path_fin: d2/Aanalytical_Ready_Data.csv
log_path: logs/etl.log
db_path: C:\Users\.........\Duck_db\mydb.duckdb
```



## **B) Running the Pipeline**
* Download the files from this repository to your local machine.
* Execute the command below in the Virtual Environment.
```plaintext
python pipeline.py
```




## **C) Additional Info:**


## Folder structure

```plaintext
etl_prefect_project
|
|--- d1                      #Here are saved the processed data                                  
|
|--- d2                      #Here are saved the Aanalytical Ready Data
|
|--- data/                               #data source or d0
|       |--- Customer_Data.xlsx
|       |--- Products_Data.xlsx
|       |--- Transaction_Data.xlsx
|
|--- etl/
|       |--- data_product_creation.py
|       |--- db_utils.py
|       |--- extract.py
|       |--- load.py
|       |--- transform.py
|       |--- utils.py
|
|--- logs/
|       |--- etl.log
|
|--- state_file/
|       |--- last_transaction_time.txt   #The datatime of the last processed transaction
|
|--- config.yaml
|--- pipeline.py
|--- README.md
|--- requirements.txt
```
