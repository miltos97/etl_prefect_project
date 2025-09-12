# Install Python
https://www.python.org/downloads/

python --version 

# Virtual Environment creation
```plaintext
python -m venv etl_env
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
etl_env\Scripts\activate
```

# Install Prefect

pip install prefect

# 1. Folder structure
```plaintext
etl_prefect_project
|
|--- d1
|--- data/
|       |--- Customer_Data.xlsx
|       |--- Products_Data.xlsx
|       |--- Transaction_Data.xlsx
|
|--- etl/
|       |--- extract.py
|       |--- load.py
|       |--- transform.py
|       |--- utils.py
|
|--- logs/
|       |--- etl.log
|
|--- config.yaml
|--- pipeline.py
|---README.md
```
# 2. Configuration File

In the config.yaml file are defined paths and parameters used in the pipeline.

# 3. Logging Setup

The custom logger utils.py writes logs to the file logs/etl.log

# 4. ETL Scripts
```plaintext
extract.py – The script for the data extraction
transform.py – The script for the data transformation
load.py – The script for the data load
```
# 5. Pipeline Script

```plaintext
pipeline.py – The script that calls the tasks executing the ETL scripts
```

# 6. Running the Pipeline
python pipeline.py

# 7. Log Verification

Check logs/etl.log for messages like:
```plaintext
2025-09-12 11:45:00 - INFO - Saving clean data...
2025-09-12 11:45:01 - INFO - Data saved successfully
```