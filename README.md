# Install Python
```plaintext
https://www.python.org/downloads/

(version 3.11.1)
```


# Virtual Environment
Create a Virtual environment, then activate it and install the requirements
```plaintext
python -m venv etl_env   #Virtual Environment creation
etl_env\Scripts\activate #Virtual Environment activation
pip install requirements.txt  #requirements installation
```

# Folder structure

Download the files to your local machine
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
|--- README.md
|--- requirements.txt
```
# Running the Pipeline
Execute the command below in the Virtual Environment
```plaintext
python pipeline.py
```