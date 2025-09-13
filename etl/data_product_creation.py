import pandas as pd
import yaml
from etl.utils import setup_logger
logger = setup_logger()

# load processed data
def load_processed_data():
    logger.info("Loading processed data...")
    processed_data = pd.read_csv(r'C:\Users\symeom\OneDrive - Pfizer\Desktop\SrAssociate_Data_Engineer\etl_prefect_project\d1\processed_data.csv')
    return processed_data




# Aggregated metrics
def aggr_data(processed_data):
    logger.info("Aggregating the data")
    aggregated = processed_data.groupby("CustomerID").agg(
    TotalSales=("TotalPrice", "sum"),
    AverageSpend=("TotalPrice", "mean"),
    NumTransactions=("TransactionID", "count")
    ).reset_index()
    return aggregated

# Age group classification
def age_group(age):
    if pd.isna(age):
        return "Unknown"
    elif age < 20:
        return "Teen"
    elif age < 30:
        return "Young Adult"
    elif age < 45:
        return "Adult"
    elif age < 60:
        return "Middle Age"
    else:
        return "Senior"

def get_customer_info(processed_data):
    logger.info("Preparing age group classification")
    customer_info = processed_data.groupby("CustomerID").first().reset_index()
    customer_info["Age"] = pd.to_numeric(customer_info["Age"], errors="coerce")
    customer_info["AgeGroup"] = customer_info["Age"].apply(age_group)
    return customer_info


# Product preference   
def get_prod_pref(processed_data):
    logger.info("Preparing product preference")
    product_pref = processed_data.groupby("CustomerID")["ProductName"].agg(
    lambda x: x.mode().iloc[0] if not x.mode().empty else "Unknown"
    ).reset_index()
    product_pref.rename(columns={"ProductName": "ProductPreference"}, inplace=True)
    return product_pref

# Merge all data
def merge_data(aggregated,customer_info,product_pref):
    logger.info("Merging all data")
    merged_df = aggregated.merge(customer_info, on="CustomerID").merge(product_pref, on="CustomerID")
    return merged_df

# Select relevant columns
def data_prod(merged_df):
    logger.info("Selecting relevant columns")
    selected_columns = ["CustomerID", "TotalSales", "AverageSpend", "NumTransactions","AgeGroup","ProductPreference","AnnualIncome","EducationLevel","MaritalStatus","Region","LoyaltyPoints","PreferredChannel"]
    final = merged_df[selected_columns]
    round_col = [ "TotalSales", "AverageSpend"]
    analytical_ready = final.copy()
    analytical_ready[round_col] = analytical_ready[round_col].astype(float).round(2)
    analytical_ready = analytical_ready.fillna('N/A')
    return analytical_ready

# Export to CSV
def save_to_csv(analytical_ready): 
    logger.info("Saving final data...")
    with open("config.yaml") as f:
        config = yaml.safe_load(f)
    output_path = config["output_path_fin"]
    analytical_ready.to_csv(output_path, index=False)
    logger.info("Data saved successfully.")


def create_data_product(processed_data):
    #processed_data = load_processed_data()
    aggregated = aggr_data(processed_data)
    customer_info = get_customer_info(processed_data)
    product_pref = get_prod_pref(processed_data)
    merged_df = merge_data(aggregated,customer_info,product_pref)
    analytical_ready = data_prod(merged_df)
    save_to_csv(analytical_ready)
    return create_data_product






