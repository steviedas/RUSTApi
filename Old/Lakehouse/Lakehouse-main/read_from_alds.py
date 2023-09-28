from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import os
import json
import pandas as pd

# Delta table path 
delta_table_path = f"abfss://abc@abc.dfs.core.windows.net/test/demo1"
# ADLS storage account name and key 
storage_options = {"azure_storage_account_name": "storageaccountname", "azure_storage_access_key": "****"} 

# Define delta table 
dt = DeltaTable(delta_table_path, storage_options=storage_options) 

# Read Data from Delta table 
dt.to_pandas()

# Check version of Delta table
print(dt.version())

# Check files from Delta tables
print(dt.files())

# Prepare Dataframe for inserting data 
df = pd.DataFrame({'id': [2], 'product_type': ['BCD'], 'sales': [1500]})

try: 
    # Append Data into Delta table on ADLS 
    write_deltalake(dt, df, mode="append") 
except Exception as e: 
    print(e)