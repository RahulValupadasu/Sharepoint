# Databricks notebook source
import os
import pytz
from datetime import datetime, timedelta
import pandas as pd
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from pyspark.sql.functions import col, hour, minute, to_timestamp, date_format, from_utc_timestamp, lit

# SharePoint site URL
sp_url = "https://zoetis.sharepoint.com/"
site_path = "PetcareBrandMarketingTeam"
site_url = f"{sp_url}{site_path}"
document_library = "Shared%20Documents/"

# Define the folder name
report_date = "2025-06-09"
report_date = datetime.strptime(report_date, "%Y-%m-%d").date()
folder_name = str(report_date)
print(f'folder_name {folder_name}')

local_excel_file_name = f"test_RV.xlsx"

# ADLS
ADLS_ACCOUNT = os.environ.get("NGSE_ADLS_ACCOUNT")
ADLS_CONTAINER = "ad-hoc-data"
print(ADLS_ACCOUNT)
print(ADLS_CONTAINER)



# COMMAND ----------

# SharePoint Service account credentials
scope = os.environ["NGSE_KEY_VAULT_SCOPE"]
username = dbutils.secrets.get(scope=scope, key="svc-azr-ngsesharepnt-user")
password = dbutils.secrets.get(scope=scope, key="svc-azr-ngsesharepnt-password")

# Authenticate and create a client context
credentials = UserCredential(username, password)
ctx = ClientContext(site_url).with_credentials(credentials)



# COMMAND ----------

data = [
    ("Sam",     "21 st white",    "canda",  214),
    ("Konstas", "21 white fish",  "us",     315),
    ("smith",   "34 hooland rd",  "mexico",  21)
]

columns = ["Name", "Address", "Country", "Id"]

df = spark.createDataFrame(data, schema=columns)
df.show()

# COMMAND ----------



# COMMAND ----------



def save_dataframe_to_excel(df, file_path):
    pandas_df = df.toPandas()
    pandas_df.to_excel(file_path, index=False, engine='openpyxl')

def create_folder(document_library, folder_name):
    target_folder_url = f"{document_library}/{folder_name}"
    folder = ctx.web.folders.add(target_folder_url)
    ctx.execute_query()

def upload_file(document_library, folder_name, file_path):
    with open(file_path, 'rb') as file:
        file_name = file_path.split('/')[-1]
        folder_url = f"/{site_path}/{document_library}/{folder_name}"
        target_file_url = f"{folder_url}/{file_name}"
        ctx.web.get_folder_by_server_relative_url(folder_url).upload_file(file_name, file).execute_query()
    print(f"File '{file_name}' uploaded successfully to '{folder_url}'.")

# create_folder(document_library, folder_name)
# save_dataframe_to_excel(df, local_excel_file_name)
upload_file(document_library, folder_name, local_excel_file_name)

# cleanup temp
#os.remove(local_excel_file_name)

print("File uploaded successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://use-cases@prodazrddp01adls.dfs.core.windows.net/usba/aggregated/TDE_Active_Churned/2024-12-13/`
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://use-cases@devazrddp01adls.dfs.core.windows.net/usba/DX/CallActivity/CallActivity.csv/`
# MAGIC
# MAGIC
