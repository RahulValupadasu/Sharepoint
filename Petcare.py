import os
from io import BytesIO
from typing import Optional

import pandas as pd
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.files.file import File
from office365.runtime.client_request_exception import ClientRequestException
from pyspark.sql import SparkSession
from urllib.parse import quote

# "dbutils" and "spark" are automatically available in Databricks notebooks.
# When running this script elsewhere, provide compatible objects (e.g. from
# Databricks Connect) or pass a ``SparkSession`` to ``read_as_spark_df``.

# SharePoint configuration
sp_url = "https://zoetis.sharepoint.com/"
# full server-relative path to the SharePoint site
site_path = "sites/PetcareBrandMarketingTeam"
site_url = f"{sp_url}{site_path}"
# document library name (spaces do not need encoding)
document_library = "Shared Documents"

# File relative path inside the document library
orders_summary_relative_path = (
    "Core Brands/Promotions/Vanguard Annual Programs/2025/"
    "2025 CAC - free doses/Tracker/order_summary.xlsx"
)

# Credentials from Databricks secrets
# "dbutils.secrets" retrieves secrets stored in Databricks. Replace these
# lines with your own secrets management solution when running elsewhere.
scope = os.environ.get("NGSE_KEY_VAULT_SCOPE")
username = dbutils.secrets.get(scope=scope, key="svc-azr-ngsesharepnt-user")
password = dbutils.secrets.get(scope=scope, key="svc-azr-ngsesharepnt-password")

credentials = UserCredential(username, password)
ctx = ClientContext(site_url).with_credentials(credentials)


def check_connection(context: ClientContext) -> None:
    """Validate SharePoint connection."""
    try:
        context.web.get().execute_query()
        print(f"Connected to site: {site_url}")
    except ClientRequestException as exc:
        if exc.response_code in (401, 403):
            raise PermissionError("Unauthorized access to SharePoint") from exc
        raise


def read_excel_from_sharepoint(
    context: ClientContext,
    library: str,
    relative_path: str,
    sheet_name: str = "Sheet1",
) -> pd.DataFrame:
    """Download an Excel file from SharePoint and return a pandas DataFrame."""
    raw_url = f"/{site_path}/{library}/{relative_path}"
    # Encode spaces and special characters but keep the slash separators
    file_url = quote(raw_url, safe="/")
    try:
        response = File.open_binary(context, file_url)
        data = BytesIO(response.content)
        return pd.read_excel(data, sheet_name=sheet_name, engine="openpyxl")
    except ClientRequestException as exc:
        if exc.response_code in (401, 403):
            raise PermissionError("Unauthorized access to SharePoint") from exc
        if exc.response_code == 404:
            raise FileNotFoundError(f"File not found: {file_url}") from exc
        raise



def read_as_spark_df(
    context: ClientContext,
    library: str,
    relative_path: str,
    sheet_name: str = "Sheet1",
    spark_session: Optional[SparkSession] = None,
):
    """Return the Excel file from SharePoint as a Spark DataFrame."""
    pandas_df = read_excel_from_sharepoint(context, library, relative_path, sheet_name)
    if spark_session is None:
        spark_session = spark  # type: ignore[name-defined]
    return spark_session.createDataFrame(pandas_df)


if __name__ == "__main__":
    try:
        check_connection(ctx)
        spark_df = read_as_spark_df(ctx, document_library, orders_summary_relative_path)
        spark_df.show()
    except PermissionError as err:
        print(err)
    except Exception as err:
        print(f"Failed to read '{orders_summary_relative_path}': {err}")
