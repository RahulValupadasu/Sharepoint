import os
from io import BytesIO
import pandas as pd
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.files.file import File
from office365.runtime.client_request_exception import ClientRequestException

# SharePoint configuration
sp_url = "https://zoetis.sharepoint.com/"
site_path = "PetcareBrandMarketingTeam"
site_url = f"{sp_url}{site_path}"
document_library = "Shared%20Documents"

# Target Excel file inside the document library
orders_summary_path = (
    "Core Brands/Promotions/Vanguard Annual Programs/2025/"
    "2025 CAC - free doses/Tracker/Orders Summary.xlsx"
)

# Service account credentials retrieved from Databricks secrets
scope = os.environ.get("NGSE_KEY_VAULT_SCOPE")
username = dbutils.secrets.get(scope=scope, key="svc-azr-ngsesharepnt-user")
password = dbutils.secrets.get(scope=scope, key="svc-azr-ngsesharepnt-password")

# Authenticate and create a client context
credentials = UserCredential(username, password)
ctx = ClientContext(site_url).with_credentials(credentials)


def read_excel_from_sharepoint(ctx, library, relative_path, sheet_name="Sheet1"):
    """Download an Excel file from SharePoint and return it as a pandas DataFrame."""
    file_url = f"/{site_path}/{library}/{relative_path}"
    try:
        response = File.open_binary(ctx, file_url)
        data = BytesIO(response.content)
        return pd.read_excel(data, sheet_name=sheet_name, engine="openpyxl")
    except ClientRequestException as exc:
        if exc.response_code in (401, 403):
            raise PermissionError("Unauthorized access to SharePoint") from exc
        raise


if __name__ == "__main__":
    try:
        df = read_excel_from_sharepoint(ctx, document_library, orders_summary_path)
        print(df)
    except PermissionError as e:
        print(e)
    except Exception as e:
        print(f"Failed to read '{orders_summary_path}': {e}")
