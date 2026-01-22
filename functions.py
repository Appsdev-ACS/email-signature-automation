import requests
import pandas as pd
from google.auth import default
import gspread



def get_access_token(CLIENT_ID,CLIENT_SECRET,TOKEN_URL):
    
    """Fetch the access token from Veracross API."""
    client_id = CLIENT_ID
    client_secret = CLIENT_SECRET

    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "staff_faculty:list"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}

    response = requests.post(TOKEN_URL, data=data, headers=headers)

    if response.status_code == 200:
        print("access token obtained!")
        return response.json().get("access_token")
    else:
        print("Error fetching access token:", response.text)
        return None
    

def get_staff(VC_STAFF_URL,access_token):
    """Fetch all student data using pagination via headers."""
    print("calling staff list.....")
    access_token = access_token
    if not access_token:
        print("No access token")
        return

    all_staff = []
    page = 1
    page_size = 1000  
    while True:
        headers = {
            "Authorization": f"Bearer {access_token}",
            "X-Page-Number": str(page),
            "X-Page-Size": str(page_size),
            "X-API-Value-Lists" : "include"

            # "X-API-Revision": "latest"  # Optional: Ensures the latest API version
        }

        response = requests.get(VC_STAFF_URL, headers=headers)
        # print("got response for student list")

        if response.status_code == 200:
            staffs = response.json()
            if staffs["data"] == []:
                break


            degree = {item["id"] : item["description"] for item in staffs["value_lists"][1]["items"]}

            for entry in staffs["data"]:

                if entry["name_suffix"] in degree:
                    if  entry["name_suffix"] == 111 or entry["name_suffix"] == 1001:
                        entry["name_suffix"] = degree[entry["name_suffix"]]
                    else:
                        entry["name_suffix"] = None


            all_staff.extend(staffs["data"])
            page += 1 
        else:
            print("Error fetching students:", response.text)
            break

    print(f"Total staffs fetched: {len(all_staff)}")

    df = pd.DataFrame(all_staff)
    # df = df.dropna(subset=["email_1"])
    df = df[df["email_1"].str.contains("@acs.sch.ae", na=False)]
    df["job_title"] = df["job_title"].fillna("")
    df["job_title"] = df["job_title"].astype(str).str.upper()
    df["FIRST_NAME"] = df["preferred_name"].fillna(df["first_name"])
    df = df.rename(columns={
        "email_1": "EMAIL",
        "last_name": "LAST_NAME",
        "job_title": "TITLE",
        "name_suffix": "DEGREE"
    })
    # df.loc[df["FIRST_NAME"] == "Yaseen", "DEGREE"] = "Ph.D."
    df["DEGREE"] = df["DEGREE"].where(df["DEGREE"].isna(), ", " + df["DEGREE"].astype(str))

    # print(df.columns)

    df = df[["EMAIL","FIRST_NAME","LAST_NAME","TITLE","DEGREE"]]
    return df




def upload_to_google_sheets(df,SPREADSHEET_NAME,sheet_name):
    """Uploads the DataFrame to Google Sheets."""
    # creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    creds, _ = default(scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)

    # Open or create the Google Sheet
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet(sheet_name)
    except gspread.SpreadsheetNotFound:
        sheet = client.create(SPREADSHEET_NAME).worksheet(sheet_name)  # Create a new sheet

    # Clear existing data
    sheet.clear()

    values = [df.columns.tolist()] + df.astype(str).values.tolist()
    sheet.update("A1", values)   # ✅ much faster
    print("✅ Data uploaded in one batch!")


def get_google_sheet_data(SPREADSHEET_NAME,sheet_name):

    creds, _ = default(scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(creds)

    # Open the spreadsheet and sheet
    try:
        sheet = client.open(SPREADSHEET_NAME).worksheet(sheet_name)
    except gspread.SpreadsheetNotFound:
        raise FileNotFoundError(f"The spreadsheet '{SPREADSHEET_NAME}' was not found.")
    
    data = sheet.get_all_values()

    if not data:
        old_df = pd.DataFrame()
    else:
        # First row is header
        old_df = pd.DataFrame(data[1:], columns=data[0])
    
    return old_df

def get_updates_df(df, old_df, key_columns=None):
    """
    Returns a DataFrame with new or updated rows compared to old_df.
    """
    if old_df.empty:
        return df.copy()

    if key_columns is None:
        key_columns = df.columns.tolist()

    # New entries
    merged = df.merge(old_df, how='outer', indicator=True)
    new_entries = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

    # Changed entries
    common_cols = old_df.columns.tolist()
    df_common = df[common_cols].copy()
    old_df_common = old_df[common_cols].copy()

    comparison = df_common.merge(
        old_df_common,
        on=key_columns,
        how='inner',
        suffixes=('_new', '_old')
    )

    # Rows where at least one column changed
    changed_mask = (comparison.filter(like='_new') != comparison.filter(like='_old')).any(axis=1)
    changed_rows = comparison.loc[changed_mask, comparison.filter(like='_new').columns]
    changed_rows.columns = [c.replace('_new', '') for c in changed_rows.columns]

    # Combine new + changed rows
    updates_df = pd.concat([new_entries, changed_rows], ignore_index=True)

    return updates_df
