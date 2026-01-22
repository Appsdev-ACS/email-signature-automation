import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
import os
import logging
from flask import Flask,jsonify
from dotenv import load_dotenv
from functions import get_access_token,get_staff,upload_to_google_sheets,get_google_sheet_data,get_updates_df

import google.auth
import google.auth.transport.requests
from google.auth import iam
from google.oauth2 import service_account

app = Flask(__name__)
load_dotenv()

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger("signature-automation")

VC_CLIENT_ID = os.getenv("CLIENT_ID")
VC_CLIENT_SECRET = os.getenv("CLIENT_SECRET")
VC_TOKEN_URL = "https://accounts.veracross.com/acsad/oauth/token"
VC_STAFF_URL = "https://api.veracross.com/ACSAD/v3/staff_faculty"

SPREADSHEET_NAME = "Email Signature Automation"

SERVICE_ACCOUNT_FILE = "service_account.json"
CSV_FILE = "staff.csv"
HTML_FILE = "signature.html"

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.settings.basic"
]

def delegated_gmail_creds(subject_email: str):
    """
    Keyless Domain-Wide Delegation using Cloud Run runtime service account (ADC).
    No service_account.json needed.
    """
    base_creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    req = google.auth.transport.requests.Request()

    if not base_creds.valid:
        base_creds.refresh(req)

    sa_email = getattr(base_creds, "service_account_email", None)
    if not sa_email:
        raise RuntimeError("Could not determine runtime service account email from ADC.")

    signer = iam.Signer(req, base_creds, sa_email)

    return service_account.Credentials(
        signer=signer,
        service_account_email=sa_email,
        token_uri="https://oauth2.googleapis.com/token",
        scopes=GMAIL_SCOPES,
        subject=subject_email,
    )

def main():
    # df = pd.read_csv(CSV_FILE)
    if not VC_CLIENT_ID or not VC_CLIENT_SECRET:
        raise RuntimeError("Missing CLIENT_ID / CLIENT_SECRET env vars")

    access_token = get_access_token(VC_CLIENT_ID,VC_CLIENT_SECRET,VC_TOKEN_URL)
    df = get_staff(VC_STAFF_URL,access_token)

    # Load CSV
    df = df.fillna("")
    # print(df)

    old_df = get_google_sheet_data(SPREADSHEET_NAME=SPREADSHEET_NAME,sheet_name="staff_details")
    # print(old_df)
    updates_df = get_updates_df(df=df,old_df=old_df)
    # print(updates_df)

    if updates_df.empty:
        logger.info("Nothing to update.")
        return {"status": "ok", "updated": 0, "message": "Nothing to update"}

    # Load HTML template
    with open(HTML_FILE, "r", encoding="utf-8") as f:
        template = f.read()
    updated_count = 0
    failures = []
    for _, row in updates_df.iterrows():
        email = row["EMAIL"]
        print(f"Processing {email}")
        try:
            # Impersonate THE USER (not admin)
            creds = delegated_gmail_creds(email)

            gmail = build("gmail", "v1", credentials=creds)

            signature = template

            # Replace placeholders
            for col in df.columns:
                signature = signature.replace(f"{{{{{col}}}}}", str(row[col]))

            # Apply signature
            gmail.users().settings().sendAs().patch(
                userId="me",
                sendAsEmail=email,
                body={"signature": signature}
            ).execute()

            logger.info("Signature applied for %s", email)
            updated_count += 1
        except Exception as e:
            logger.exception("Failed applying signature for %s", email)
            failures.append({"email": email, "error": str(e)})
    upload_to_google_sheets(df=df,SPREADSHEET_NAME=SPREADSHEET_NAME,sheet_name="staff_details")
    return {
        "status": "ok" if not failures else "partial",
        "updated": updated_count,
        "failed": len(failures),
        "failures": failures[:20],  # cap response size
    }


@app.route("/run")
def run_job():
    try:
        result = main()  # call your existing main() function
        code = 200 if result["status"] in ("ok", "partial") else 500

        return jsonify(result), code
    except Exception as e:
        logger.exception("Job failed")
        return jsonify({"status": "error", "message": str(e)}), 500
if __name__ == "__main__":
    # app.run() #staging

        # Cloud Run sets PORT
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
