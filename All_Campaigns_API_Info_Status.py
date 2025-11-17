import os
import io
import json
import requests
import pandas as pd
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

# Load Credentials
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
DATE_PRESET = "this_month"

# Function to get campaign status
def get_campaign_status(campaign_id, access_token):
    url = f"https://graph.facebook.com/v19.0/{campaign_id}"
    params = {
        "fields": "status",
        "access_token": access_token
    }
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Lanza un error para códigos de estado 4xx/5xx
        return response.json().get('status', 'UNKNOWN')
    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching status for {campaign_id}: {e}")
        return 'UNKNOWN'

# === Load Campaigns from Google Sheets ===
print("📥 Loading campaigns from Google Sheets...")

# Leer credenciales del entorno
creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
if not creds_json:
    print("❌ Variable GOOGLE_CREDENTIALS_JSON no encontrada.")
    exit()

# Convertir JSON string a diccionario
creds_dict = json.loads(creds_json)
scopes = ["https://www.googleapis.com/auth/spreadsheets"]
credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)

# Autorizar gspread
client = gspread.authorize(credentials)

# Leer ID y nombre de la hoja
CAMPAIGN_INFO_SPREADSHEET_ID = os.getenv("CAMPAIGN_INFO_SPREADSHEET_ID")
SHEET_NAME = os.getenv("SHEET_NAME", None)  # opcional

try:
    if SHEET_NAME:
        sheet = client.open_by_key(CAMPAIGN_INFO_SPREADSHEET_ID).worksheet("Campaigns")
    else:
        sheet = client.open_by_key(CAMPAIGN_INFO_SPREADSHEET_ID).sheet1  # primera hoja

    data = sheet.get_all_records()
    campaigns_df = pd.DataFrame(data)

    print(f"🔍 Loaded {len(campaigns_df)} campaigns from Google Sheet '{sheet.title}'.\n")

except Exception as e:
    print(f"❌ Error loading Google Sheet: {e}")
    exit()

# === Fetch Campaign Data ===
results = []
paused_campaigns = []

for _, row in campaigns_df.iterrows():
    campaign_name = row["Campaign Name"]
    campaign_id = row["Campaign ID"]

    print(f"➡️  Fetching data for: {campaign_name} ({campaign_id})...")

    # ---- Step 1: Get Campaign Status ----
    campaign_status = get_campaign_status(campaign_id, ACCESS_TOKEN)

    # ---- Step 2: Get the insights (métricas) ----
    url = f"https://graph.facebook.com/v19.0/{campaign_id}/insights"
    params = {
        "fields": "spend,actions",  # Eliminamos 'campaign_status' de aquí
        "date_preset": DATE_PRESET,
        "level": "campaign",
        "access_token": ACCESS_TOKEN
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        error = response.json().get('error', {}).get('message', 'Unknown error')
        print(f"❌ Error: {error}\n")
        continue

    data = response.json().get("data", [])
    if not data:
        print(f"⚠️  No data found for {campaign_name}.\n")
        continue

    item = data[0]
    spend = float(item.get("spend", 0))
    actions = item.get("actions", [])

    # Alert
    states_to_alert = ['PAUSED', 'ADVERTISER_PAUSED', 'INACTIVE']

    if campaign_status in states_to_alert:
        paused_campaigns.append({"id": campaign_id, "name": campaign_name})
        print(f"⚠️ ¡ALERT! Campaign '{campaign_name}' status '{campaign_status}'.\n")

    leads = next((int(a["value"]) for a in actions if a["action_type"] == "lead"), 0)
    cpl = round(spend / leads, 2) if leads > 0 else 0

    results.append({
        "Campaign ID": campaign_id,
        "Campaign Name": campaign_name,
        "Status": campaign_status,
        "Spend": spend,
        "Leads": leads,
        "CPL": cpl,
    })

    print(f"✅ {campaign_name} → Status: {campaign_status}, Spend: ${spend:.2f}, Leads: {leads}, CPL: {cpl}\n")

# Export results
df = pd.DataFrame(results)
df.to_csv("Campaign_Insights.csv", index=False)
print(f"\n📊 Report saved: Campaign_Insights.csv ({len(df)} campaigns processed)")

# === Write results back to Google Sheet ===
try:
    print("\n📤 Updating 'Results' sheet in Google Sheets...")

    # Abre la hoja principal
    spreadsheet = client.open_by_key(CAMPAIGN_INFO_SPREADSHEET_ID)

    # Intenta acceder a la pestaña "Results"
    try:
        results_sheet = spreadsheet.worksheet("Results")
        results_sheet.clear()
    except Exception:
        # Si no existe, la crea
        results_sheet = spreadsheet.add_worksheet(title="Results", rows="100", cols="10")

    # Convierte DataFrame a lista de listas
    results_values = [df.columns.tolist()] + df.values.tolist()

    # Escribe los datos
    results_sheet.update("A1", results_values)

    print("✅ 'Results' sheet updated successfully!")

except Exception as e:
    print(f"❌ Error updating 'Results' sheet: {e}")


# Notifications
if paused_campaigns:
    print("\n🔔 ¡ATTENTION! There are campaign paused. Please, check!.")
    for campaign in paused_campaigns:
        print(f"- {campaign['name']} ({campaign['id']})")
else:

    print("\n✅ All campaign are running. ¡Everything is ok!")








