import os
import io
import requests
import pandas as pd
from dotenv import load_dotenv

# Load Credentials
load_dotenv()
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# Date Range
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


# Load CSV File Campaigns Info
print("📥 Loading campaigns from campaigns.csv...")
try:
    csv_data = os.getenv("CAMPAIGNS_CSV")
    campaigns_df = pd.read_csv(io.StringIO(csv_data))
except FileNotFoundError:
    print("❌ File campaigns.csv not found.")
    exit()

print(f"🔍 Processing {len(campaigns_df)} campaigns using '{DATE_PRESET}'...\n")

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

# Notifications
if paused_campaigns:
    print("\n🔔 ¡ATTENTION! There are campaign paused. Please, check!.")
    for campaign in paused_campaigns:
        print(f"- {campaign['name']} ({campaign['id']})")
else:

    print("\n✅ All campaign are running. ¡Everything is ok!")
