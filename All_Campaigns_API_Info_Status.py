import os
import requests
import pandas as pd
from dotenv import load_dotenv

# Cargar credenciales
load_dotenv()
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# Elegir rango de fechas
DATE_PRESET = "this_month"


# Función para obtener el estado de una campaña
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


# Cargar archivo con campañas
print("📥 Loading campaigns from campaigns.csv...")
try:
    campaigns_df = pd.read_csv("campaigns_info.csv")
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

    # ---- Paso 1: Obtener el estado de la campaña por separado ----
    campaign_status = get_campaign_status(campaign_id, ACCESS_TOKEN)

    # ---- Paso 2: Obtener los insights (métricas) ----
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

    # Lógica de alerta
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

# Exportar resultados
df = pd.DataFrame(results)
df.to_csv("Campaign_Insights_Andres.csv", index=False)
print(f"\n📊 Report saved: Campaign_Insights_Andres.csv ({len(df)} campaigns processed)")

# Sección de Notificaciones
if paused_campaigns:
    print("\n🔔 ¡ATTENTION! There are campaign paused. Please, check!.")
    for campaign in paused_campaigns:
        print(f"- {campaign['name']} ({campaign['id']})")
else:
    print("\n✅ All campaign are running. ¡Everything is ok!")