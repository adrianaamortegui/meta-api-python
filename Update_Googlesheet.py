import os
import json
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Autenticación con Google
SERVICE_ACCOUNT_FILE = 'facebookcampaignsdr-andres.json'
SPREADSHEET_ID = '16aMdJ4PNk2Kj_QI4Ox59u-uitcMcT8me6yLWkwtdetk'
scopes = ['https://www.googleapis.com/auth/spreadsheets']
credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
client = gspread.authorize(credentials)

# Abrir hoja
sheet = client.open_by_key(SPREADSHEET_ID)
#worksheet = sheet.sheet1
worksheet = sheet.worksheet("October")

# Leer hoja desde A2 en adelante (dejando A1 para la fecha)
existing_data = worksheet.get_all_values()[1:]  # Salta A1

if existing_data:
    headers = existing_data[0]
    rows = existing_data[1:]
    # Crear DataFrame con columnas existentes
    df_sheet = pd.DataFrame(rows, columns=headers)
else:
    df_sheet = pd.DataFrame(columns=["Campaign Name", "Spend", "Leads", "CPL"])

# Read CSV Update
df_csv = pd.read_csv("Campaign_Insights.csv", keep_default_na=False)
#df_csv.replace("N/A", "", inplace=True)

# Limpieza completa
df_csv.replace([np.nan, np.inf, -np.inf, "NaN", "N/A"], "", inplace=True)
df_csv.fillna("", inplace=True)
df_csv = df_csv.astype(str)  # Asegurar que todo sea string



# Si la hoja esta vacia usar los datos del CSV directamente
if df_sheet.empty:
    df_updated = df_csv.copy()
else:
    # Hacer merge por Campaign Name
    df_updated = pd.merge(df_sheet, df_csv, on="Campaign Name", how="left", suffixes=('', '_new'))

    # Actualizar columnas específicas
    for col in ['Spend', 'Leads', 'CPL', 'Status']:
        if f"{col}_new" in df_updated.columns:
            df_updated[col] = df_updated[f"{col}_new"]
            df_updated.drop(columns=[f"{col}_new"], inplace=True)

# 1. Reemplazar NaN, inf, -inf
df_updated.replace([np.nan, np.inf, -np.inf, "NaN", "N/A"], "", inplace=True)

# 2. Asegurar que no haya nulos restantes
df_updated.replace([np.nan, np.inf, -np.inf, "NaN", "N/A"], "", inplace=True)
df_updated.fillna("", inplace=True)

# 3. Convertir todo a string (100% JSON-safe)
df_updated = df_updated.astype(str)

print("¿Hay NaNs?", df_updated.isnull().values.any())
print("¿Hay infinitos?", np.isinf(df_updated.select_dtypes(include=[np.number])).values.any())
print(df_updated.head())

# ✅ Actualizar A1 con la fecha
fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
worksheet.update_acell('A1', f"Last updated: {fecha_actual}")

# ✅ Subir la tabla desde A2 hacia abajo
worksheet.update(
    values=[df_updated.columns.tolist()] + df_updated.values.tolist(),
    range_name='A2'
)

print("✅ Google Sheet updated: fecha en A1 y datos desde A2.")


