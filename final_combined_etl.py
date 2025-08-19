import pandas as pd
import sqlite3
from datetime import datetime

# === CONFIG ===
input_file = "input/Monthly Data BHS Jul'25.xlsx"
db_path = "etl_kpis.db"
output_file = "final_KPI_output.csv"

# === LOAD SHEETS ===
charges = pd.read_excel(input_file, sheet_name="Charges Jul'25")
payments = pd.read_excel(input_file, sheet_name="Payment Jul'25")
pending_ar = pd.read_excel(input_file, sheet_name="Pending AR jul'25")
adjustments = pd.read_excel(input_file, sheet_name="Adjustment Jul'25")

# === STANDARDIZE COLUMNS ===
for df in [charges, payments, pending_ar, adjustments]:
    df.columns = df.columns.str.strip().str.lower()

# === CLEAN DATES ===
def parse_dates(df, cols):
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

parse_dates(charges, ['claim date', 'service date', 'payment date'])
parse_dates(payments, ['payment date'])
parse_dates(pending_ar, ['due date'])

# === KPI CALCULATIONS ===

# 1. Charge Lag = Payment Date - Service Date
if 'payment date' in charges.columns and 'service date' in charges.columns:
    charges['charge_lag'] = (charges['payment date'] - charges['service date']).dt.days

# 2. Billing Lag = Payment Date - Claim Date
if 'payment date' in charges.columns and 'claim date' in charges.columns:
    charges['billing_lag'] = (charges['payment date'] - charges['claim date']).dt.days

# 3. GCR = Payments / Charges
if 'payment' in charges.columns and 'claim charges' in charges.columns:
    charges['gcr'] = charges['payment'] / charges['claim charges']

# 4. NCR = (Payments - Denials) / Charges
if {'payment', 'denial amount', 'claim charges'}.issubset(charges.columns):
    charges['ncr'] = (charges['payment'] - charges['denial amount']) / charges['claim charges']

# 5. FPR = Claims paid on first submission
if 'denial(yes/no)' in charges.columns:
    charges['fpr'] = (charges['denial(yes/no)'].str.strip().str.upper() == 'NO').astype(int)

# 6. Denial Rate = Denial Amount / Claim Charges
if {'denial amount', 'claim charges'}.issubset(charges.columns):
    charges['denial_rate'] = charges['denial amount'] / charges['claim charges']

# 7. Financial Status = High Risk / OK
if 'claim balance' in charges.columns:
    charges['financial_status'] = charges['claim balance'].apply(
        lambda x: 'High Risk' if x > 1000 else 'OK'
    )

# 8. AR Days
if 'denial age' in pending_ar.columns:
    pending_ar['ar_days'] = pending_ar['denial age']

# 9. 90+ AR Days
if 'ar_days' in pending_ar.columns:
    pending_ar['is_90plus'] = pending_ar['ar_days'] > 90

# === RENAME TO AVOID COLUMN CONFLICTS ===
for df, tag in zip([payments, adjustments], ['payment', 'adjust']):
    df.rename(columns={col: f"{col}_{tag}" for col in df.columns if col != 'account num'}, inplace=True)

# === MERGE FINAL DATA ===
final_df = charges.merge(payments, on='account num', how='left')
final_df = final_df.merge(adjustments, on='account num', how='left')
if 'account num' in pending_ar.columns:
    final_df = final_df.merge(
        pending_ar[['account num', 'ar_days', 'is_90plus']],
        on='account num',
        how='left'
    )

# === EXPORT FINAL CSV ===
final_df.to_csv(output_file, index=False)
print(f"✅ Final CSV saved as: {output_file}")

# === SAVE TO SQLITE ===
conn = sqlite3.connect(db_path)
final_df.to_sql("claims_kpis", conn, if_exists="replace", index=False)
conn.close()
print(f"✅ Data inserted into DB: {db_path}")
