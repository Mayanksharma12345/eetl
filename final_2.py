import pandas as pd
import sqlite3
import os
import shutil
import time
from datetime import datetime
from logger import get_logger

# === LOGGER ===
logger = get_logger()
run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
logger.info(f"Starting ETL Run ID: {run_id}")

# === CONFIG ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
ARCHIVE_DIR = os.path.join(BASE_DIR, "archive")
DB_PATH = os.path.join(BASE_DIR, "etl_kpis.db")

# Create folders if missing
for folder in [INPUT_DIR, OUTPUT_DIR, ARCHIVE_DIR]:
    os.makedirs(folder, exist_ok=True)

# === FUNCTIONS ===
def find_sheet(sheets, keyword):
    """Find first sheet containing keyword (case-insensitive)"""
    for sheet in sheets:
        if keyword.lower() in sheet.lower():
            return sheet
    return None

def parse_dates(df, date_cols):
    """Parse dates if columns exist"""
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

# === PROCESS FILES ===
files_processed = 0
for file in os.listdir(INPUT_DIR):
    if file.lower().endswith(".xlsx"):
        start_time = time.time()
        file_path = os.path.join(INPUT_DIR, file)
        logger.info(f"Processing file: {file}")

        # Extract month-year from filename for table naming
        file_tag = os.path.splitext(file)[0].replace(" ", "_").lower()

        try:
            with pd.ExcelFile(file_path) as xl:
                sheets = xl.sheet_names
                logger.info(f"Found sheets: {sheets}")

            # Detect required sheets
            charges_sheet = find_sheet(sheets, "charges")
            payments_sheet = find_sheet(sheets, "payment")
            pending_ar_sheet = find_sheet(sheets, "pending ar")
            adjustments_sheet = find_sheet(sheets, "adjustment")

            if not all([charges_sheet, payments_sheet, pending_ar_sheet, adjustments_sheet]):
                logger.error("Missing one or more required sheets, skipping file.")
                continue

            # Load sheets
            charges = pd.read_excel(file_path, sheet_name=charges_sheet)
            payments = pd.read_excel(file_path, sheet_name=payments_sheet)
            pending_ar = pd.read_excel(file_path, sheet_name=pending_ar_sheet)
            adjustments = pd.read_excel(file_path, sheet_name=adjustments_sheet)

            # Standardize column names
            for df in [charges, payments, pending_ar, adjustments]:
                df.columns = df.columns.str.strip().str.lower()

            # Parse dates
            charges = parse_dates(charges, ['claim date', 'service date', 'payment date'])
            payments = parse_dates(payments, ['payment date'])
            pending_ar = parse_dates(pending_ar, ['dis date', 'due date'])

            # Rename to avoid conflicts
            for df, tag in zip([payments, adjustments], ['payment', 'adjust']):
                df.columns = [f"{col}_{tag}" if col != 'account num' else col for col in df.columns]

            # Merge data
            final_df = charges.merge(payments, on='account num', how='left')
            final_df = final_df.merge(adjustments, on='account num', how='left')

            if 'account num' in pending_ar.columns:
                cols_to_keep = ['account num'] + [
                    col for col in ['dis date', 'due date', 'denial age', 'ar_days']
                    if col in pending_ar.columns
                ]
                final_df = final_df.merge(pending_ar[cols_to_keep], on='account num', how='left')

            # Remove duplicate columns (safety)
            final_df = final_df.loc[:, ~final_df.columns.duplicated()]

            # Export CSV
            output_file = os.path.join(OUTPUT_DIR, f"{file_tag}_clean.csv")
            final_df.to_csv(output_file, index=False)
            logger.info(f"CSV saved: {output_file}")

            # Save to SQLite
            conn = sqlite3.connect(DB_PATH)
            table_name = f"claims_clean_{file_tag}"
            final_df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.close()
            logger.info(f"Data inserted into table: {table_name} in {DB_PATH}")

            # Archive original file
            archive_path = os.path.join(ARCHIVE_DIR, file)
            for attempt in range(3):
                try:
                    shutil.move(file_path, archive_path)
                    logger.info(f"Moved processed file to archive: {file}")
                    break
                except PermissionError:
                    logger.warning(f"⚠ File still in use, retrying in 1s... (Attempt {attempt + 1}/3)")
                    time.sleep(1)
            else:
                logger.error(f"Failed to move file after 3 attempts: {file_path}")

            # Summary for this file
            elapsed = round(time.time() - start_time, 2)
            logger.info(f"Processed {len(final_df):,} rows from {file} in {elapsed}s")
            files_processed += 1

        except Exception as e:
            logger.exception(f"Failed to process file {file}: {e}")

logger.info(f"ETL Run {run_id} completed — {files_processed} file(s) processed")
