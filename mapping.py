import pandas as pd
from rapidfuzz import process, fuzz

# -------- Sheet-specific mapping --------
sheet_mappings = {
    "Charges Jul'25": {
        "Account Num": "Claim No",
        "Svc Date": "DOS",
        "Batch Date": "Charge Entry Date",
        "Amount": "Billed Amount",
        "Performing Provider": "Provider Name",
        "Insurance": "Payer/Insurance",
        "Group": "Facility Name",
        "FC": "Financial Class"
    },
    "Payment Jul'25": {
        "Account Num": "Claim No",
        "Svc Date": "DOS",
        "Amount": "Paid Amount",
        "Insurance": "Payer/Insurance",
        "Performing Provider": "Provider Name",
        "Batch Date": "Payment Entry Date",
        "Group": "Facility Name",
        "FC": "Financial Class"
    },
    "Adjustment Jul'25": {
        "Account Num": "Claim No",
        "Svc Date": "DOS",
        "Amount": "Adjustment Amount",
        "Description": "Adjustment Description",
        "Insurance": "Payer/Insurance",
        "Performing Provider": "Provider Name",
        "Batch Date": "Adjustment Entry Date",
        "Group": "Facility Name",
        "FC": "Financial Class"
    },
    "Pending AR jul'25": {
        "Account Num": "Claim No",
        "Reg Date": "Charge Entry Date",
        "Amount": "AR Balance",
        "Aging Bucket": "Aging Range",
        "Rcvbl Status": "Financial Status",
        "Insurance": "Payer/Insurance",
        "Performing Provider": "Provider Name",
        "Group": "Facility Name",
        "FC": "Financial Class"
    }
}

# -------- Fuzzy matching helper --------
def fuzzy_match_header(col_name, mapping_keys, score_cutoff=85):
    if col_name in mapping_keys:
        return col_name
    match, score, _ = process.extractOne(   col_name, mapping_keys, scorer=fuzz.token_sort_ratio )
    if score >= score_cutoff:
        return match
    return None

# -------- Normalize headers for one sheet --------
def normalize_sheet_headers(df, mapping):
    # Fuzzy match incoming headers to mapping keys
    new_columns = {}
    for col in df.columns:
        matched_key = fuzzy_match_header(col, mapping.keys())
        if matched_key and matched_key in mapping:
            new_columns[col] = mapping[matched_key]
        else:
            new_columns[col] = col  # Keep original if no match
    df = df.rename(columns=new_columns)
    return df

# -------- Process Excel file --------
def process_excel_with_mapping(file_path):
    excel_file = pd.ExcelFile(file_path)
    standardized_sheets = {}

    for sheet_name in excel_file.sheet_names:
        if sheet_name in sheet_mappings:
            df = excel_file.parse(sheet_name)
            df = normalize_sheet_headers(df, sheet_mappings[sheet_name])
            standardized_sheets[sheet_name] = df
            print(f"[INFO] Processed: {sheet_name} → Columns normalized")
        else:
            print(f"[WARNING] No mapping defined for: {sheet_name}")

    return standardized_sheets

# -------- Merge Sheets if Needed --------
def merge_standardized_sheets(sheets_dict):
    merged_df = pd.concat(sheets_dict.values(), ignore_index=True, sort=False)
    return merged_df

# ==================
# Example Usage:
# ==================
if __name__ == "__main__":
    file_path = "/input/Monthly Data BHS Jul'25.xlsx"
    sheets_data = process_excel_with_mapping(file_path)

    # Example: merge all processed sheets
    merged_data = merge_standardized_sheets(sheets_data)
    print("Merged Data Shape:", merged_data.shape)
    print(merged_data.head())

    # Save for further KPI calculations
    merged_data.to_csv("standardized_kpi_data.csv", index=False)
