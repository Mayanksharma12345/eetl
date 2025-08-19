import os
import time
import sqlite3
import shutil
from datetime import datetime, timedelta
from typing import List, Dict, Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from pydantic import BaseModel
from rapidfuzz import process, fuzz

# RBAC bits (we use only the Permission enum from your rbac.py)
from rbac import Permission

# =========================
# App & Security setup
# =========================
app = FastAPI(title="ETL Processing System with JWT + RBAC")
security = HTTPBearer()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(BASE_DIR, "input")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
ARCHIVE_DIR = os.path.join(BASE_DIR, "archive")
DB_PATH = os.path.join(BASE_DIR, "etl_kpis.db")

for folder in [INPUT_DIR, OUTPUT_DIR, ARCHIVE_DIR]:
    os.makedirs(folder, exist_ok=True)

# IMPORTANT: set as env var in production:  SECRET_KEY="long_random_string"
SECRET_KEY = os.environ.get("SECRET_KEY", "CHANGE_ME_TO_A_LONG_RANDOM_SECRET")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60


# =========================
# JWT helpers (Step 3)
# =========================
def create_access_token(sub: str, username: str, role: str) -> str:
    payload = {
        "sub": sub,
        "username": username,
        "role": role,
        "iat": datetime.utcnow(),
        "exp": datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)


def decode_token(token: str) -> Dict[str, Any] | None:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError:
        return None


class LoginRequest(BaseModel):
    username: str
    password: str


@app.post("/auth/login")
def login(req: LoginRequest):
    """
    Validate credentials from SQLite users table and issue a JWT.
    """
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, username, password_hash, role FROM users WHERE username=?",
            (req.username,),
        )
        row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    user_id, username, password_hash, role = row

    # verify password using passlib[bcrypt]
    from passlib.context import CryptContext

    pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")
    if not pwd_ctx.verify(req.password, password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    token = create_access_token(str(user_id), username, role)
    return {"access_token": token, "token_type": "bearer"}


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict[str, Any]:
    """
    Extract current user from JWT and (optionally) confirm they still exist.
    """
    token = credentials.credentials
    payload = decode_token(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

    # You can re-check the user/role from DB here if you want it authoritative.
    # For performance we’ll trust the token by default:
    return {
        "user_id": payload.get("sub"),
        "username": payload.get("username"),
        "role": payload.get("role"),
    }


# =====================================================
# RBAC layer (Step 4) — map roles to Permission sets
# =====================================================
ROLE_PERMS: Dict[str, set[Permission]] = {
    "user": {
        Permission.READ,
        Permission.VIEW_REPORTS,
    },
    "manager": {
        Permission.READ,
        Permission.VIEW_REPORTS,
        Permission.WRITE,
        Permission.PROCESS_FILES,
    },
    "admin": {
        Permission.READ,
        Permission.VIEW_REPORTS,
        Permission.WRITE,
        Permission.DELETE,
        Permission.ADMIN,
        Permission.PROCESS_FILES,
        Permission.MANAGE_ETL,
    },
}


def require_permissions(*perms: Permission):
    """
    Dependency that enforces the caller's role has all required permissions.
    Usage:
        @app.post("/X")
        def endpoint(user=Depends(require_permissions(Permission.PROCESS_FILES))): ...
    """
    def _dep(user: Dict[str, Any] = Depends(get_current_user)):
        allowed = ROLE_PERMS.get((user.get("role") or "").lower(), set())
        if not set(perms).issubset(allowed):
            raise HTTPException(status_code=403, detail="Not enough permissions")
        return user

    return _dep


# =========================
# ETL utils (Step 5)
# =========================
sheet_mappings = {
    "Charges": {
        "Account Num": "Claim No",
        "Svc Date": "DOS",
        "Batch Date": "Charge Entry Date",
        "Amount": "Billed Amount",
        "Responsible Provider": "Provider Name",
        "Insurance": "Payer/Insurance",
        "Group": "Facility Name",
        "FC": "Financial Class",
    },
    "Payment": {
        "Account Num": "Claim No",
        "Svc Date": "DOS",
        "Amount": "Paid Amount",
        "Insurance": "Payer/Insurance",
        "Responsible Provider": "Provider Name",
        "Batch Date": "Payment Entry Date",
        "Group": "Facility Name",
        "FC": "Financial Class",
    },
    "Adjustment": {
        "Account Num": "Claim No",
        "Svc Date": "DOS",
        "Amount": "Adjustment Amount",
        "Description": "Adjustment Description",
        "Insurance": "Payer/Insurance",
        "Responsible Provider": "Provider Name",
        "Batch Date": "Adjustment Entry Date",
        "Group": "Facility Name",
        "FC": "Financial Class",
    },
    "Pending AR": {
        "Account Num": "Claim No",
        "Reg Date": "Charge Entry Date",
        "Amount": "AR Balance",
        "Aging Bucket": "Aging Range",
        "Rcvbl Status": "Financial Status",
        "Insurance": "Payer/Insurance",
        "Responsible Provider": "Provider Name",
        "Group": "Facility Name",
        "FC": "Financial Class",
    },
}


def fuzzy_match_header(col_name, mapping_keys, score_cutoff=85):
    match, score, _ = process.extractOne(col_name, mapping_keys, scorer=fuzz.token_sort_ratio)
    if score >= score_cutoff:
        return match
    return None


def normalize_headers(df, mapping):
    new_cols = {}
    for col in df.columns:
        match_key = fuzzy_match_header(col, mapping.keys()) or col
        new_cols[col] = mapping.get(match_key, col)
    return df.rename(columns=new_cols)


def safe_merge(left_df, right_df, on_col):
    dup_cols = [c for c in right_df.columns if c in left_df.columns and c != on_col]
    if dup_cols:
        right_df = right_df.drop(columns=dup_cols)
    return left_df.merge(right_df, on=on_col, how="left")


def make_unique_columns(columns):
    seen = {}
    out = []
    for c in columns:
        if c not in seen:
            seen[c] = 0
            out.append(c)
        else:
            seen[c] += 1
            out.append(f"{c}_{seen[c]}")
    return out


def calculate_kpis(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["Paid Amount", "Billed Amount", "Adjustment Amount", "AR Balance"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    if "DOS" in df.columns and "Charge Entry Date" in df.columns:
        df["Charge Lag (days)"] = (df["Charge Entry Date"] - df["DOS"]).dt.days

    if "Charge Entry Date" in df.columns and "Payment Entry Date" in df.columns:
        df["Billing Lag (days)"] = (df["Payment Entry Date"] - df["Charge Entry Date"]).dt.days

    if "Paid Amount" in df.columns and "Billed Amount" in df.columns:
        billed = pd.to_numeric(df["Billed Amount"], errors="coerce").fillna(0)
        paid = pd.to_numeric(df["Paid Amount"], errors="coerce").fillna(0)
        df["GCR (%)"] = ((paid / billed.replace(0, pd.NA)) * 100).fillna(0).round(2)

    if {"Paid Amount", "Billed Amount", "Adjustment Amount"}.issubset(df.columns):
        billed = pd.to_numeric(df["Billed Amount"], errors="coerce").fillna(0)
        adj = pd.to_numeric(df["Adjustment Amount"], errors="coerce").fillna(0)
        paid = pd.to_numeric(df["Paid Amount"], errors="coerce").fillna(0)
        collectible = (billed - adj).replace(0, pd.NA)
        df["NCR (%)"] = ((paid / collectible) * 100).fillna(0).round(2)
        df["CCR (%)"] = ((paid / collectible) * 100).fillna(0).round(2)

    if {"AR Balance", "Billed Amount"}.issubset(df.columns):
        billed_sum = df["Billed Amount"].sum()
        avg_daily_charges = billed_sum / 30 if billed_sum else None
        df["AR Days"] = (df["AR Balance"] / avg_daily_charges).round(1) if avg_daily_charges else None

    if "Aging Range" in df.columns and "AR Balance" in df.columns:
        df["90+ AR Days (%)"] = df.apply(
            lambda x: x["AR Balance"] if "90" in str(x["Aging Range"]) else 0,
            axis=1,
        )

    if "Financial Status" in df.columns:
        total_claims = len(df)
        denied_claims = df["Financial Status"].astype(str).str.contains("denied", case=False, na=False).sum()
        df["Denial Rate (%)"] = (denied_claims / total_claims * 100).round(2) if total_claims else None

    return df


def process_single_file(file_path: str, run_id: str) -> dict:
    start = time.time()
    try:
        with pd.ExcelFile(file_path) as xl:
            processed = {}
            for sheet in xl.sheet_names:
                for key, mapping in sheet_mappings.items():
                    if key.lower() in sheet.lower():
                        df = xl.parse(sheet)
                        df = normalize_headers(df, mapping)
                        for col in df.columns:
                            if "date" in col.lower():
                                df[col] = pd.to_datetime(df[col], errors="coerce")
                        processed[key] = df
                        break

        required = {"Charges", "Payment", "Adjustment", "Pending AR"}
        if not required.issubset(processed):
            missing = required - set(processed)
            raise ValueError(f"Missing required sheets: {', '.join(missing)}")

        merged = processed["Charges"]
        merged = safe_merge(merged, processed["Payment"], "Claim No")
        merged = safe_merge(merged, processed["Adjustment"], "Claim No")
        merged = safe_merge(merged, processed["Pending AR"], "Claim No")

        merged = calculate_kpis(merged)
        merged.columns = [c.lower().strip() for c in merged.columns]
        merged.columns = make_unique_columns(merged.columns)

        out_csv = os.path.join(
            OUTPUT_DIR, f"{os.path.splitext(os.path.basename(file_path))[0]}_with_kpis.csv"
        )
        merged.to_csv(out_csv, index=False)

        with sqlite3.connect(DB_PATH) as conn:
            merged.to_sql(f"claims_with_kpis_{run_id}", conn, if_exists="replace", index=False)

        time.sleep(0.2)
        shutil.move(file_path, os.path.join(ARCHIVE_DIR, os.path.basename(file_path)))

        return {"success": True, "rows": len(merged), "output": out_csv, "elapsed": round(time.time() - start, 2)}
    except Exception as e:
        return {"success": False, "error": str(e)}


# =========================
# Schemas (Step 5)
# =========================
class ProcessingStatus(BaseModel):
    success: bool
    message: str
    files_processed: int
    run_id: str
    processing_time: float


class ETLStats(BaseModel):
    total_files: int
    processed_files: int
    failed_files: int
    last_run_id: str
    available_files: List[str]


# =========================
# Endpoints (Step 5)
# =========================
@app.get("/")
def root():
    return {"message": "ETL Processing System", "status": "running"}


@app.get("/api/user-info")
def user_info(user=Depends(get_current_user)):
    return user


@app.post("/api/process-files", response_model=ProcessingStatus)
def process_files(user=Depends(require_permissions(Permission.PROCESS_FILES))):
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    start = time.time()
    files_processed = 0

    for f in os.listdir(INPUT_DIR):
        if f.lower().endswith(".xlsx"):
            res = process_single_file(os.path.join(INPUT_DIR, f), run_id)
            if res.get("success"):
                files_processed += 1
            else:
                # You could collect & return errors per file; keeping simple here.
                pass

    return ProcessingStatus(
        success=True,
        message=f"Processed {files_processed} file(s)",
        files_processed=files_processed,
        run_id=run_id,
        processing_time=round(time.time() - start, 2),
    )


@app.get("/api/etl-stats", response_model=ETLStats)
def get_etl_stats(user=Depends(require_permissions(Permission.READ))):
    try:
        input_files = [f for f in os.listdir(INPUT_DIR) if f.lower().endswith(".xlsx")]
        output_files = [f for f in os.listdir(OUTPUT_DIR) if f.lower().endswith(".csv")]
        with sqlite3.connect(DB_PATH) as conn:
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)

        last_run_id = "No runs yet"
        if not tables.empty:
            names = tables["name"].tolist()
            run_ids = [n.split("_")[-1] for n in names if n.startswith("claims_with_kpis_")]
            if run_ids:
                last_run_id = sorted(run_ids)[-1]

        return ETLStats(
            total_files=len(input_files),
            processed_files=len(output_files),
            failed_files=0,
            last_run_id=last_run_id,
            available_files=input_files,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to gather stats: {e}")


@app.get("/api/reports")
def get_reports(user=Depends(require_permissions(Permission.VIEW_REPORTS))):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
        return {"available_reports": tables["name"].tolist() if not tables.empty else []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list reports: {e}")


@app.get("/api/reports/{table_name}")
def get_report_data(table_name: str, user=Depends(require_permissions(Permission.VIEW_REPORTS))):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 1000", conn)
        return {
            "table_name": table_name,
            "row_count": len(df),
            "columns": df.columns.tolist(),
            "data": df.to_dict(orient="records"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get report data: {e}")


@app.delete("/api/files/{filename}")
def delete_input_file(filename: str, user=Depends(require_permissions(Permission.DELETE))):
    try:
        path = os.path.join(INPUT_DIR, filename)
        if not os.path.exists(path):
            raise HTTPException(status_code=404, detail="File not found")
        os.remove(path)
        return {"message": f"Deleted {filename}"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete file: {e}")
