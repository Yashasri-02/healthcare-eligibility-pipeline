import os
import logging
from pathlib import Path
import re
import pandas as pd
from config import PARTNER_CONFIG


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def ensure_output_dir(path: str = "output"):
    Path(path).mkdir(parents=True, exist_ok=True)


def format_phone(phone):
    if pd.isna(phone):
        return None

    phone = str(phone)
    digits = re.sub(r"\D", "", phone)

    if len(digits) == 10:
        return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
    return None


def standardize_dataframe(df: pd.DataFrame, partner_conf: dict) -> tuple:
    """Standardize partner dataframe.

    Returns a tuple: (standardized_rows_df, dropped_rows_df).
    Dropped rows include missing external_id or unparseable dob.
    """
    # Rename columns using mapping (partner -> standard)
    df = df.rename(columns=partner_conf["column_mapping"])

    # Ensure all expected columns exist to avoid KeyErrors later
    for col in ["external_id", "first_name", "last_name", "dob", "email", "phone"]:
        if col not in df.columns:
            df[col] = pd.NA

    # Validate external_id presence. Previously we also dropped bad DOBs;
    # change: only drop rows with missing external_id so DOBs may be empty.
    mask_missing_id = df["external_id"].isna() | df["external_id"].astype(str).str.strip().eq("")

    # Decide dropped rows: only missing id
    dropped_mask = mask_missing_id
    dropped_rows = df[dropped_mask].copy()
    kept = df[~dropped_mask].copy()

    if len(dropped_rows) > 0:
        logging.warning(f"{len(dropped_rows)} row(s) dropped for {partner_conf['partner_code']} (missing external_id)")

    # Transformations on kept rows
    kept["first_name"] = kept["first_name"].astype(str).str.title().replace("Nan", pd.NA)
    kept["last_name"] = kept["last_name"].astype(str).str.title().replace("Nan", pd.NA)
    kept["email"] = kept["email"].astype(str).str.lower().replace("nan", pd.NA)

    # Use parsed_dob only for kept rows and format
    kept["dob"] = pd.to_datetime(kept["dob"], errors="coerce").dt.strftime("%Y-%m-%d")

    kept["phone"] = kept["phone"].apply(format_phone)
    kept["partner_code"] = partner_conf["partner_code"]

    cols = ["external_id", "first_name", "last_name", "dob", "email", "phone", "partner_code"]
    standardized = kept.loc[:, cols]

    # For dropped rows, keep original columns + partner_code for traceability
    if not dropped_rows.empty:
        dropped_rows = dropped_rows.assign(partner_code=partner_conf["partner_code"]) 

    return standardized, dropped_rows


def ingest_partner(partner_name: str, partner_conf: dict) -> tuple:
    file_path = partner_conf["file_path"]
    delimiter = partner_conf.get("delimiter", ",")

    logging.info(f"Reading {partner_name} from {file_path} using delimiter '{delimiter}'")
    try:
        # pandas will handle malformed rows gracefully when on_bad_lines is set (pandas>=1.3)
        df = pd.read_csv(file_path, delimiter=delimiter, dtype=str, on_bad_lines="skip")
    except TypeError:
        # older pandas versions may not support on_bad_lines
        df = pd.read_csv(file_path, delimiter=delimiter, dtype=str)
    except Exception as e:
        logging.error(f"Failed to read {file_path}: {e}")
        empty_cols = ["external_id", "first_name", "last_name", "dob", "email", "phone", "partner_code"]
        return pd.DataFrame(columns=empty_cols), pd.DataFrame(columns=empty_cols)

    # Identify partner's id column (mapping key whose value is 'external_id')
    id_col = None
    for k, v in partner_conf["column_mapping"].items():
        if v == "external_id":
            id_col = k
            break

    if id_col and id_col not in df.columns:
        logging.warning(f"Expected id column '{id_col}' not found in {file_path}. Proceeding, but all rows will be considered missing id.")

    # Standardize and validate
    std_df, dropped_rows = standardize_dataframe(df, partner_conf)

    return std_df, dropped_rows


def main():
    ensure_output_dir("output")

    all_partners = []
    all_dropped = []

    for partner, conf in PARTNER_CONFIG.items():
        logging.info(f"Processing partner: {partner}")
        partner_std, partner_dropped = ingest_partner(partner, conf)
        if partner_std is not None and not partner_std.empty:
            all_partners.append(partner_std)
        if partner_dropped is not None and not partner_dropped.empty:
            all_dropped.append(partner_dropped)

    if all_partners:
        unified_df = pd.concat(all_partners, ignore_index=True)
    else:
        unified_df = pd.DataFrame(columns=["external_id", "first_name", "last_name", "dob", "email", "phone", "partner_code"]) 

    out_path = Path("output") / "unified_eligibility.csv"
    unified_df.to_csv(out_path, index=False)

    # Write dropped rows audit
    if all_dropped:
        dropped_df = pd.concat(all_dropped, ignore_index=True)
    else:
        dropped_df = pd.DataFrame(columns=["external_id", "first_name", "last_name", "dob", "email", "phone", "partner_code"]) 

    dropped_path = Path("output") / "dropped_rows.csv"
    dropped_df.to_csv(dropped_path, index=False)

    logging.info(f"Pipeline completed successfully. Wrote {len(unified_df)} rows to {out_path}")
    logging.info(f"Wrote {len(dropped_df)} dropped rows to {dropped_path}")

    # Print a small sample for quick verification
    print(unified_df.head(50).to_string(index=False))


if __name__ == "__main__":
    main()
