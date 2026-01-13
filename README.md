# Healthcare Eligibility Pipeline

## Overview
This project implements a configuration-driven data ingestion pipeline that processes healthcare eligibility files from multiple partners with different file formats and schemas. The pipeline standardizes the data into a unified output schema ready for downstream consumption.

The solution demonstrates scalable ingestion by ensuring that new partners can be added through configuration updates without changing core processing logic.
# Healthcare Eligibility Pipeline

This repository contains a small, configuration-driven pipeline that ingests member eligibility files from multiple partners, normalizes each partner's schema into a common format, and produces a single unified CSV ready for downstream use.

The design goal is simplicity and scalability: onboarding a new partner should require only a configuration change (no code changes to the core pipeline).

---

## What this project does

- Reads partner files that may use different delimiters and column names.
- Maps partner-specific columns to a standardized schema.
- Normalizes data (names, dates, emails, phone formatting) and tags each row with the source `partner_code`.
- Validates basic requirements (drops rows missing a partner-provided ID).
- Writes a single output file: `output/unified_eligibility.csv`.

## Files you will care about

- `config.py` — Partner configuration dictionary. Each partner entry contains `file_path`, `delimiter`, `partner_code`, and `column_mapping` (partner column → standard field).
- `eligibility_pipeline.py` — The pipeline script. It reads files according to `config.py`, standardizes rows, validates them, and writes the unified CSV.
- `data/` — Sample inputs (e.g., `acme.txt`, `bettercare.csv`).
- `output/` — The pipeline writes `unified_eligibility.csv` here.

## Standardized output schema

The pipeline produces rows with the following columns (and transformations):

- `external_id` — mapped from partner's unique ID field (required). Rows with no `external_id` are dropped.
- `first_name` — Title Case (e.g., `John`).
- `last_name` — Title Case.
- `dob` — Date of birth in ISO-8601 format (`YYYY-MM-DD`). Invalid or unparseable dates become empty.
- `email` — Lowercase.
- `phone` — Formatted as `XXX-XXX-XXXX` when possible.
- `partner_code` — The identifier for the source partner (from `config.py`).

## Quick start — run the pipeline

1. Make sure you have Python 3.8+ installed and a working virtual environment. Install dependencies:

```powershell
pip install pandas
```

2. From the project root, run:

```powershell
python eligibility_pipeline.py
```

3. The pipeline writes `output/unified_eligibility.csv`. A short preview of the output is printed to the console after the run.

If you are using the included virtual environment, you can run the exact Python executable created for the project (example):

```powershell
C:/Users/yasha/Desktop/healthcare_pipeline/.venv/Scripts/python.exe eligibility_pipeline.py
```

## Adding a new partner

To onboard a new partner, edit `config.py` and add a new top-level entry (no changes to `eligibility_pipeline.py` are required). Each partner entry should include:

- `file_path`: Relative path to the input file.
- `delimiter`: The file delimiter (for example `"|"` or `","`).
- `partner_code`: Short identifier used to tag rows.
- `column_mapping`: A mapping of partner column names to the standardized field names used above. Example:

```python
"NEWPARTNER": {
	"file_path": "data/newpartner.txt",
	"delimiter": "|",
	"partner_code": "NEWPARTNER",
	"column_mapping": {
		"their_id": "external_id",
		"given": "first_name",
		"family": "last_name",
		"birthdate": "dob",
		"email_addr": "email",
		"mobile": "phone"
	}
}
```

After saving `config.py`, re-run `eligibility_pipeline.py` and the new partner will be processed automatically.

## Validation & error handling

- Rows missing `external_id` are dropped and logged (this prevents creating records without a unique identifier).
- Date parsing uses `pandas.to_datetime(..., errors='coerce')` so malformed dates won't crash the job — they become empty values.
- When possible, malformed CSV rows are skipped (pandas uses `on_bad_lines='skip'` when available).

These choices keep the pipeline robust for small test inputs; for production usage you may want to: write invalid/dropped rows to an audit file, add stricter column-level validation, or fail-fast based on policy.

## Output example

A successful run against the provided samples writes `output/unified_eligibility.csv` with rows similar to:

```
external_id,first_name,last_name,dob,email,phone,partner_code
1234567890A,John,Doe,1955-03-15,john.doe@email.com,555-123-4567,ACME
BC-001,Alice,Johnson,1965-08-10,alice.j@test.com,555-222-3333,BETTERCARE
```

## Suggestions & next steps

- Persist dropped or rejected rows to an `output/` audit file for downstream review.
- Add unit tests for the transformation logic (happy path, missing id, malformed dates, phone variants).
- For production scale, move to a distributed engine (Spark / Databricks) and keep `config.py` as the single source of partner metadata.

---

If you want, I can also add a small test suite or produce an audit CSV of dropped rows. Tell me which you'd prefer and I'll implement it.

