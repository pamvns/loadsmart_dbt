# Loadsmart Data Challenge

Dimensional data model built with **dbt + BigQuery**, plus a **Jupyter Notebook** with Python utility functions and a data export script.

---

## Project structure

```
loadsmart_dbt/
├── models/
│   ├── staging/
│   │   ├── stg_ae_data.sql          # cleans, casts, and deduplicates the raw seed
│   │   └── _staging.yml             # docs + tests
│   └── marts/
│       ├── dimensions/
│       │   ├── dim_carrier.sql
│       │   ├── dim_carrier.yml
│       │   ├── dim_shipper.sql
│       │   ├── dim_shipper.yml
│       │   ├── dim_lane.sql
│       │   ├── dim_lane.yml
│       │   ├── dim_equipment_type.sql
│       │   └── dim_equipment_type.yml
│       └── facts/
│           ├── fct_loads.sql
│           └── _facts.yml
├── seeds/
│   ├── 2026_data_challenge_ae_data.csv
│   └── schema.yml
├── notebooks/
│   └── data_challenge.ipynb         # Python functions + export script
├── dbt_project.yml
├── packages.yml
├── profiles.yml
└── .env                             # BigQuery credentials (not committed)
```

---

## Star schema

All dimension tables share `loadsmart_id` as their natural key — the same key used in `fct_loads`. There are no surrogate keys; joins are done in Power BI (or the notebook) on `loadsmart_id`.

```
             dim_carrier
             dim_shipper
                         \
                          fct_loads ── dim_lane
                         /
         dim_equipment_type
```

| Layer | Model | Grain | Materialization |
|---|---|---|---|
| Staging | `stg_ae_data` | 1 row / load | View |
| Dimension | `dim_carrier` | 1 row / load | Table |
| Dimension | `dim_shipper` | 1 row / load | Table |
| Dimension | `dim_lane` | 1 row / load | Table |
| Dimension | `dim_equipment_type` | 1 row / load | Table |
| Fact | `fct_loads` | 1 row / load | Table |

---

## Column naming conventions

All staging and fact columns follow a type-suffix convention:

| Suffix | Type | Example |
|---|---|---|
| `_ts` | DATETIME | `booked_ts`, `delivered_ts` |
| `_dt` | DATE | `book_dt`, `delivery_dt` |
| `_nm` | NUMERIC | `book_price_nm`, `pnl_nm` |
| `_fl` | FLOAT64 | `carrier_rating_fl` |
| `_days` | INT64 (days) | `transit_days`, `quote_to_book_days` |

Columns with no suffix are booleans (`is_*`, `was_*`, `has_*`), string identifiers (`loadsmart_id`, `carrier_nm`, `shipper_nm`), or categoricals (`equipment_type`, `sourcing_channel`).

---

## Prerequisites

| Tool | Version |
|---|---|
| Python | 3.9+ |
| dbt-bigquery | 1.10+ |
| Google Cloud account | with BigQuery access |

---

## 1 — BigQuery setup

1. Create a GCP project and enable the BigQuery API.
2. Create a service account with the **BigQuery Data Editor** and **BigQuery Job User** roles.
3. Download the JSON key file.
4. Create a dataset (e.g. `analytics`) in the `US` multi-region.

---

## 2 — Environment setup

```bash
# Clone and enter the repo
git clone <repo-url>
cd loadsmart_dbt

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate

# Install dbt and Python dependencies
pip install dbt-bigquery
pip install google-cloud-bigquery google-cloud-bigquery-storage \
            db-dtypes pandas paramiko pyarrow jupyter
```

---

## 3 — Configure credentials

Copy the example below into a `.env` file at the project root and fill in your values:

```bash
export DBT_PROJECT_ID="your-gcp-project-id"
export DBT_DATASET="analytics"
export DBT_LOCATION="US"
export DBT_TARGET="dev"
export DBT_KEYFILE="/absolute/path/to/service-account-key.json"
```

Load the variables before running dbt:

```bash
source .env
```

---

## 4 — Run the dbt pipeline

```bash
# Install dbt packages
dbt deps

# Load the raw CSV into BigQuery
dbt seed

# Build all models (staging → dimensions → fact)
dbt run

# Run schema tests
dbt test
```

Expected output after `dbt run`:

```
1 of 6  OK  stg_ae_data          [CREATE VIEW]
2 of 6  OK  dim_carrier          [CREATE TABLE  ~4.9k rows]
3 of 6  OK  dim_shipper          [CREATE TABLE  ~5.3k rows]
4 of 6  OK  dim_lane             [CREATE TABLE  ~5.3k rows]
5 of 6  OK  dim_equipment_type   [CREATE TABLE  ~5.3k rows]
6 of 6  OK  fct_loads            [CREATE TABLE  ~5.3k rows]
```

> **Note:** The source CSV contains 4 duplicate `loadsmart_id` rows (identical records). The staging model deduplicates them with `QUALIFY ROW_NUMBER() OVER (PARTITION BY loadsmart_id ...) = 1`, so the final row count is 5,357.

---

## 5 — Run the Jupyter Notebook

```bash
# From the project root (with venv active and .env sourced)
jupyter notebook notebooks/data_challenge.ipynb
```

The notebook contains four sections:

| # | Function | Description |
|---|---|---|
| 1 | `split_lane(lane)` | Parses `"City,ST -> City,ST"` into `pickup_city`, `pickup_state`, `delivery_city`, `delivery_state` |
| 2 | `send_csv_via_email(...)` | Sends a CSV as an email attachment via SMTP/STARTTLS |
| 3 | `send_csv_via_sftp(...)` | Uploads a CSV to a remote sFTP server (via paramiko) |
| 4 | `export_last_month_deliveries(output_path)` | Queries BigQuery and writes `last_month_deliveries.csv` |

Run cells in order. Section 4 requires the dbt models to be built first and the `DBT_KEYFILE` environment variable to be set (or the keyfile path hardcoded in the config cell).

### Email (section 2)

Gmail requires an **App Password** (not your account password). Generate one at: Google Account → Security → 2-Step Verification → App Passwords.

### sFTP (section 3)

Supports both password and private-key authentication. To test locally on macOS, enable **Remote Login** in System Settings → General → Sharing, then connect to `localhost` with your Mac username and password.

### Export output columns (section 4)

| Column | Source table | Source column |
|---|---|---|
| `loadsmart_id` | `fct_loads` | `loadsmart_id` |
| `shipper_nm` | `dim_shipper` | `shipper_nm` |
| `delivery_dt` | `fct_loads` | `delivery_dt` |
| `pickup_city` | `dim_lane` | `origin_city` |
| `pickup_state` | `dim_lane` | `origin_state` |
| `delivery_city` | `dim_lane` | `destination_city` |
| `delivery_state` | `dim_lane` | `destination_state` |
| `book_price_nm` | `fct_loads` | `book_price_nm` |
| `carrier_nm` | `dim_carrier` | `carrier_nm` |

The exported file is filtered to the **last calendar month with available deliveries** in the dataset (derived from `MAX(delivery_dt)`).
