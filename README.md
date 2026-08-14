# Sales Medallion Pipeline

End-to-end sales data pipeline built with **PySpark** and the **Medallion
Architecture** (Bronze → Silver → Gold) on **Databricks**, using **Delta
Lake** for storage.

The project takes a raw, intentionally messy sales dataset (~25,000 rows
across Customers, Products, Orders, and Transactions) and transforms it into
a clean, analytics-ready **star schema**, then answers a set of real
business questions directly off that schema.

## Architecture

```
Raw CSV
   │
   ▼
┌─────────────┐     ┌─────────────────┐     ┌──────────────────┐
│   BRONZE    │ --> │      SILVER      │ --> │       GOLD        │
│  Raw ingest │     │ Clean & validate │     │  Star schema +     │
│  (as-is)    │     │                  │     │  business queries  │
└─────────────┘     └─────────────────┘     └──────────────────┘
```

| Layer  | Table(s)                                                                 | What happens |
|--------|---------------------------------------------------------------------------|--------------|
| Bronze | `workspace.bronze.bronze_sales`                                          | Raw CSV loaded into Delta, untouched |
| Silver | `workspace.silver.silver_sales`                                         | Type casting, dedup, null handling, status validation, address/phone parsing, email validation, date features |
| Gold   | `dim_customers`, `dim_products`, `dim_location`, `dim_date`, `fact_sales`, `fact_transactions` | Star schema in `workspace.gold` |

## Project structure

```
Sales-Medallion-Pipeline/
├── README.md
├── requirements.txt
├── .gitignore
├── data/
│   ├── raw/                          # raw_sales_dataset.csv goes here
│   └── sample/                       # small sample for quick local testing
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   ├── 03_gold_dimensional_model.py
│   └── 04_gold_business_analytics.py
├── docs/
│   └── data_dictionary.md
└── images/                           # screenshots / diagrams
```

Notebooks are exported in **Databricks source format** (`# Databricks
notebook source` header, `# COMMAND ----------` cell markers), so they can
be committed as plain `.py` files, reviewed in a normal diff, and re-imported
straight back into a Databricks workspace as notebooks (**Workspace → Import
→ File**, or via the Databricks CLI).

## Silver layer — cleaning steps

- Safe type casting (`try_cast`, `try_to_date`) so bad values become `null`
  instead of breaking the pipeline
- Deduplication
- Null handling: meaningful defaults for descriptive fields, hard drop for
  rows missing critical numeric/date fields
- Order & transaction status validation against an accepted value list
- Address column split into house number, street, city, state, country
- Country-aware phone number formatting (India, USA/Canada, UK, Germany,
  France, Australia, Brazil)
- Email validation via regex
- Date feature engineering: year, month, day, day-of-week, quarter, weekend
  flag

## Gold layer — star schema

**Dimensions:** `dim_customers`, `dim_products`, `dim_location`, `dim_date`
**Facts:** `fact_sales`, `fact_transactions`

## Business questions answered

- Which country generates the most revenue?
- Who are the top 5 customers by total spending?
- How did sales trend month by month in 2024?
- Which product category drives the most revenue?
- Do weekdays or weekends bring in more sales?
- What is the most sold product in each country? (window function)
- Which quarter had the highest revenue in 2024?

## Tech stack

PySpark · Databricks · Delta Lake · SQL · Python · Medallion Architecture

## Running this project

1. Upload `data/raw/sales_dataset.csv` to a Databricks Volume or DBFS path
   and update the path in `01_bronze_ingestion.py` if needed.
2. Import the four notebooks from `notebooks/` into a Databricks workspace.
3. Run them in order: `01` → `02` → `03` → `04`.

## What I learned

Building this end-to-end — from raw ingestion through to analytics-ready
Gold tables — was the most useful part of learning the Medallion pattern in
practice. The Silver layer was the hardest piece to get right, especially
multi-country phone formatting and writing type casting that degrades
gracefully on bad data instead of crashing the job.
