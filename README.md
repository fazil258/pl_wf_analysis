# 🏢 P&L Workforce Analysis

> **Medallion Architecture · Databricks · Unity Catalog · Delta Lake · PySpark**
>
> Consolidated P&L and HR analytics platform for FY 2025 — TechNova Industries & Nova AI

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Architecture](#-architecture)
- [Repository Structure](#-repository-structure)
- [Prerequisites](#-prerequisites)
- [Quick Start](#-quick-start)
- [Notebooks](#-notebooks)
- [KPIs & Curated Views](#-kpis--curated-views)
- [Unity Catalog Schema](#-unity-catalog-schema)
---

## 🎯 Project Overview

**Global Tech Holdings** owns two technology subsidiaries and requires a consolidated financial reporting platform for fiscal year 2025.

| Company | ID | Industry | Country |
|---|---|---|---|
| TechNova Industries | 1 | Technology | USA |
| Nova AI | 2 | Technology | USA |

### What This Project Does

- Ingests raw financial and HR data from an Excel workbook into Delta Lake tables
- Cleans, transforms, and models the data through a 5-layer Medallion architecture
- Builds a star-schema dimensional warehouse (4 dims + 2 facts)
- Exposes 10 analytical KPI views covering P&L reporting and HR cost tracking
- Orchestrates the full pipeline via Databricks Workflows with automated DQ checks

### Platform

Originally developed on **Microsoft Fabric**, fully migrated to **Databricks** with Unity Catalog and ADLS Gen2 storage.

---

## 🏗 Architecture

```
Source (Excel)
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE RAW          bronze_catalog.raw                         │
│  Raw Delta tables — all columns as StringType, audit cols added │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE CLEAN        bronze_catalog.clean                       │
│  Type casting · null handling · date parsing · deduplication    │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER              silver_catalog.transform                   │
│  Column drops · FullName derivation · net_amount · fiscal labels│
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  MART                gold_catalog.mart                          │
│  dim_account · dim_department · dim_employee · dim_company      │
│  fact_general_ledger · fact_payroll                             │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│  CURATED             gold_catalog.curated                       │
│  10 KPI views — P&L analytics · HR cost tracking                │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
                         Power BI / Tableau
```

### Storage Layout

```
├── bronze/raw/                  ← Raw Delta tables
├── bronze/clean/                ← Cleaned Delta tables
├── silver/transform             ← Transformed tables
├── gold/mart/dim/               ← Dimension tables
├── gold/mart/fact/              ← Fact tables
├── gold/curated/                ← KPI materialisations
└──gold/datacube                 ← Creating Data Cube
```

---

## 📁 Repository Structure

```
/mini_project_pl_wf_analysis
│
├── bronze/
│   └── nb_bronze_clean.ipynb        # Ingestion, Type casting & null handling
├── silver/
│   └──nb_silver.ipynb               # Business transforms & feature engineering
├── gold/
│   ├── nb_gold_mart.ipynb           # Dimensional warehouse build
|   ├── nb_gold_datacube.ipynb       # For creaing data cubes
│   └── nb_curated.py                # 10 KPI analytical views
│
└── README.md
```

---



## ✅ Prerequisites

| Requirement | Version / Detail |
|---|---|
| Databricks Runtime | 14.3 LTS (Spark 3.5.0, Scala 2.12) |
| Python | 3.10+ |
| Azure Storage | ADLS Gen2 storage account with `medallion` container |
| Unity Catalog | Enabled on Databricks workspace |
| Databricks CLI | v0.200+ (`pip install databricks-cli`) |
| Git | Any recent version |

### Required Secrets (Databricks Secret Scope: `adls-scope`)

```bash
databricks secrets create-scope --scope adls-scope

databricks secrets put --scope adls-scope --key storage-account-name
databricks secrets put --scope adls-scope --key sp-client-id
databricks secrets put --scope adls-scope --key sp-client-secret
databricks secrets put --scope adls-scope --key sp-tenant-id
```

---

## 🚀 Quick Start

### 1. Clone the repo into Databricks Repos

```bash
# In Databricks UI: Repos → Add Repo
# Or via CLI:
databricks repos create \
  --url https://github.com/fazil258/pl_wf_analysis \
  --provider gitHub
```

### 2. Set up Unity Catalog schemas

Run the setup SQL once in a Databricks SQL notebook or the SQL editor:

```sql
CREATE CATALOG IF NOT EXISTS catalog_gtf_holdings;

CREATE SCHEMA IF NOT EXISTS bronze.catalog_raw;
CREATE SCHEMA IF NOT EXISTS bronze_catalog.clean;
CREATE SCHEMA IF NOT EXISTS silver_catalog.transform;
CREATE SCHEMA IF NOT EXISTS gold_catalog.mart;
CREATE SCHEMA IF NOT EXISTS gold_catalog.curated;
```

### 3. Upload source data

```python
# Upload the Excel file to DBFS
dbutils.fs.cp("file:/local/path/pl_wf_dataset.xlsx",
              "dbfs:/FileStore/uploads/pl_wf_dataset.xlsx")
```

### 4. Run notebooks in order

```
nb_bronze_clean → nb_silver → nb_mart → nb_curated 
```


```

### 5. Verify curated views

```sql
SHOW VIEWS IN catalog_gtf_holdings.curated;
SELECT * FROM catalog_gtf_holdings.curated.vw_net_profit_by_month LIMIT 10;
```

---

## 📓 Notebooks


### `nb_bronze_clean` — Raw Ingestion & Data Cleaning
- Writes to `bronze/raw/<table>` and registers in `bronze_catalog.raw`
- Parses dates with format `dd-MM-yyyy HH:mm`
- Writes to `bronze/clean/<table>` and registers in `bronze_catalog.clean`

### `nb_silver` — Transformation
- Drops unused columns (`account_code`, `employee_code`, `department_code`, `reference_number`, `description`)
- Derives `full_name = concat_ws(" ", first_name, last_name)` for employees
- Adds `net_amount = credit_amount - debit_amount` to general ledgers
- Adds `total_compensation` and `total_deductions` to payroll
- Adds `fiscal_month_label` (e.g. `2025-01`) for grouping
- Writes to `silver/<table>` and registers in `silver_catalog.transform`


### `nb_mart` — Dimensional Warehouse
Builds the star schema from Silver tables:

| Table | Type | Key Derivations |
|---|---|---|
| `dim_account` | Dimension | `pl_section` (Revenue/COGS/OpEx/Tax) · `opex_category` by account range |
| `dim_department` | Dimension | — |
| `dim_employee` | Dimension | — |
| `dim_company` | Dimension | — |
| `fact_general_ledger` | Fact | Posted entries only · joined to `dim_account` for `pl_section` · partitioned by `fiscal_year, company_id` |
| `fact_payroll` | Fact | `fiscal_year` and `fiscal_period` derived from `pay_date` · partitioned by `fiscal_year, company_id` |

### `nb_curated` — KPI Views
Creates 10 SQL views in `gold_catalog.curated` using `CREATE OR REPLACE VIEW`. See [KPIs section](#-kpis--curated-views) below.



## 📈 KPIs & Curated Views

All views live in `gold_catalog.curated`:

| # | View Name | Description | Key Formula |
|---|---|---|---|
| 1 | `vw_monthly_consolidated_revenue` | Total revenue by month with MoM growth % | `SUM(credit_amount) WHERE pl_section='Revenue'` + `LAG()` |
| 2 | `vw_cost_of_sales_by_month` | COGS per company per month | `SUM(debit_amount) WHERE pl_section='COGS'` |
| 3 | `vw_gross_profit_margin` | Gross profit margin % per company per month | `(Revenue - COGS) / Revenue * 100` |
| 4 | `vw_opex_breakdown` | Operating expenses by category and company | `SUM(debit_amount) WHERE pl_section='Operating Expense'` grouped by `opex_category` |
| 5 | `vw_avg_compensation_by_position` | Mean total compensation by position level | `AVG(total_compensation)` grouped by `position, company_id` |
| 6 | `vw_net_profit_by_month` | Bottom-line profitability per month | `Revenue - COGS - OpEx - Tax` |
| 7 | `vw_overtime_bonus_analysis` | Variable pay by department with OT/base ratio | `SUM(overtime_pay) / SUM(gross_salary) * 100` |
| 8 | `vw_cost_per_department` | Total payroll cost per department | `SUM(total_compensation)` per department |
| 9 | `vw_headcount_by_department` | Active employee count by department | `COUNT(*) WHERE is_active = TRUE` |
| 10 | `vw_payroll_cost_pct_revenue` | Labor efficiency ratio per month | `SUM(payroll) / SUM(revenue) * 100` |

### Account ID Ranges (P&L Classification)

```
4000–4999  →  Revenue
5000–5999  →  COGS
6000–6099  →  OpEx: Personnel
6100–6199  →  OpEx: Occupancy
6200–6299  →  OpEx: Technology
6300–6399  →  OpEx: Marketing
6400–6499  →  OpEx: Travel & Entertainment
6500–6599  →  OpEx: Professional Fees
6700–6799  →  OpEx: Financial Charges
6800–6899  →  OpEx: Depreciation & Amortisation
7000–7099  →  OpEx: R&D
7100–7199  →  Tax
1000–3999  →  Balance Sheet (not in P&L)
```

---

## 🗂 Unity Catalog Schema

```
catalog_gtf_holdings
│
├── bronze_raw
│   ├── accounts            (157 rows)
│   ├── company             (2 rows)
│   ├── departments         (20 rows)
│   ├── employee            (2,000 rows)
│   ├── general_ledgers     (20,000 rows)
│   ├── payroll             (8,000 rows)
│   └── meta_data           (6 rows)
│
├── bronze_clean
│   └── (same 6 tables, typed columns)
│
├── silver
│   └── (same 6 tables, derived columns added)
│
├── mart
│   ├── dim_account         (157 rows — includes pl_section, opex_category)
│   ├── dim_department      (20 rows)
│   ├── dim_employee        (2,000 rows — includes full_name)
│   ├── dim_company         (2 rows)
│   ├── fact_general_ledger (~20,000 Posted rows — partitioned by fiscal_year, company_id)
│   └── fact_payroll        (~8,000 rows — partitioned by fiscal_year, company_id)
│
├── curated
|   └── (10 SQL views — see KPIs table above)
|
└── datacube
    └──Combination of dim and fact table
```

---

## ⚙️ Databricks Workflow


```
bronze_raw → bronze_clean → silver → mart → curated → datacube
```

