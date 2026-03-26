# Databricks notebook source
df_acc=spark.table('silver_catalog.transform.accounts')
df_comp=spark.table('silver_catalog.transform.company')
df_dept=spark.table('silver_catalog.transform.departments')
df_emp=spark.table('silver_catalog.transform.employee')
df_gen_led=spark.table('silver_catalog.transform.general_ledgers')
df_pay=spark.table('silver_catalog.transform.payroll')

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df_dim_account=df_acc.withColumn(
    "pl_section",
    F.when(
        (F.col("account_id") >= 4000) & (F.col("account_id") < 5000),
        F.lit("Revenue")
    ).when(
        (F.col("account_id") >= 5000) & (F.col("account_id") < 6000),
        F.lit("COGS")
    ).when(
        (F.col("account_id") >= 6000) & (F.col("account_id") < 7100),
        F.lit("Operating Expense")
    ).when(
        (F.col("account_id") >= 7100) & (F.col("account_id") < 7200),
        F.lit("Tax")
    ).otherwise(
        F.lit("Balance Sheet")
    )
).withColumn(
    "opex_category",
    F.when(
        (F.col("account_id") >= 6000) & (F.col("account_id") < 6100),
        F.lit("Personnel")
    ).when(
        (F.col("account_id") >= 6100) & (F.col("account_id") < 6200),
        F.lit("Occupancy")
    ).when(
        (F.col("account_id") >= 6200) & (F.col("account_id") < 6300),
        F.lit("Technology")
    ).when(
        (F.col("account_id") >= 6300) & (F.col("account_id") < 6400),
        F.lit("Marketing")
    ).when(
        (F.col("account_id") >= 6400) & (F.col("account_id") < 6500),
        F.lit("Travel & Entertainment")
    ).when(
        (F.col("account_id") >= 6500) & (F.col("account_id") < 6600),
        F.lit("Professional Fees")
    ).when(
        (F.col("account_id") >= 6600) & (F.col("account_id") < 6700),
        F.lit("Communications")
    ).when(
        (F.col("account_id") >= 6700) & (F.col("account_id") < 6800),
        F.lit("Financial Charges")
    ).when(
        (F.col("account_id") >= 6800) & (F.col("account_id") < 6900),
        F.lit("Depreciation & Amortisation")
    ).when(
        (F.col("account_id") >= 6900) & (F.col("account_id") < 7000),
        F.lit("Other Operating")
    ).when(
        (F.col("account_id") >= 7000) & (F.col("account_id") < 7100),
        F.lit("R&D")
    ).otherwise(F.lit(None))
)

df_dim_account.write.mode("overwrite").saveAsTable("gold_catalog.mart.dim_account")

# COMMAND ----------

df_dim_departments=df_dept.select(
    "department_id",
    "department_name",
    "cost_center"
)
df_dim_departments.write.mode("overwrite").saveAsTable("gold_catalog.mart.dim_departments")

# COMMAND ----------

df_dim_employee=df_emp.select(
    "employee_id",
    "FullName",
    "email",
    "department_id",
    "company_id",
    "position",
    "hire_date",
    "termination_date",
    "base_salary",
    "is_active"
)
df_dim_employee.write.mode("overwrite").saveAsTable("gold_catalog.mart.dim_employee")

# COMMAND ----------

# DBTITLE 1,Cell 6
df_dim_acc_ref=spark.table("gold_catalog.mart.dim_account").select("account_id","pl_section","opex_category","account_type","account_name")


# COMMAND ----------

df_fact_gl=(
    df_gen_led
    .filter(F.col("status") == "Posted")
    .join(df_dim_acc_ref,on="account_id",how="left")
    .select(
        "gl_id",
        "entry_date",
        "posting_date",
        "fiscal_year",
        "fiscal_period",
        "fiscal_month_label",
        "company_id",
        "account_id",
        "department_id",
        "transaction_type",
        "debit_amount",
        "credit_amount",
        "net_amount",
        "pl_section",
        "opex_category",
        "account_type",
        "account_name",
        "status",
        "created_by",
        "approved_by"
    )
)
df_fact_gl.write.mode("overwrite").saveAsTable("gold_catalog.mart.fact_gl")

# COMMAND ----------

df_fact_payroll=df_pay.select(
    "payroll_id",
    "employee_id",
    "company_id",
    "department_id",
    "pay_period_start",
    "pay_period_end",
    "pay_date",
    "fiscal_year",
    "fiscal_period",
    "fiscal_month_label",
    "gross_salary",
    "bonus",
    "overtime_pay",
    "commission",
    "allowances",
    "total_compensation",
    "tax_deduction",
    "social_security",
    "health_insurance",
    "retirement_contribution",
    "other_deductions",
    "total_deductions",
    "net_salary",
    "status"
)
df_fact_payroll.write.mode("overwrite").saveAsTable("gold_catalog.mart.fact_payroll")

# COMMAND ----------

df_dim_company=df_comp.select(
    "company_id",
    "company_name",
    "industry",
    "country",
    "established_date",
    "is_active"
)
df_dim_company.write.mode("overwrite").saveAsTable("gold_catalog.mart.dim_company")