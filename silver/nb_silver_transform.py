# Databricks notebook source
df_acc=spark.table('bronze_catalog.clean.accounts')
df_comp=spark.table('bronze_catalog.clean.company')
df_dept=spark.table('bronze_catalog.clean.departments')
df_emp=spark.table('bronze_catalog.clean.employee')
df_gen_led=spark.table('bronze_catalog.clean.general_ledgers')
df_pay=spark.table('bronze_catalog.clean.payroll')


# COMMAND ----------

from pyspark.sql import functions as F
df_acc=df_acc.drop('account_code')
df_emp=df_emp.drop('employee_code')
df_dept=df_dept.drop('department_code')
df_gen_led=df_gen_led.drop('reference_number')
df_gen_led= df_gen_led.drop('description')


# COMMAND ----------

df_acc.show()

# COMMAND ----------

df_emp=df_emp.withColumn("FullName",F.concat_ws(" ","first_name","last_name"))
df_emp =df_emp.drop("first_name")
df_emp=df_emp.drop("last_name")

# COMMAND ----------

df_gen_led=df_gen_led.withColumn(
    "net_amount",
    F.col("credit_amount") - F.col("debit_amount")
)

df_gen_led=df_gen_led.withColumn(
    "fiscal_month_label",
    F.concat(
        F.col("fiscal_year").cast("string"),
        F.lit("-"),
        F.lpad(F.col("fiscal_period").cast("string"),2,"0")
    )
)

# COMMAND ----------

df_pay=df_pay.drop("payment_method")
 
df_pay=df_pay.withColumn(
    "total_compensation",
    F.col("gross_salary") + F.col("bonus") + F.col("overtime_pay")
    + F.col("commission") + F.col("allowances")
)
 
df_pay=df_pay.withColumn(
    "total_deductions",
    F.col("tax_deduction") + F.col("social_security") + F.col("health_insurance")
    + F.col("retirement_contribution") + F.col("other_deductions")
)

df_pay=df_pay.withColumn("fiscal_year",  F.year(F.col("pay_date")))
df_pay=df_pay.withColumn("fiscal_period",F.month(F.col("pay_date")))
df_pay=df_pay.withColumn(
    "fiscal_month_label",
    F.concat(
        F.col("fiscal_year").cast("string"),
        F.lit("-"),
        F.lpad(F.col("fiscal_period").cast("string"),2,"0")
    )
)

# COMMAND ----------

df_acc.write.mode('overwrite').saveAsTable('silver_catalog.transform.accounts')
df_comp.write.mode('overwrite').saveAsTable('silver_catalog.transform.company')
df_dept.write.mode('overwrite').saveAsTable('silver_catalog.transform.departments')
df_emp.write.mode('overwrite').saveAsTable('silver_catalog.transform.employee')
df_gen_led.write.mode('overwrite').saveAsTable('silver_catalog.transform.general_ledgers')
df_pay.write.mode('overwrite').saveAsTable('silver_catalog.transform.payroll')