# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------

# DBTITLE 1,Cell 3
df_acc=spark.table('bronze_catalog.raw.accounts')
df_comp=spark.table('bronze_catalog.raw.company')
df_dept=spark.table('bronze_catalog.raw.departments')
df_emp=spark.table('bronze_catalog.raw.employee')
df_gen_led=spark.table('bronze_catalog.raw.general_ledgers')
df_pay=spark.table('bronze_catalog.raw.payroll')


# COMMAND ----------

df_acc.show()

# COMMAND ----------

df_emp=df_emp.withColumn("hire_date",to_date(col("hire_date"),"dd-MM-yyyy HH:mm"))
df_emp=df_emp.withColumn("termination_date",to_date(col("termination_date"),"dd-MM-yyyy HH:mm"))

# COMMAND ----------

df_emp.select('hire_date','termination_date').show()

# COMMAND ----------

df_gen_led=df_gen_led.withColumn("entry_date",to_date(col("entry_date"),"dd-MM-yyyy HH:mm"))
df_gen_led=df_gen_led.withColumn("posting_date",to_date(col("posting_date"),"dd-MM-yyyy HH:mm"))

# COMMAND ----------

df_gen_led.select('entry_date','posting_date').show()

# COMMAND ----------

df_pay= df_pay.withColumn("pay_period_start",to_date(col("pay_period_start"),"dd-MM-yyyy HH:mm"))
df_pay= df_pay.withColumn("pay_period_end",to_date(col("pay_period_end"),"dd-MM-yyyy HH:mm"))
df_pay=df_pay.withColumn("pay_date",to_date(col("pay_date"),"dd-MM-yyyy HH:mm"))

# COMMAND ----------

df_pay.select('pay_period_start','pay_period_end','pay_date').show()

# COMMAND ----------

# DBTITLE 1,Cell 11
df_acc.write.mode('overwrite').saveAsTable('bronze_catalog.clean.accounts')
df_comp.write.mode('overwrite').saveAsTable('bronze_catalog.clean.company')
df_dept.write.mode('overwrite').saveAsTable('bronze_catalog.clean.departments')
df_emp.write.mode('overwrite').saveAsTable('bronze_catalog.clean.employee')
df_gen_led.write.mode('overwrite').saveAsTable('bronze_catalog.clean.general_ledgers')
df_pay.write.mode('overwrite').saveAsTable('bronze_catalog.clean.payroll')

# COMMAND ----------

