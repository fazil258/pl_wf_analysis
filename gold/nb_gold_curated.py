# Databricks notebook source
spark.read.table("gold_catalog.mart.dim_account").createOrReplaceTempView("dim_account")
spark.read.table("gold_catalog.mart.dim_departments").createOrReplaceTempView("dim_departments")
spark.read.table("gold_catalog.mart.dim_employee").createOrReplaceTempView("dim_employee")
spark.read.table("gold_catalog.mart.dim_company").createOrReplaceTempView("dim_company")
spark.read.table("gold_catalog.mart.fact_gl").createOrReplaceTempView("fact_general_ledger")
spark.read.table("gold_catalog.mart.fact_payroll").createOrReplaceTempView("fact_payroll")

# COMMAND ----------

df_kpi1=spark.sql("""
    WITH monthly_rev AS (
        SELECT
            fiscal_year,
            fiscal_period,
            fiscal_month_label,
            SUM(credit_amount) AS total_revenue
        FROM fact_general_ledger
        WHERE pl_section='Revenue'
        AND status    ='Posted'
        GROUP BY fiscal_year,fiscal_period,fiscal_month_label
    )
    SELECT
        fiscal_year,
        fiscal_period,
        fiscal_month_label,
        ROUND(total_revenue,2) AS total_revenue,
        ROUND(LAG(total_revenue) OVER (ORDER BY fiscal_year,fiscal_period),2) AS prev_month_revenue,
        ROUND((total_revenue-LAG(total_revenue) OVER (ORDER BY fiscal_year,fiscal_period))/NULLIF(LAG(total_revenue) OVER (ORDER BY fiscal_year,fiscal_period),0)* 100,2) AS mom_growth_pct
    FROM monthly_rev
    ORDER BY fiscal_year,fiscal_period
""")
 
df_kpi1.createOrReplaceTempView("vw_monthly_consolidated_revenue")
print("KPI 1 — Monthly Consolidated Revenue")
df_kpi1.show(12)

# COMMAND ----------

df_kpi2=spark.sql("""
    SELECT
        g.fiscal_year,
        g.fiscal_period,
        g.fiscal_month_label,
        g.company_id,
        c.company_name,
        ROUND(SUM(g.debit_amount),2) AS cost_of_sales
    FROM fact_general_ledger g
    JOIN dim_company c ON g.company_id=c.company_id
    WHERE g.pl_section='COGS'
      AND g.status    ='Posted'
    GROUP BY
        g.fiscal_year,g.fiscal_period,g.fiscal_month_label,
        g.company_id,c.company_name
    ORDER BY g.fiscal_year,g.fiscal_period,g.company_id
""")
 
df_kpi2.createOrReplaceTempView("vw_cost_of_sales_by_month")
print("KPI 2 — Cost of Sales by Month")
df_kpi2.show(24)


# COMMAND ----------

df_kpi3=spark.sql("""
    SELECT
        g.fiscal_year,
        g.fiscal_period,
        g.fiscal_month_label,
        g.company_id,
        c.company_name,
        ROUND(SUM(CASE WHEN g.pl_section='Revenue' THEN g.credit_amount ELSE 0 END),2)
            AS revenue,
        ROUND(SUM(CASE WHEN g.pl_section='COGS' THEN g.debit_amount  ELSE 0 END),2)
            AS cogs,
        ROUND(SUM(CASE WHEN g.pl_section='Revenue' THEN g.credit_amount ELSE 0 END)
           -SUM(CASE WHEN g.pl_section='COGS' THEN g.debit_amount  ELSE 0 END),2)
            AS gross_profit,
        ROUND(
            (SUM(CASE WHEN g.pl_section='Revenue' THEN g.credit_amount ELSE 0 END)
          -SUM(CASE WHEN g.pl_section='COGS' THEN g.debit_amount  ELSE 0 END))
           /NULLIF(SUM(CASE WHEN g.pl_section='Revenue' THEN g.credit_amount ELSE 0 END),0)
           *100,
        2) AS gross_profit_margin_pct
    FROM fact_general_ledger g
    JOIN dim_company c ON g.company_id=c.company_id
    WHERE g.status='Posted'
      AND g.pl_section IN ('Revenue','COGS')
    GROUP BY
        g.fiscal_year,g.fiscal_period,g.fiscal_month_label,
        g.company_id,c.company_name
    ORDER BY g.fiscal_year,g.fiscal_period,g.company_id
""")
 
df_kpi3.createOrReplaceTempView("vw_gross_profit_margin")
print("KPI 3 — Monthly Gross Profit Margin")
df_kpi3.show(24)


# COMMAND ----------

df_kpi4=spark.sql("""
    SELECT
        g.fiscal_year,
        g.fiscal_period,
        g.fiscal_month_label,
        g.company_id,
        c.company_name,
        COALESCE(g.opex_category,'Other') AS opex_category,
        ROUND(SUM(g.debit_amount),2)      AS opex_amount
    FROM fact_general_ledger g
    JOIN dim_company c ON g.company_id=c.company_id
    WHERE g.pl_section='Operating Expense'
      AND g.status    ='Posted'
    GROUP BY
        g.fiscal_year,g.fiscal_period,g.fiscal_month_label,
        g.company_id,c.company_name,
        COALESCE(g.opex_category,'Other')
    ORDER BY g.fiscal_year,g.fiscal_period,g.company_id,opex_amount DESC
""")
 
df_kpi4.createOrReplaceTempView("vw_opex_breakdown")
print("KPI 4 — Operating Expense Breakdown")
df_kpi4.show()

# COMMAND ----------

df_kpi5=spark.sql("""
    SELECT
        p.company_id,
        c.company_name,
        e.position,
        COUNT(DISTINCT p.employee_id)              AS employee_count,
        ROUND(AVG(p.total_compensation),2)        AS avg_total_compensation,
        ROUND(AVG(p.gross_salary),2)              AS avg_gross_salary,
        ROUND(AVG(p.bonus),2)                     AS avg_bonus,
        ROUND(AVG(p.overtime_pay),2)              AS avg_overtime,
        ROUND(AVG(p.commission),2)                AS avg_commission,
        ROUND(AVG(p.allowances),2)                AS avg_allowances
    FROM fact_payroll p
    JOIN dim_employee e ON p.employee_id=e.employee_id
    JOIN dim_company  c ON p.company_id =c.company_id
    GROUP BY p.company_id,c.company_name,e.position
    ORDER BY p.company_id,e.position
""")
 
df_kpi5.createOrReplaceTempView("vw_avg_compensation_by_position")
print("KPI 5 — Average Compensation by Position")
df_kpi5.show()


# COMMAND ----------

df_kpi6=spark.sql("""
    WITH pl_segments AS (
        SELECT
            fiscal_year,
            fiscal_period,
            fiscal_month_label,
            company_id,
            SUM(CASE WHEN pl_section='Revenue' THEN credit_amount ELSE 0 END) AS revenue,
            SUM(CASE WHEN pl_section='COGS' THEN debit_amount  ELSE 0 END) AS cogs,
            SUM(CASE WHEN pl_section='Operating Expense' THEN debit_amount  ELSE 0 END) AS opex,
            SUM(CASE WHEN pl_section='Tax' THEN debit_amount  ELSE 0 END) AS tax_expense
        FROM fact_general_ledger
        WHERE status='Posted'
        GROUP BY fiscal_year,fiscal_period,fiscal_month_label,company_id
    )
    SELECT
        p.fiscal_year,
        p.fiscal_period,
        p.fiscal_month_label,
        p.company_id,
        c.company_name,
        ROUND(p.revenue,2) AS revenue,
        ROUND(p.cogs,2) AS cogs,
        ROUND(p.revenue-p.cogs,2) AS gross_profit,
        ROUND(p.opex,2) AS operating_expenses,
        ROUND(p.revenue-p.cogs-p.opex,2) AS ebit,
        ROUND(p.tax_expense,2) AS tax_expense,
        ROUND(p.revenue-p.cogs-p.opex-p.tax_expense,2) AS net_profit,
        ROUND(
            (p.revenue-p.cogs-p.opex-p.tax_expense)
           /NULLIF(p.revenue,0)*100,
        2) AS net_profit_margin_pct
    FROM pl_segments p
    JOIN dim_company c ON p.company_id=c.company_id
    ORDER BY p.fiscal_year,p.fiscal_period,p.company_id
""")
 
df_kpi6.createOrReplaceTempView("vw_net_profit_by_month")
print("KPI 6 — Net Profit by Month")
df_kpi6.show(12)


# COMMAND ----------

df_kpi7=spark.sql("""
    SELECT
        p.company_id,
        c.company_name,
        p.department_id,
        d.department_name,
        ROUND(SUM(p.overtime_pay),2) AS total_overtime_pay,
        ROUND(SUM(p.bonus),2)   AS total_bonus,
        ROUND(SUM(p.gross_salary),2) AS total_gross_salary,
        ROUND(SUM(p.commission),2) AS total_commission,
        ROUND(
            SUM(p.overtime_pay)
           /NULLIF(SUM(p.gross_salary),0)*100,
        2) AS overtime_to_base_ratio_pct,
        ROUND(
            SUM(p.bonus)
           /NULLIF(SUM(p.gross_salary),0)*100,
        2) AS bonus_to_base_ratio_pct,
        COUNT(DISTINCT p.employee_id) AS employee_count
    FROM fact_payroll p
    JOIN dim_departments d ON p.department_id=d.department_id
    JOIN dim_company c ON p.company_id=c.company_id
    GROUP BY p.company_id,c.company_name,p.department_id,d.department_name
    ORDER BY p.company_id,total_overtime_pay DESC
""")
 
df_kpi7.createOrReplaceTempView("vw_overtime_bonus_analysis")
print("KPI 7 — Overtime and Bonus Analysis")
df_kpi7.show()


# COMMAND ----------

df_kpi8=spark.sql("""
    SELECT
        p.company_id,
        c.company_name,
        p.department_id,
        d.department_name,
        d.cost_center,
        ROUND(SUM(p.total_compensation),2) AS total_compensation_cost,
        ROUND(SUM(p.gross_salary),2) AS total_gross_salary,
        ROUND(SUM(p.bonus),2) AS total_bonus,
        ROUND(SUM(p.overtime_pay),2) AS total_overtime,
        ROUND(SUM(p.commission),2) AS total_commission,
        ROUND(SUM(p.allowances),2) AS total_allowances,
        COUNT(DISTINCT p.employee_id) AS employee_count,
        ROUND(SUM(p.total_compensation)
             /NULLIF(COUNT(DISTINCT p.employee_id),0),2) AS cost_per_employee
    FROM fact_payroll p
    JOIN dim_departments d ON p.department_id=d.department_id
    JOIN dim_company    c ON p.company_id   =c.company_id
    GROUP BY
        p.company_id,c.company_name,p.department_id,
        d.department_name,d.cost_center
    ORDER BY p.company_id,total_compensation_cost DESC
""")
 
df_kpi8.createOrReplaceTempView("vw_cost_per_department")
print("KPI 8 — Cost per Department")
df_kpi8.show()


# COMMAND ----------

df_kpi9=spark.sql("""
    SELECT
        e.company_id,
        c.company_name,
        e.department_id,
        d.department_name,
        d.cost_center,
        COUNT(*)                         AS active_headcount,
        SUM(CASE WHEN e.position='Junior' THEN 1 ELSE 0 END) AS junior_count,
        SUM(CASE WHEN e.position='Senior' THEN 1 ELSE 0 END) AS senior_count,
        SUM(CASE WHEN e.position='Manager' THEN 1 ELSE 0 END) AS manager_count,
        ROUND(AVG(e.base_salary),2)     AS avg_base_salary,
        ROUND(SUM(e.base_salary),2)     AS total_annual_base_cost
    FROM dim_employee e
    JOIN dim_departments d ON e.department_id=d.department_id
    JOIN dim_company c ON e.company_id=c.company_id
    WHERE e.is_active=TRUE
    GROUP BY
        e.company_id,c.company_name,
        e.department_id,d.department_name,d.cost_center
    ORDER BY e.company_id,active_headcount DESC
""")
 
df_kpi9.createOrReplaceTempView("vw_headcount_by_department")
print("KPI 9 — Headcount Distribution by Department")
df_kpi9.show()

# COMMAND ----------

df_kpi10=spark.sql("""
    WITH revenue_monthly AS (
        SELECT
            company_id,
            fiscal_year,
            fiscal_period,
            fiscal_month_label,
            SUM(credit_amount) AS total_revenue
        FROM fact_general_ledger
        WHERE pl_section='Revenue'
          AND status='Posted'
        GROUP BY company_id,fiscal_year,fiscal_period,fiscal_month_label
    ),
    payroll_monthly AS (
        SELECT
            company_id,
            fiscal_year,
            fiscal_period,
            fiscal_month_label,
            SUM(total_compensation) AS total_payroll_cost,
            COUNT(DISTINCT employee_id) AS headcount
        FROM fact_payroll
        GROUP BY company_id,fiscal_year,fiscal_period,fiscal_month_label
    )
    SELECT
        r.fiscal_year,
        r.fiscal_period,
        r.fiscal_month_label,
        r.company_id,
        c.company_name,
        ROUND(r.total_revenue,2) AS total_revenue,
        ROUND(p.total_payroll_cost,2) AS total_payroll_cost,
        p.headcount,
        ROUND(p.total_payroll_cost
             /NULLIF(r.total_revenue,0)*100,2) AS payroll_pct_of_revenue,
        ROUND(r.total_revenue
             /NULLIF(p.headcount,0),2) AS revenue_per_employee
    FROM revenue_monthly r
    JOIN payroll_monthly p
        ON  r.company_id=p.company_id
        AND r.fiscal_year=p.fiscal_year
        AND r.fiscal_period=p.fiscal_period
    JOIN dim_company c ON r.company_id=c.company_id
    ORDER BY r.fiscal_year,r.fiscal_period,r.company_id
""")
 
df_kpi10.createOrReplaceTempView("vw_payroll_cost_pct_revenue")
print("KPI 10 — Payroll Cost as % of Company Revenue")
df_kpi10.show(12)
