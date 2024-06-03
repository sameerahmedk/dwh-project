import sqlite3
import pandas as pd

# Connect to the star schema database
conn = sqlite3.connect('./star_schema.db')

# SQL query to join fact table with all dimension tables
query = """
SELECT
    f.job_id,
    f.company_id,
    d1.company_name,
    d1.company_size,
    d1.industry,
    f.views,
    f.applies,
    f.remote_allowed,
    f.application_type,
    f.formatted_work_type,
    f.sponsored,
    f.salary_id,
    d2.min_salary,
    d2.med_salary,
    d2.max_salary,
    d2.pay_period,
    d2.currency,
    d2.compensation_type,
    f.salary_variation,
    f.apply_rate,
    f.date_id,
    d3.listed_time,
    d3.Time,
    d3.Date,
    d3.Month,
    d3.Quarter,
    d3.Year
FROM
    FactJobPostings f
JOIN
    DimCompany d1 ON f.company_id = d1.company_id
JOIN
    DimSalaries d2 ON f.salary_id = d2.salary_id
JOIN
    DimDates d3 ON f.date_id = d3.date_id
"""

# Execute the query and store the result in a DataFrame
df_snapshot = pd.read_sql_query(query, conn)

# Save the DataFrame to a CSV file
df_snapshot.to_csv('./fact_table_snapshot.csv', index=False)

conn.close()
