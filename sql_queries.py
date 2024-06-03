import sqlite3
import pandas as pd

db_path = "C:/Users/HP/Documents/DWH/star_schema.db"

try:
    conn = sqlite3.connect(db_path)
except sqlite3.OperationalError as e:
    print(f"Error: {e}")
    exit()

queries = {
    "Query 1": """
        SELECT
            c.company_name,
            SUM(f.views) AS total_views,
            SUM(f.applies) AS total_applications
        FROM
            FactJobPostings f
        JOIN
            DimCompany c ON f.company_id = c.company_id
        GROUP BY
            c.company_name;
    """,
    "Query 2": """
        SELECT
            job_id,
            apply_rate
        FROM
            FactJobPostings
        WHERE
            remote_allowed = TRUE;
    """,
    "Query 3": """
        SELECT
            d.Month AS month,
            AVG(s.min_salary) AS average_min_salary,
            AVG(s.max_salary) AS average_max_salary
        FROM
            FactJobPostings f
        JOIN
            DimDates d ON f.date_id = d.date_id
        JOIN
            DimSalaries s ON f.salary_id = s.salary_id
        GROUP BY
            d.Month;
    """,
    "Query 4": """
        SELECT
            c.industry,
            COUNT(f.job_id) AS total_job_postings,
            SUM(f.views) AS total_views
        FROM
            FactJobPostings f
        JOIN
            DimCompany c ON f.company_id = c.company_id
        GROUP BY
            c.industry;
    """,
    "Query 5": """
        SELECT
            job_id,
            apply_rate,
            s.min_salary,
            s.max_salary
        FROM
            FactJobPostings f
        JOIN
            DimSalaries s ON f.salary_id = s.salary_id
        ORDER BY
            apply_rate DESC
        LIMIT 10;
    """,
}

# Execute the queries and store results
results = {}
for query_name, query in queries.items():
    results[query_name] = pd.read_sql_query(query, conn)

# Print results to the terminal
for query_name, result in results.items():
    print(f"\n{query_name}:\n")
    print(result)

conn.close()
