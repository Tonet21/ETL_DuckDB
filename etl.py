import os
import duckdb

MD_TOKEN = os.getenv("MD_TOKEN")
con = duckdb.connect(f"md:?motherduck_token={MD_TOKEN}")

con.sql("CREATE SCHEMA IF NOT EXISTS analytics;")

con.sql(
    """
CREATE OR REPLACE TABLE raw_salaries AS
SELECT
    work_year,
    experience_level,
    employment_type,
    job_title,
    salary,
    salary_currency,
    salary_in_usd,
    employee_residence,
    remote_ratio,
    company_location,
    company_size
FROM my_db.salaries;
"""
)

con.sql(
    """
CREATE OR REPLACE TABLE analytics.avg_salary_year_exp AS
SELECT
    work_year,
    experience_level,
    ROUND(AVG(salary_in_usd), 2) AS avg_usd_salary
FROM raw_salaries
GROUP BY work_year, experience_level
ORDER BY work_year, experience_level;
"""
)

##con.sql("SELECT * FROM analytics.avg_salary_year_exp LIMIT 5").show()
## It shows (on terminal) the first 5 entries of the table we just created.

df_avg = con.sql("SELECT * FROM analytics.avg_salary_year_exp").df()
df_avg["avg_salary_k"] = (
    df_avg["avg_usd_salary"] / 1_000
)  ##Crating a new row of salaries on thousands of dollars.

##print(df_avg.head())

con.register("pandas_avg_salary", df_avg)

con.sql(
    """
CREATE OR REPLACE TABLE analytics.avg_salary_year_exp_pandas AS
SELECT
  work_year,
  experience_level,
  avg_salary_k
FROM pandas_avg_salary
WHERE avg_salary_k > 100
ORDER BY avg_salary_k DESC
"""
)

##con.sql("SELECT * FROM analytics.avg_salary_year_exp_pandas LIMIT 5").show()
