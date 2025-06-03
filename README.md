# ETL_DuckDB

# ETL Exploration with DuckDB and MotherDuck

This project explores the basics of setting up an ETL (Extract, Transform, Load) pipeline using Python, DuckDB, and MotherDuck. It is based on a tutorial from KDNuggets, adapted and expanded to deepen my understanding of the ETL process, SQL schemas, and the importance of using DataFrames during transformation.

N.B Make sure to add your MotherDuck access token as a secret.
---

##  Why ETL Is Important

**ETL (Extract, Transform, Load)** is the foundation of modern data workflows. It's how raw data gets turned into clean, structured, and usable formats for analysis, reporting, or machine learning.

- **Extract**: Pull data from source systems (e.g., APIs, databases, files).
- **Transform**: Clean, reshape, or enrich the data for analysis.
- **Load**: Store the processed data into a system where it can be queried or visualized.

ETL is essential because raw data is rarely ready to use. Without ETL, analysis would be unreliable, inconsistent, and error-prone.

---

##  Why I Use DataFrames (Pandas)

When pulling data from DuckDB into Python, I convert it into a **Pandas DataFrame** like this:

```python
df = con.sql("SELECT * FROM analytics.avg_salary_year_exp").df()

```

This allows me to:

- Perform flexible, complex transformations that are hard in pure SQL.

- Use rich Python libraries for cleaning, manipulation, and visualization.

- Prepare data for machine learning workflows, which often expect a DataFrame.

Later, I can register that DataFrame as a table again in DuckDB to write more SQL queries on it:
```python

con.register("pandas_avg_salary", df)

```

Once the DataFrame is registered, it acts as temporary table inside the database and I can interact with it using SQL.

## What Is a Schema in SQL?

In SQL, a schema is like a namespace or folder inside a database that groups related tables and views. Example:
```sql

CREATE SCHEMA analytics;

CREATE TABLE analytics.avg_salary_year_exp (...);
```

Schemas are important because they:

 - Keep data organized (e.g., analytics.employees, sales.transactions)

 - Allow permission control (e.g., restrict access to certain schemas)

 - Avoid naming conflicts (you can have analytics.users and auth.users)

Using schemas helps scale a database cleanly and logically as projects grow.
