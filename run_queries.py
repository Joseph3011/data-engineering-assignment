import duckdb
import os

# Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
WAREHOUSE_PATH = os.path.join(BASE_DIR, "warehouse")   
OUTPUT_PATH = os.path.join(BASE_DIR, "outputs")

os.makedirs(OUTPUT_PATH, exist_ok=True)

# DuckDB connection
con = duckdb.connect()

#Register CSVs as tables
con.execute(f"""
    CREATE OR REPLACE TABLE dim_customers AS 
    SELECT * FROM read_csv_auto('{WAREHOUSE_PATH}/dim_customers.csv');
""")

con.execute(f"""
    CREATE OR REPLACE TABLE dim_products AS 
    SELECT * FROM read_csv_auto('{WAREHOUSE_PATH}/dim_products.csv');
""")

con.execute(f"""
    CREATE OR REPLACE TABLE dim_date AS 
    SELECT * FROM read_csv_auto('{WAREHOUSE_PATH}/dim_date.csv');
""")

con.execute(f"""
    CREATE OR REPLACE TABLE fact_orders AS 
    SELECT * FROM read_csv_auto('{WAREHOUSE_PATH}/fact_orders.csv');
""")

# Load queries.sql
sql_file = os.path.join(BASE_DIR, "queries.sql")
with open(sql_file, "r") as f:
    sql_text = f.read()

# Split queries by ";" 
queries = [q.strip() for q in sql_text.split(";") if q.strip()]

# Run each query and save result
for i, query in enumerate(queries, start=1):
    result = con.execute(query).df()
    output_file = os.path.join(OUTPUT_PATH, f"query_{i}.csv")
    result.to_csv(output_file, index=False)
    print(f"Query {i} executed → {output_file}")

con.close()
