import pandas as pd
from pathlib import Path
import re

# Paths 
RAW_PATH = Path("raw")
OUTPUT_PATH = Path("warehouse")
OUTPUT_PATH.mkdir(exist_ok=True)

# Extract
customers = pd.read_csv(RAW_PATH / "customers_raw.csv", on_bad_lines='skip', quotechar='"')
products = pd.read_csv(RAW_PATH / "products_raw.csv", on_bad_lines='skip', quotechar='"')
orders = pd.read_csv(RAW_PATH / "orders_raw.csv", on_bad_lines='skip', quotechar='"')

# Transform 

# Customers 
customers.drop_duplicates(inplace=True)

# Clean emails: lowercase and strip spaces
customers['email'] = customers['email'].str.lower().str.strip()

# Keep only valid emails (exactly one '@' and at least one '.' after '@')
def is_valid_email(email):
    pattern = r'^[^@]+@[^@]+\.[^@]+$'
    return bool(re.match(pattern, email))

customers = customers[customers['email'].apply(is_valid_email)]
dim_customers = customers

# Products 
products.drop_duplicates(inplace=True)
products['category'] = products['category'].str.lower().str.strip()
dim_products = products

# Orders / Fact 
orders.drop_duplicates(inplace=True)

# Convert quantity to numeric safely
orders['quantity'] = pd.to_numeric(orders['quantity'], errors='coerce')

# Parse dates robustly
orders['order_date'] = pd.to_datetime(orders['order_date'], dayfirst=True, errors='coerce')
orders = orders.dropna(subset=['order_date', 'quantity'])

# Merge with products to get price
orders = orders.merge(dim_products[['product_id', 'price']], on='product_id', how='left')

# Convert price to numeric safely
orders['price'] = pd.to_numeric(orders['price'], errors='coerce')
orders = orders.dropna(subset=['price'])

# Calculate total_amount
orders['total_amount'] = orders['quantity'] * orders['price']

# Dim Date 
dim_date = pd.DataFrame()
dim_date['date'] = pd.to_datetime(orders['order_date'])
dim_date = dim_date.drop_duplicates()
dim_date['date_id'] = dim_date['date'].dt.strftime('%Y%m%d').astype(int)
dim_date['year'] = dim_date['date'].dt.year
dim_date['month'] = dim_date['date'].dt.month
dim_date['day'] = dim_date['date'].dt.day
dim_date['weekday'] = dim_date['date'].dt.day_name()

# Map date_id to orders
orders = orders.merge(dim_date[['date', 'date_id']], left_on='order_date', right_on='date', how='left')
fact_orders = orders[['order_id', 'customer_id', 'product_id', 'date_id', 'quantity', 'total_amount']]

# Load 
dim_customers.to_csv(OUTPUT_PATH / "dim_customers.csv", index=False)
dim_products.to_csv(OUTPUT_PATH / "dim_products.csv", index=False)
dim_date.to_csv(OUTPUT_PATH / "dim_date.csv", index=False)
fact_orders.to_csv(OUTPUT_PATH / "fact_orders.csv", index=False)

# Summary 
print("ETL completed! The warehouse folder now contains all cleaned and transformed CSV files.")
print(f"Dim Customers: {len(dim_customers)} rows")
print(f"Dim Products: {len(dim_products)} rows")
print(f"Dim Date: {len(dim_date)} rows")
print(f"Fact Orders: {len(fact_orders)} rows")
