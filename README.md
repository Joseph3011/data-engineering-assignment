# 📊 ETL Data Warehouse Assignment

## 📌 Overview
This project implements a **mini data warehouse pipeline** with:

- **ETL** (Extract → Transform → Load) using Python + Pandas  
- **Warehouse layer** stored as CSV dimension and fact tables  
- **Analytics queries** executed with DuckDB  
- **Star schema design** (Fact + Dimensions)  

---

## ⚙️ Setup

### 1. Clone repo
```bash
git clone <repo-url>
cd <repo-folder>
```
## 2. Create virtual environment
```bash
python3 -m venv env
source env/bin/activate   # macOS/Linux
env\Scripts\activate      # Windows
```

## 3. Install dependencies
```bash
pip install -r requirements.txt
```
## 1.Run ETL Pipeline
```bash
python etl_pipeline.py
```

#🚀 Usage
## 2.Run Analytics Queries
```bash
python run_queries.py
```


#🖼️ Star Schema

- Fact Table: fact_orders

- Dimensions: dim_customers, dim_products, dim_date

# Summary

- Built a full ETL pipeline with error handling & transformations

- Designed a Star Schema for analytics

- Integrated DuckDB for SQL queries over CSV data

- Exported analytics results to outputs/
