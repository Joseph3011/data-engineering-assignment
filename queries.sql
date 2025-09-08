-- Top 5 customers by total spend
SELECT c.name, SUM(f.total_amount) AS total_spent
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.name
ORDER BY total_spent DESC
LIMIT 5;

-- Monthly revenue trend
SELECT d.year, d.month, SUM(f.total_amount) AS monthly_revenue
FROM fact_orders f
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

-- Best selling product category
SELECT p.category, SUM(f.quantity) AS total_sold
FROM fact_orders f
JOIN dim_products p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sold DESC
LIMIT 1;

-- Revenue by city
SELECT c.city, SUM(f.total_amount) AS revenue
FROM fact_orders f
JOIN dim_customers c ON f.customer_id = c.customer_id
GROUP BY c.city
ORDER BY revenue DESC;
