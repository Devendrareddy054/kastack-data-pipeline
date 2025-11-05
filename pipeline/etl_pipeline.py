import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/kastack_db")
DATA_DIR = "data"

def run_etl():
    # Read all CSVs
    customers = pd.read_csv(f"{DATA_DIR}/customers.csv")
    orders = pd.read_csv(f"{DATA_DIR}/orders.csv", parse_dates=[
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"
    ])
    items = pd.read_csv(f"{DATA_DIR}/order_items.csv", parse_dates=["shipping_limit_date"])
    payments = pd.read_csv(f"{DATA_DIR}/order_payments.csv")
    
    # MERGE all sources into one DataFrame
    orders_cust = orders.merge(customers, on="customer_id", how="left")
    orders_items = orders_cust.merge(items, on="order_id", how="left")
    full = orders_items.merge(payments, on="order_id", how="left")
    
    # CLEAN data
    full = full.drop_duplicates()
    for col in ["customer_city", "customer_state", "payment_type"]:
        if col in full.columns:
            full[col] = full[col].fillna("Unknown")
    for col in ["price", "payment_value"]:
        if col in full.columns:
            full[col] = full[col].fillna(0)
    
    # LOAD to PostgreSQL
    engine = create_engine(DB_URL)
    full.to_sql("orders_full", engine, if_exists="replace", index=False)
    
    # SALES SUMMARY (by customer & region, from orders_full)
    query_sales = """
    DROP TABLE IF EXISTS sales_summary;
    CREATE TABLE sales_summary AS
    SELECT
        customer_unique_id,
        customer_city,
        customer_state,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(price) AS total_sales
    FROM orders_full
    GROUP BY customer_unique_id, customer_city, customer_state;
    """

    # DELIVERY PERFORMANCE SUMMARY (from orders_full)
    query_delivery = """
    DROP TABLE IF EXISTS delivery_performance;
    CREATE TABLE delivery_performance AS
    SELECT
        customer_state,
        COUNT(DISTINCT order_id) AS order_count,
        AVG(EXTRACT(DAY FROM (order_delivered_customer_date - order_purchase_timestamp))) AS avg_delivery_days
    FROM orders_full
    WHERE order_delivered_customer_date IS NOT NULL
    GROUP BY customer_state;
    """
    
    with engine.begin() as conn:
        conn.execute(text(query_sales))
        conn.execute(text(query_delivery))

if __name__ == "__main__":
    run_etl()
