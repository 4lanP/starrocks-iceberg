import os
import json
import time
import random
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

DATA_DIR = os.path.join(os.path.dirname(__file__), "storage")
BRONZE_DIR = os.path.join(DATA_DIR, "bronze")
SILVER_DIR = os.path.join(DATA_DIR, "silver")
ICEBERG_META_DIR = os.path.join(SILVER_DIR, "metadata")
CSV_RAW_DIR = os.path.join(DATA_DIR, "raw_csv")

os.makedirs(BRONZE_DIR, exist_ok=True)
os.makedirs(SILVER_DIR, exist_ok=True)
os.makedirs(ICEBERG_META_DIR, exist_ok=True)
os.makedirs(CSV_RAW_DIR, exist_ok=True)

def generate_initial_data(num_records=100000):
    print(f"[INFO] Generating {num_records} synthetic e-commerce records for Bronze Layer...")
    
    cities = ["Ha Noi", "Ho Chi Minh", "Da Nang", "Can Tho", "Hai Phong", "Nha Trang", "Hue"]
    categories = ["Electronics", "Clothing", "Home & Kitchen", "Books", "Beauty", "Sports"]
    payment_methods = ["Credit Card", "E-Wallet", "Bank Transfer", "COD"]
    
    np.random.seed(42)
    
    start_date = datetime(2026, 1, 1)
    
    records = []
    for i in range(1, num_records + 1):
        order_id = f"ORD-{i:07d}"
        customer_id = f"CUST-{random.randint(1000, 9999)}"
        city = random.choice(cities)
        category = random.choice(categories)
        amount = round(random.uniform(10.0, 1500.0), 2)
        quantity = random.randint(1, 5)
        payment = random.choice(payment_methods)
        created_at = start_date + timedelta(seconds=random.randint(0, 86400 * 180))
        status = "COMPLETED" if random.random() > 0.1 else "PENDING"
        
        records.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "city": city,
            "category": category,
            "amount": amount,
            "quantity": quantity,
            "payment_method": payment,
            "status": status,
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S"),
            "updated_at": created_at.strftime("%Y-%m-%d %H:%M:%S")
        })
        
    df = pd.DataFrame(records)
    
    # Save as CSV for traditional comparison
    csv_path = os.path.join(CSV_RAW_DIR, "sales_raw.csv")
    df.to_csv(csv_path, index=False)
    
    # Save as Bronze Parquet
    bronze_path = os.path.join(BRONZE_DIR, "bronze_sales.parquet")
    df.to_parquet(bronze_path, index=False, engine="pyarrow", compression="snappy")
    
    print(f"[OK] Generated raw CSV ({os.path.getsize(csv_path) / (1024*1024):.2f} MB)")
    print(f"[OK] Generated Bronze Parquet ({os.path.getsize(bronze_path) / (1024*1024):.2f} MB)")
    return df

if __name__ == "__main__":
    generate_initial_data(100000)
