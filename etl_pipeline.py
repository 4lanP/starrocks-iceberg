import os
import json
import time
import datetime
import duckdb
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog

BASE_DIR = os.path.dirname(__file__)
DATA_DIR = os.path.join(BASE_DIR, "storage")
BRONZE_PATH = os.path.join(DATA_DIR, "bronze", "bronze_sales.parquet")
SILVER_DIR = os.path.join(DATA_DIR, "silver")

class IcebergLakehouseEngine:
    def __init__(self):
        os.makedirs(SILVER_DIR, exist_ok=True)
        self.warehouse_path = os.path.abspath(SILVER_DIR).replace("\\", "/")
        self.db_path = os.path.join(self.warehouse_path, "catalog.db").replace("\\", "/")
        
        self.catalog = load_catalog("local", **{
            "type": "sql",
            "uri": f"sqlite:///{self.db_path}",
            "warehouse": self.warehouse_path
        })
        self.catalog.create_namespace_if_not_exists("default")
        
    def get_table(self):
        try:
            return self.catalog.load_table("default.silver_sales")
        except Exception:
            return None

    def run_etl_bronze_to_silver_v1(self):
        print("[ETL Engine] Starting Bronze to Silver Transformation (Real Iceberg Snapshot 1)...")
        con = duckdb.connect()
        
        df_silver = con.execute(f"""
            SELECT 
                CAST(order_id AS VARCHAR) as order_id,
                CAST(customer_id AS VARCHAR) as customer_id,
                CAST(UPPER(city) AS VARCHAR) as city,
                CAST(category AS VARCHAR) as category,
                CAST(amount AS DOUBLE) as amount,
                CAST(quantity AS BIGINT) as quantity,
                CAST(ROUND(amount * quantity, 2) AS DOUBLE) as total_revenue,
                CAST(payment_method AS VARCHAR) as payment_method,
                CAST(status AS VARCHAR) as status,
                CAST(created_at AS VARCHAR) as created_at,
                CAST(updated_at AS VARCHAR) as updated_at,
                CAST(1 AS INTEGER) as version
            FROM read_parquet('{BRONZE_PATH.replace('\\', '/')}')
            WHERE status = 'COMPLETED'
        """).df()

        arrow_table = pa.Table.from_pandas(df_silver)

        try:
            self.catalog.drop_table("default.silver_sales")
        except Exception:
            pass

        table = self.catalog.create_table("default.silver_sales", schema=arrow_table.schema)
        table.append(arrow_table)

        snapshots = table.snapshots()
        snap1 = snapshots[-1]
        print(f"[OK] Iceberg Snapshot 1 Created! ID: {snap1.snapshot_id} | Records: {len(df_silver)}")
        return snap1

    def run_etl_upsert_v2(self):
        print("[ETL Engine] Applying CDC Update/Upsert Data (Real Iceberg Snapshot 2)...")
        table = self.get_table()
        if table is None or not table.snapshots():
            self.run_etl_bronze_to_silver_v1()
            table = self.get_table()

        df_existing = table.scan().to_pandas()

        df_updated = df_existing.copy()
        mask = (df_updated['category'] == 'Electronics') & (df_updated['city'] == 'HA NOI')
        df_updated.loc[mask, 'amount'] = df_updated.loc[mask, 'amount'] * 0.5
        df_updated.loc[mask, 'total_revenue'] = (df_updated.loc[mask, 'amount'] * df_updated.loc[mask, 'quantity']).round(2)
        df_updated.loc[mask, 'updated_at'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df_updated.loc[mask, 'version'] = 2

        arrow_updated = pa.Table.from_pandas(df_updated)
        table.overwrite(arrow_updated)

        snapshots = table.snapshots()
        snap2 = snapshots[-1]
        print(f"[OK] Iceberg Snapshot 2 Created! ID: {snap2.snapshot_id} | Total Snapshots in History: {len(snapshots)}")
        return snap2

if __name__ == "__main__":
    from generate_data import generate_initial_data
    generate_initial_data(100000)
    engine = IcebergLakehouseEngine()
    engine.run_etl_bronze_to_silver_v1()
    engine.run_etl_upsert_v2()
