import sys
from etl_pipeline import IcebergLakehouseEngine
import pyarrow as pa
import pyarrow.compute as pc

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

def main():
    print("=" * 70)
    print("[TEST 1] DEMO XOA DU LIEU VA TIME TRAVEL IN 5 DONG DAU BANG ICEBERG")
    print("=" * 70)

    engine = IcebergLakehouseEngine()
    table = engine.get_table()

    if table is None:
        print("[!] Chua tim thay bang Iceberg! Hay chay 'python etl_pipeline.py' truo'c.")
        return

    snaps_initial = table.snapshots()
    first_snap = snaps_initial[0]
    
    arrow_first = table.scan(snapshot_id=first_snap.snapshot_id).to_arrow()
    df_first_top5 = arrow_first.slice(0, 5).to_pandas()
    
    target_order_id = df_first_top5.iloc[0]['order_id']
    target_revenue = df_first_top5.iloc[0]['total_revenue']

    print(f"\n[1] 5 DONG DAU TIEN CUA SNAPSHOT BAN DAU (Snapshot #{first_snap.snapshot_id}):")
    print("-" * 70)
    print(df_first_top5[['order_id', 'customer_id', 'city', 'category', 'total_revenue']].to_string(index=False))
    print("-" * 70)

    print(f"\n[*] Don hang se bi XOA THU trong Snapshot moi: Order ID '{target_order_id}' (${target_revenue:,.2f})")

    arrow_current = table.scan().to_arrow()
    mask = pc.not_equal(arrow_current['order_id'], target_order_id)
    arrow_deleted = arrow_current.filter(mask)
    
    print("\n[...] Dang thuc hien xoa don hang va commit Snapshot moi...")
    table.overwrite(arrow_deleted)

    table = engine.get_table()
    snaps_after = table.snapshots()
    new_snap = snaps_after[-1]

    print(f"[OK] Xoa va tao Snapshot moi thanh cong! Snapshot ID = {new_snap.snapshot_id}")

    arrow_latest = table.scan(snapshot_id=new_snap.snapshot_id).to_arrow()
    df_latest_top5 = arrow_latest.slice(0, 5).to_pandas()
    
    print(f"\n[2] 5 DONG DAU TIEN CUA SNAPSHOT MOI (Snapshot #{new_snap.snapshot_id} - DA XOA '{target_order_id}'):")
    print("-" * 70)
    print(df_latest_top5[['order_id', 'customer_id', 'city', 'category', 'total_revenue']].to_string(index=False))
    print("-" * 70)

    arrow_old_history = table.scan(snapshot_id=first_snap.snapshot_id).to_arrow()
    df_old_top5 = arrow_old_history.slice(0, 5).to_pandas()

    print(f"\n[3] 5 DONG DAU TIEN CUA SNAPSHOT CU QUA TIME TRAVEL (Snapshot #{first_snap.snapshot_id} - VAN CON '{target_order_id}'):")
    print("-" * 70)
    print(df_old_top5[['order_id', 'customer_id', 'city', 'category', 'total_revenue']].to_string(index=False))
    print("-" * 70)

    print("\n" + "=" * 70)
    print("[SUCCESS] Snapshot moi va Snapshot cu deu duoc in 5 dong dau chuan xac!")
    print("=" * 70)

if __name__ == "__main__":
    main()
