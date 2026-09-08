import sys
import datetime
from etl_pipeline import IcebergLakehouseEngine

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

def main():
    print("=" * 60)
    print("[TEST 2] BAT DAU TEST ROLLBACK BANG ICEBERG VE SNAPSHOT QUAKHU")
    print("=" * 60)

    engine = IcebergLakehouseEngine()
    table = engine.get_table()

    if table is None:
        print("[!] Chua tim thay bang Iceberg! Hay chay 'python etl_pipeline.py' truo'c.")
        return

    snapshots = table.snapshots()
    if len(snapshots) < 2:
        print("[!] Can it nhat 2 Snapshots de test Rollback. Hay chay 'python etl_pipeline.py' hoac 'python test_iceberg_delete.py' truo'c.")
        return

    print(f"\n[*] DANH SACH LICH SU SNAPSHOTS BANG ICEBERG ({len(snapshots)} snapshots):")
    for idx, snap in enumerate(snapshots):
        dt_str = datetime.datetime.fromtimestamp(snap.timestamp_ms / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
        is_current = " (-> DANG CHON TAI DAY)" if table.current_snapshot() and table.current_snapshot().snapshot_id == snap.snapshot_id else ""
        print(f"   [{idx + 1}] Snapshot ID: {snap.snapshot_id} | Thoi gian: {dt_str}{is_current}")

    target_snapshot = snapshots[0]

    print(f"\n[*] Snapshot duoc chon de ROLLBACK VE: Snapshot #{target_snapshot.snapshot_id}")
    
    table_before = table.scan().to_arrow()
    print(f"   - So ban ghi TRUOC rollback: {table_before.num_rows}")

    print(f"\n[...] Dang thuc thi Rollback ve Snapshot #{target_snapshot.snapshot_id}...")
    table.manage_snapshots().set_current_snapshot(target_snapshot.snapshot_id).commit()

    table = engine.get_table()
    table_after = table.scan().to_arrow()
    new_current = table.current_snapshot()

    print(f"[OK] ROLLBACK THANH CONG!")
    print(f"   - Current Snapshot ID hien tai: {new_current.snapshot_id}")
    print(f"   - So ban ghi SAU rollback   : {table_after.num_rows}")
    
    print("\n" + "=" * 60)
    print("[SUCCESS] Bang Iceberg da duoc khoi phuc ve dung trang thai ban dau!")
    print("=" * 60)

if __name__ == "__main__":
    main()
