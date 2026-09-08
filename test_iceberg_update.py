import sys
import datetime
from etl_pipeline import IcebergLakehouseEngine
import pyarrow as pa
import pyarrow.compute as pc

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')

def main():
    print("=" * 75)
    print("UPDATE DU LIEU 1 DONG VA TIME TRAVEL TRONG ICEBERG")
    print("=" * 75)

    engine = IcebergLakehouseEngine()
    table = engine.get_table()

    if table is None:
        print("Chua tim thay bang Iceberg!")
        return

    snaps_initial = table.snapshots()
    first_snap = snaps_initial[0]

    arrow_current = table.scan().to_arrow()

    # Get first record info using arrow
    target_order_id = arrow_current['order_id'][0].as_py()
    old_amount = arrow_current['amount'][0].as_py()
    old_status = arrow_current['status'][0].as_py()

    # Filter and display first record
    mask = pc.equal(arrow_current['order_id'], target_order_id)
    first_record = arrow_current.filter(mask)
    df_first = first_record.to_pandas()

    print(f"\n[1] TRANG THAI CUA '{target_order_id}' TRUOC KHI UPDATE:")
    print("-" * 75)
    print(df_first[['order_id', 'customer_id', 'city', 'category', 'amount', 'total_revenue', 'status']].to_string(index=False))
    print("-" * 75)

    new_amount = 9999.00
    new_status = "REFUNDED"

    print(f"\n[...] Dang thuc hien UPDATE '{target_order_id}': amount {old_amount} -> {new_amount}, status '{old_status}' -> '{new_status}'...")

    # Update using arrow operations
    updated_amount = pc.if_else(
        pc.equal(arrow_current['order_id'], target_order_id),
        pa.scalar(new_amount),
        arrow_current['amount']
    )
    updated_status = pc.if_else(
        pc.equal(arrow_current['order_id'], target_order_id),
        pa.scalar(new_status),
        arrow_current['status']
    )
    updated_total_revenue = pc.if_else(
        pc.equal(arrow_current['order_id'], target_order_id),
        pc.multiply(pa.scalar(new_amount), arrow_current['quantity']),
        arrow_current['total_revenue']
    )
    updated_at = pc.if_else(
        pc.equal(arrow_current['order_id'], target_order_id),
        pa.scalar(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        arrow_current['updated_at']
    )

    arrow_updated = pa.table({
        'order_id': arrow_current['order_id'],
        'customer_id': arrow_current['customer_id'],
        'city': arrow_current['city'],
        'category': arrow_current['category'],
        'amount': updated_amount,
        'quantity': arrow_current['quantity'],
        'total_revenue': updated_total_revenue,
        'payment_method': arrow_current['payment_method'],
        'status': updated_status,
        'created_at': arrow_current['created_at'],
        'updated_at': updated_at,
        'version': arrow_current['version']
    })

    table.overwrite(arrow_updated)

    table = engine.get_table()
    snaps_after = table.snapshots()
    new_snap = snaps_after[-1]

    print(f"[OK] Update va commit Snapshot moi thanh cong! Snapshot ID = {new_snap.snapshot_id}")

    arrow_latest = table.scan(snapshot_id=new_snap.snapshot_id).to_arrow()
    match_latest = arrow_latest.filter(pc.equal(arrow_latest['order_id'], target_order_id))

    print(f"\n[2] TRANG THAI CUA '{target_order_id}' TAI SNAPSHOT MOI (Snapshot #{new_snap.snapshot_id}):")
    print("-" * 75)
    print(match_latest.to_pandas()[['order_id', 'customer_id', 'city', 'category', 'amount', 'total_revenue', 'status']].to_string(index=False))
    print("-" * 75)

    arrow_old_history = table.scan(snapshot_id=first_snap.snapshot_id).to_arrow()
    match_old = arrow_old_history.filter(pc.equal(arrow_old_history['order_id'], target_order_id))

    print(f"\n[3] TRANG THAI CUA '{target_order_id}' TAI SNAPSHOT BAN DAU QUA TIME TRAVEL (Snapshot #{first_snap.snapshot_id}):")
    print("-" * 75)
    print(match_old.to_pandas()[['order_id', 'customer_id', 'city', 'category', 'amount', 'total_revenue', 'status']].to_string(index=False))
    print("-" * 75)

    print("\n" + "=" * 75)
    print("Update va Time Travel kiem tra snapshot cu thanh cong!")
    print("=" * 75)

if __name__ == "__main__":
    main()
