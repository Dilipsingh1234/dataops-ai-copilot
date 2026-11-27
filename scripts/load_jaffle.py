"""Load the historical snapshot. Run with python -m scripts.load_jaffle."""

from hashlib import sha256
from pathlib import Path

from sqlalchemy import text

from backend.database import engine


DATA = Path(__file__).resolve().parents[1] / "data/jaffle_shop"
BATCH_ID = "jaffle-shop-data:87154139ff1c5af2c522da4e14e38b502541823e"
# Destination columns are listed in the original CSV field order.
TABLES = {
    "customers": ("raw_customers.csv", 930, "customer_id, customer_name"),
    "stores": ("raw_stores.csv", 6, "store_id, store_name, opened_at, tax_rate"),
    "products": ("raw_products.csv", 10,
                 "product_sku, product_name, product_type, price_cents, product_description"),
    "orders": ("raw_orders.csv", 63148,
               "order_id, customer_id, ordered_at, store_id, subtotal_cents, tax_paid_cents, order_total_cents"),
    "order_items": ("raw_items.csv", 90183, "order_item_id, order_id, product_sku"),
}


def stage_snapshot(connection):
    """Bulk-copy the verified bytes into typed, transaction-local work tables."""
    checksums = {}
    for line in (DATA / "SHA256SUMS").read_text().splitlines():
        digest, path = line.split()
        checksums[path] = digest

    for table, (filename, expected, columns) in TABLES.items():
        path = f"raw/{filename}"
        data = (DATA / path).read_bytes()
        if sha256(data).hexdigest() != checksums[path]:
            raise ValueError(f"Checksum mismatch for {filename}")
        connection.exec_driver_sql(
            f"CREATE TEMP TABLE stage_{table} ON COMMIT DROP AS "
            f"SELECT {columns} FROM public.{table} WITH NO DATA"
        )
        # Use Psycopg's COPY on the same connection and transaction as SQLAlchemy.
        with connection.connection.driver_connection.cursor() as cursor:
            with cursor.copy(f"COPY stage_{table} ({columns}) FROM STDIN WITH (FORMAT CSV, HEADER)") as copy:
                copy.write(data)
        count = connection.exec_driver_sql(f"SELECT count(*) FROM stage_{table}").scalar_one()
        if count != expected:
            raise ValueError(f"{filename}: expected {expected} rows, found {count}")


def load_snapshot():
    expected = {table: spec[1] for table, spec in TABLES.items()}
    with engine.begin() as connection:
        stage_snapshot(connection)
        # Serialize loaders and block other writers until validation finishes.
        targets = ", ".join(f"public.{table}" for table in TABLES)
        connection.exec_driver_sql(f"LOCK TABLE {targets} IN SHARE ROW EXCLUSIVE MODE")
        counts = {
            table: connection.exec_driver_sql(f"SELECT count(*) FROM public.{table}").scalar_one()
            for table in TABLES
        }
        inserting = not any(counts.values())
        if not inserting and counts != expected:
            raise ValueError(f"Unexpected existing table counts: {counts}")

        for table, (_, _, columns) in TABLES.items():
            source = columns
            if table == "orders":
                columns += ", source_batch_id"
                source += ", CAST(:batch_id AS text)"
            snapshot = f"SELECT {source} FROM stage_{table}"
            params = {"batch_id": BATCH_ID}
            if inserting:
                connection.execute(text(f"INSERT INTO public.{table} ({columns}) {snapshot}"), params)

            # Equal counts plus an empty multiset difference proves every value matches.
            actual = connection.exec_driver_sql(f"SELECT count(*) FROM public.{table}").scalar_one()
            mismatch = connection.execute(text(
                f"SELECT EXISTS ({snapshot} EXCEPT ALL SELECT {columns} FROM public.{table})"
            ), params).scalar_one()
            if actual != expected[table] or mismatch:
                raise ValueError(f"Stored {table} rows differ from the pinned snapshot")

        # Every order must join to its customer and store, and every item to its order and SKU.
        linked_orders, linked_items, empty_orders = connection.exec_driver_sql("""
            SELECT
                (SELECT count(*) FROM public.orders
                 JOIN public.customers USING (customer_id) JOIN public.stores USING (store_id)),
                (SELECT count(*) FROM public.order_items
                 JOIN public.orders USING (order_id) JOIN public.products USING (product_sku)),
                (SELECT count(*) FROM public.orders o WHERE NOT EXISTS
                    (SELECT 1 FROM public.order_items i WHERE i.order_id = o.order_id))
        """).one()
        if (linked_orders, linked_items, empty_orders) != (expected["orders"], expected["order_items"], 506):
            raise ValueError(f"Unexpected relationship counts: {linked_orders}, {linked_items}, {empty_orders}")

    action = "inserted" if inserting else "skipped"
    for table, count in expected.items():
        print(f"{table}: {count:,} {action}")
    print(f"Verified {empty_orders} orders without items. Batch: {BATCH_ID}")


if __name__ == "__main__":
    load_snapshot()
