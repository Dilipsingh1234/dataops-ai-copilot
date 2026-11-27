"""Load one verified synthetic batch. Run with python -m scripts.load_batch."""

import argparse
import json

from sqlalchemy import text
from sqlalchemy.exc import IntegrityError, OperationalError, SQLAlchemyError

from backend.database import engine
from scripts.generate_orders import DATA
from scripts.validate_batch import BatchValidationError, batch_fingerprint, check_orders, read_verified_batch, reference_data


class BatchLoadError(ValueError):
    """A load failure safe to report to the caller."""


class BatchLoadDatabaseError(BatchLoadError):
    """A database connectivity failure that may succeed on retry."""


def claim_batch(conn, params):
    claimed = conn.execute(text("""
        INSERT INTO public.ingestion_batches
            (batch_id, batch_fingerprint, order_count, item_count, airflow_run_id)
        VALUES (:batch_id, :fingerprint, :orders, :items, :run_id)
        ON CONFLICT (batch_id) DO NOTHING RETURNING batch_id
    """), params).scalar_one_or_none()
    if claimed is not None:
        return True
    # A separate Read Committed statement sees the winner after a concurrent insert.
    existing = conn.execute(text("""
        SELECT batch_fingerprint, order_count, item_count
        FROM public.ingestion_batches WHERE batch_id = :batch_id
    """), params).one()
    if existing != (params["fingerprint"], params["orders"], params["items"]):
        raise BatchLoadError(f"{params['batch_id']}: conflicts with the committed batch")
    return False


def insert_batch(conn, params, orders, items):
    batch_id = params["batch_id"]
    for table, key, rows in (("orders", "order_id", orders), ("order_items", "order_item_id", items)):
        existing = conn.execute(text(
            f"SELECT {key} FROM public.{table} WHERE {key} = ANY(CAST(:ids AS uuid[])) LIMIT 1"
        ), {"ids": [row["id"] for _, row in rows]}).scalar_one_or_none()
        if existing is not None:
            raise BatchLoadError(f"{batch_id}: existing {table}.{key} {existing}")

    conn.execute(text("""
        INSERT INTO public.orders (order_id, customer_id, store_id, ordered_at, subtotal_cents,
                                   tax_paid_cents, order_total_cents, source_batch_id)
        VALUES (:id, :customer, :store_id, :ordered_at, :subtotal, :tax_paid, :order_total, :batch_id)
    """), [dict(row, batch_id=batch_id) for _, row in orders])
    conn.execute(text("""
        INSERT INTO public.order_items (order_item_id, order_id, product_sku)
        VALUES (:id, :order_id, :sku)
    """), [row for _, row in items])
    counts = conn.execute(text("""
        SELECT count(DISTINCT o.order_id), count(i.order_item_id)
        FROM public.orders o LEFT JOIN public.order_items i USING (order_id)
        WHERE o.source_batch_id = :batch_id
    """), params).one()
    if counts != (len(orders), len(items)):
        raise BatchLoadError(f"{batch_id}: stored counts differ from verified input")


def load_batch(batch_id, *, expected_fingerprint=None, airflow_run_id=None, incoming=DATA / "incoming"):
    manifest, business_date, orders, items = read_verified_batch(batch_id, incoming)
    fingerprint = batch_fingerprint(manifest)
    if expected_fingerprint is not None and expected_fingerprint != fingerprint:
        raise BatchLoadError(f"{batch_id}: expected fingerprint differs from verified manifest")
    params = dict(batch_id=batch_id, fingerprint=fingerprint, orders=len(orders),
                  items=len(items), run_id=airflow_run_id)
    try:
        with engine.begin() as conn:
            conn.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            claimed = claim_batch(conn, params)
            if claimed:
                conn.exec_driver_sql("LOCK TABLE public.customers, public.stores, public.products IN SHARE MODE")
                check_orders(orders, items, business_date, reference_data(conn))
                insert_batch(conn, params, orders, items)
    except BatchValidationError as error:
        raise BatchValidationError(f"{batch_id}: {error}") from None
    except IntegrityError:
        raise BatchLoadError(f"{batch_id}: database key or constraint conflict") from None
    except OperationalError:
        raise BatchLoadDatabaseError(f"{batch_id}: PostgreSQL unavailable") from None
    except SQLAlchemyError:
        raise BatchLoadError(f"{batch_id}: PostgreSQL load failed") from None
    return {
        "batch_id": batch_id, "fingerprint": fingerprint,
        "status": "loaded" if claimed else "already_loaded",
        "order_count": len(orders), "item_count": len(items),
        "orders_inserted": len(orders) if claimed else 0,
        "items_inserted": len(items) if claimed else 0,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--expected-fingerprint")
    parser.add_argument("--airflow-run-id")
    args = parser.parse_args()
    try:
        print(json.dumps(load_batch(args.batch_id, expected_fingerprint=args.expected_fingerprint,
                                    airflow_run_id=args.airflow_run_id)))
    except (BatchLoadError, BatchValidationError) as error:
        parser.exit(1, f"{error}\n")
