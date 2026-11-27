"""Local PostgreSQL integration test. Requires permission to create a disposable database."""

import csv
import hashlib
import json
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
from threading import Barrier
from unittest.mock import patch
from uuid import uuid4

from sqlalchemy import create_engine, event, text

from backend.database import engine
from scripts import load_batch as loader
from scripts.generate_orders import DATA, generate_batch


class LoadBatchTest(unittest.TestCase):
    def setUp(self):
        # Only this newly created, uniquely named database is dropped during cleanup.
        name = "test_jaffle_" + uuid4().hex
        admin = engine.execution_options(isolation_level="AUTOCOMMIT")
        with admin.connect() as conn:
            conn.exec_driver_sql(f"CREATE DATABASE {name}")

        def drop_database():
            with admin.connect() as conn:
                conn.exec_driver_sql(f"DROP DATABASE {name}")

        self.addCleanup(drop_database)
        self.db = create_engine(engine.url.set(database=name))
        self.addCleanup(self.db.dispose)
        self.enterContext(patch.object(loader, "engine", self.db))
        self.incoming = Path(self.enterContext(tempfile.TemporaryDirectory()))
        with self.db.begin() as conn:
            for path in sorted((DATA.parent / "migrations").glob("*.sql")):
                sql = path.read_text().strip().removeprefix("BEGIN;").removesuffix("COMMIT;")
                conn.exec_driver_sql(sql)
            for table, fields in [
                ("customers", "customer_id, customer_name"),
                ("stores", "store_id, store_name, opened_at, tax_rate"),
                ("products", "product_sku, product_name, product_type, price_cents, product_description"),
            ]:
                with (DATA / "jaffle_shop/raw" / f"raw_{table}.csv").open(newline="") as source:
                    reader = csv.reader(source)
                    header = next(reader)
                    placeholders = ", ".join(["%s"] * len(header))
                    conn.exec_driver_sql(f"INSERT INTO public.{table} ({fields}) VALUES ({placeholders})",
                                         [tuple(row) for row in reader])

    def counts(self):
        with self.db.connect() as conn:
            return tuple(conn.exec_driver_sql(f"SELECT count(*) FROM public.{table}").scalar_one()
                         for table in ("ingestion_batches", "orders", "order_items"))

    def test_concurrent_claim_conflicts_and_rollback(self):
        batch = "isolated-001"
        generate_batch(batch, date(2017, 9, 1), 42, 10, self.incoming)
        barrier = Barrier(2)

        def synchronize_claim(conn, cursor, statement, parameters, context, executemany):
            if "INSERT INTO public.ingestion_batches" in statement:
                barrier.wait(timeout=10)

        event.listen(self.db, "before_cursor_execute", synchronize_claim)
        try:
            with ThreadPoolExecutor(2) as pool:
                runs = [pool.submit(loader.load_batch, batch, incoming=self.incoming,
                                    airflow_run_id=f"worker-{i}") for i in range(2)]
                results = [run.result(timeout=30) for run in runs]
        finally:
            event.remove(self.db, "before_cursor_execute", synchronize_claim)
        self.assertCountEqual([r["status"] for r in results], ["loaded", "already_loaded"])
        expected = (1, results[0]["order_count"], results[0]["item_count"])
        self.assertEqual(self.counts(), expected)
        winner = next(i for i, r in enumerate(results) if r["status"] == "loaded")
        with self.db.connect() as conn:
            self.assertEqual(conn.exec_driver_sql("SELECT airflow_run_id FROM public.ingestion_batches").scalar_one(),
                             f"worker-{winner}")

        manifest_path = self.incoming / batch / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["seed"] += 1
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(loader.BatchLoadError, "conflicts with the committed batch"):
            loader.load_batch(batch, incoming=self.incoming)

        # Fresh batch ID with identical business IDs must also be rejected.
        collision = "isolated-collision"
        (self.incoming / batch).rename(self.incoming / collision)
        manifest["batch_id"] = collision
        (self.incoming / collision / "manifest.json").write_text(json.dumps(manifest))
        with self.assertRaisesRegex(loader.BatchLoadError, "existing orders.order_id"):
            loader.load_batch(collision, incoming=self.incoming)

        failing = "isolated-rollback"
        generate_batch(failing, date(2017, 9, 1), 42, 10, self.incoming)
        with self.assertRaisesRegex(loader.BatchLoadError, "expected fingerprint differs"):
            loader.load_batch(failing, incoming=self.incoming, expected_fingerprint="0" * 64)

        # Exercise the item-ID collision with otherwise fresh, valid orders.
        items_path = self.incoming / failing / "raw_items.csv"
        manifest_path = items_path.with_name("manifest.json")
        original_items = items_path.read_bytes()
        original_manifest = manifest_path.read_bytes()
        with self.db.connect() as conn:
            existing_item = conn.exec_driver_sql("SELECT order_item_id FROM public.order_items LIMIT 1").scalar_one()
        with items_path.open(newline="") as source:
            rows = list(csv.reader(source))
        rows[1][0] = str(existing_item)
        with items_path.open("w", newline="") as target:
            csv.writer(target).writerows(rows)
        manifest = json.loads(original_manifest)
        manifest["sha256"][items_path.name] = hashlib.sha256(items_path.read_bytes()).hexdigest()
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(loader.BatchLoadError, "existing order_items.order_item_id"):
            loader.load_batch(failing, incoming=self.incoming)
        items_path.write_bytes(original_items)
        manifest_path.write_bytes(original_manifest)

        def fail_after_orders(conn, cursor, statement, parameters, context, executemany):
            if "INSERT INTO public.order_items" in statement:
                rows = conn.execute(text("SELECT count(*) FROM public.orders WHERE source_batch_id = :id"),
                                    {"id": failing}).scalar_one()
                self.assertEqual(rows, 10)
                conn.exec_driver_sql("SELECT 1 / 0")

        event.listen(self.db, "before_cursor_execute", fail_after_orders)
        try:
            with self.assertRaisesRegex(loader.BatchLoadError, "PostgreSQL load failed"):
                loader.load_batch(failing, incoming=self.incoming)
        finally:
            event.remove(self.db, "before_cursor_execute", fail_after_orders)
        self.assertEqual(self.counts(), expected)
        print(f"Concurrent runs: loaded + already_loaded; ledger/orders/items: {expected}. "
              "Fingerprint and order/item ID conflicts rejected. Mid-load rollback verified.")
