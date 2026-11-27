"""Validate one generated Jaffle Shop batch. Run with python -m scripts.validate_batch."""

import argparse
import csv
import hashlib
import io
import json
import re
from collections import Counter
from datetime import date, datetime
from decimal import ROUND_HALF_UP
from pathlib import Path
from uuid import UUID

from sqlalchemy.exc import SQLAlchemyError

from backend.database import engine
from scripts.generate_orders import DATA, ITEM_HEADER, ORDER_HEADER


ORDERS = "raw_orders.csv"
ITEMS = "raw_items.csv"
HEADERS = {ORDERS: ORDER_HEADER, ITEMS: ITEM_HEADER}


class BatchValidationError(ValueError):
    """A safe, actionable explanation of the first batch failure."""


class BatchDatabaseError(BatchValidationError):
    """A database failure that may succeed on retry."""


def read_bytes(path):
    try:
        return path.read_bytes()
    except OSError:
        raise BatchValidationError(f"{path.name}: missing or unreadable file") from None


def read_manifest(directory, batch_id):
    try:
        manifest = json.loads(read_bytes(directory / "manifest.json"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise BatchValidationError("manifest.json: invalid JSON") from None
    if not isinstance(manifest, dict) or manifest.get("batch_id") != batch_id:
        raise BatchValidationError("manifest.json batch_id: must match the requested batch")
    if manifest.get("source") != "synthetic":
        raise BatchValidationError("manifest.json source: expected synthetic")
    if type(manifest.get("generator_version")) is not int or manifest["generator_version"] != 1:
        raise BatchValidationError("manifest.json generator_version: expected version 1")
    if type(manifest.get("seed")) is not int:
        raise BatchValidationError("manifest.json seed: expected an integer")
    for field in ("expected_row_counts", "sha256"):
        entries = manifest.get(field)
        if not isinstance(entries, dict) or set(entries) != set(HEADERS):
            raise BatchValidationError(f"manifest.json {field}: expected entries for {ORDERS} and {ITEMS}")
    for name, count in manifest["expected_row_counts"].items():
        if type(count) is not int or count < 1:
            raise BatchValidationError(f"manifest.json expected_row_counts: invalid count for {name}")
    try:
        business_date = date.fromisoformat(manifest["business_date"])
    except (KeyError, ValueError, TypeError):
        raise BatchValidationError("manifest.json business_date: expected an ISO date") from None
    return manifest, business_date


def read_rows(directory, name, manifest):
    data = read_bytes(directory / name)
    if hashlib.sha256(data).hexdigest() != manifest["sha256"][name]:
        raise BatchValidationError(f"{name}: SHA-256 differs from manifest")
    try:
        stream = io.StringIO(data.decode("utf-8"), newline="")
    except UnicodeDecodeError:
        raise BatchValidationError(f"{name}: invalid UTF-8") from None
    reader = csv.reader(stream, strict=True)
    header = HEADERS[name]
    rows = []
    try:
        actual = next(reader, [])
        if actual != list(header):
            missing = [col for col in header if col not in actual]
            reason = f"missing column {', '.join(missing)}" if missing else f"expected header {','.join(header)}"
            raise BatchValidationError(f"{name} row 1: {reason}")
        for values in reader:
            where = f"{name} row {reader.line_num}"
            if len(values) != len(header):
                raise BatchValidationError(f"{where}: expected {len(header)} fields, found {len(values)}")
            row = dict(zip(header, values))
            for column, value in row.items():
                if not value.strip():
                    raise BatchValidationError(f"{where} {column}: required value is empty")
            rows.append((where, row))
    except csv.Error:
        raise BatchValidationError(f"{name} row {reader.line_num}: malformed CSV") from None
    expected = manifest["expected_row_counts"][name]
    if len(rows) != expected:
        raise BatchValidationError(f"{name}: expected {expected} rows, found {len(rows)}")
    return rows


def uuid_value(row, column, where):
    try:
        value = UUID(row[column])
        if str(value) != row[column].lower():
            raise ValueError
        return value
    except ValueError:
        raise BatchValidationError(f"{where} {column}: expected a hyphenated UUID") from None


def amount(row, column, where):
    value = row[column]
    if not re.fullmatch(r"[0-9]+", value):
        raise BatchValidationError(f"{where} {column}: expected nonnegative integer cents")
    digits = value.lstrip("0") or "0"
    if len(digits) > 10 or int(digits) > 2147483647:
        raise BatchValidationError(f"{where} {column}: exceeds PostgreSQL INTEGER range")
    return int(digits)


def reference_data(connection):
    customers = set(connection.exec_driver_sql("SELECT customer_id FROM public.customers").scalars())
    stores = {key: (rate, opened) for key, rate, opened in connection.exec_driver_sql(
        "SELECT store_id, tax_rate, opened_at FROM public.stores"
    )}
    prices = dict(connection.exec_driver_sql("SELECT product_sku, price_cents FROM public.products").all())
    return customers, stores, prices


def check_orders(order_rows, item_rows, business_date, references):
    customers, stores, prices = references
    orders = {}
    for where, row in order_rows:
        order_id = uuid_value(row, "id", where)
        if order_id in orders:
            raise BatchValidationError(f"{where} id: duplicate order ID")
        if uuid_value(row, "customer", where) not in customers:
            raise BatchValidationError(f"{where} customer: unknown customer ID")
        store = uuid_value(row, "store_id", where)
        if store not in stores:
            raise BatchValidationError(f"{where} store_id: unknown store ID")
        rate, opened = stores[store]
        try:
            stamp = datetime.fromisoformat(row["ordered_at"])
            if stamp.tzinfo is not None or stamp.isoformat(timespec="seconds") != row["ordered_at"]:
                raise ValueError
        except ValueError:
            raise BatchValidationError(f"{where} ordered_at: expected YYYY-MM-DDTHH:MM:SS") from None
        if stamp.date() != business_date or stamp < opened:
            raise BatchValidationError(f"{where} ordered_at: outside business date or before store opened")
        subtotal = amount(row, "subtotal", where)
        tax = amount(row, "tax_paid", where)
        total = amount(row, "order_total", where)
        if tax != (subtotal * rate).quantize(1, rounding=ROUND_HALF_UP):
            raise BatchValidationError(f"{where} tax_paid: differs from store tax rounded half up")
        if total != subtotal + tax:
            raise BatchValidationError(f"{where} order_total: differs from subtotal plus tax")
        orders[order_id] = (where, subtotal)

    item_ids = set()
    counts, subtotals = Counter(), Counter()
    for where, row in item_rows:
        item = uuid_value(row, "id", where)
        if item in item_ids:
            raise BatchValidationError(f"{where} id: duplicate item ID")
        item_ids.add(item)
        parent = uuid_value(row, "order_id", where)
        if parent not in orders:
            raise BatchValidationError(f"{where} order_id: order absent from this batch")
        if row["sku"] not in prices:
            raise BatchValidationError(f"{where} sku: unknown product SKU")
        counts[parent] += 1
        subtotals[parent] += prices[row["sku"]]
    for order_id, (where, subtotal) in orders.items():
        if not 1 <= counts[order_id] <= 3:
            raise BatchValidationError(f"{where} id: expected 1 to 3 items for this synthetic order")
        if subtotal != subtotals[order_id]:
            raise BatchValidationError(f"{where} subtotal: expected {subtotals[order_id]} cents from item prices")


def extract_batch_files(batch_id, incoming):
    """Check the manifest and file fingerprints without parsing CSV contents."""
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", batch_id):
        raise BatchValidationError(f"Invalid batch ID {batch_id!r}")
    directory = Path(incoming) / batch_id
    try:
        manifest, business_date = read_manifest(directory, batch_id)
        for name in HEADERS:
            if hashlib.sha256(read_bytes(directory / name)).hexdigest() != manifest["sha256"][name]:
                raise BatchValidationError(f"{name}: SHA-256 differs from manifest")
        if any(path.name not in {*HEADERS, "manifest.json"} for path in directory.iterdir()):
            raise BatchValidationError("unexpected file in batch directory")
        return manifest, business_date
    except BatchValidationError as error:
        raise BatchValidationError(f"{batch_id}: {error}") from None


def read_verified_batch(batch_id, incoming):
    """Return rows parsed from checksum-verified bytes."""
    manifest, business_date = extract_batch_files(batch_id, incoming)
    directory = Path(incoming) / batch_id
    try:
        orders = read_rows(directory, ORDERS, manifest)
        items = read_rows(directory, ITEMS, manifest)
        return manifest, business_date, orders, items
    except BatchValidationError as error:
        raise BatchValidationError(f"{batch_id}: {error}") from None


def batch_fingerprint(manifest):
    canonical = json.dumps(manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def validate_batch_summary(batch_id, incoming=DATA / "incoming", expected_fingerprint=None):
    manifest, business_date, orders, items = read_verified_batch(batch_id, incoming)
    fingerprint = batch_fingerprint(manifest)
    if expected_fingerprint is not None and fingerprint != expected_fingerprint:
        raise BatchValidationError(f"{batch_id}: batch fingerprint changed after extraction")
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            check_orders(orders, items, business_date, reference_data(connection))
    except SQLAlchemyError:
        raise BatchDatabaseError(f"{batch_id}: PostgreSQL: unable to read reference data") from None
    except BatchValidationError as error:
        raise BatchValidationError(f"{batch_id}: {error}") from None
    return {"batch_id": batch_id, "fingerprint": fingerprint,
            "order_count": len(orders), "item_count": len(items)}


def validate_batch(batch_id, incoming=DATA / "incoming"):
    result = validate_batch_summary(batch_id, incoming)
    return f"{batch_id}: valid ({result['order_count']} orders, {result['item_count']} items)"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", required=True)
    args = parser.parse_args()
    try:
        print(validate_batch(args.batch_id))
    except BatchValidationError as error:
        parser.exit(1, f"{error}\n")
