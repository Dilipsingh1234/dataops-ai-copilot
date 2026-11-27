"""Generate a reproducible synthetic order batch from the pinned catalog."""

import argparse
import csv
import hashlib
import io
import json
import random
import re
from datetime import date, datetime, time
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import NAMESPACE_URL, uuid5


DATA = Path(__file__).resolve().parents[1] / "data"
REFERENCE = DATA / "jaffle_shop"
ORDER_HEADER = ("id", "customer", "ordered_at", "store_id", "subtotal", "tax_paid", "order_total")
ITEM_HEADER = ("id", "order_id", "sku")


def reference_rows(table, key, checksums):
    path = f"raw/raw_{table}.csv"
    data = (REFERENCE / path).read_bytes()
    if hashlib.sha256(data).hexdigest() != checksums[path]:
        raise ValueError(f"Pinned reference checksum mismatch: {path}")
    rows = csv.DictReader(io.StringIO(data.decode("utf-8"), newline=""))
    return sorted(rows, key=lambda row: row[key])


def write_batch(directory, batch_id, business_date, seed, order_count):
    checksums = {}
    for line in (REFERENCE / "SHA256SUMS").read_text().splitlines():
        digest, filename = line.split()
        checksums[filename] = digest
    customers = reference_rows("customers", "id", checksums)
    products = reference_rows("products", "sku", checksums)
    stores = [store for store in reference_rows("stores", "id", checksums)
              if datetime.fromisoformat(store["opened_at"]).date() <= business_date]
    if not stores:
        raise ValueError(f"No stores open on {business_date}")

    rng = random.Random(seed)
    namespace = uuid5(NAMESPACE_URL, f"urn:jaffle-shop:synthetic:{batch_id}")
    orders_path = directory / "raw_orders.csv"
    items_path = directory / "raw_items.csv"
    item_count = 0
    with (orders_path.open("w", encoding="utf-8", newline="") as orders_file,
          items_path.open("w", encoding="utf-8", newline="") as items_file):
        orders = csv.writer(orders_file)
        items = csv.writer(items_file)
        orders.writerow(ORDER_HEADER)
        items.writerow(ITEM_HEADER)
        for number in range(1, order_count + 1):
            order_id = uuid5(namespace, f"order:{number}")
            customer = rng.choice(customers)
            store = rng.choice(stores)
            ordered_at = datetime.combine(business_date, time(rng.randrange(8, 20), rng.randrange(60)))
            subtotal = 0
            for position in range(1, rng.randint(1, 3) + 1):
                product = rng.choice(products)
                item_id = uuid5(order_id, f"item:{position}")
                items.writerow((item_id, order_id, product["sku"]))
                subtotal += int(product["price"])
                item_count += 1
            tax = int((subtotal * Decimal(store["tax_rate"])).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            orders.writerow((order_id, customer["id"], ordered_at.isoformat(), store["id"],
                             subtotal, tax, subtotal + tax))

    manifest = {
        "batch_id": batch_id,
        "source": "synthetic",
        "generator_version": 1,
        "business_date": business_date.isoformat(),
        "seed": seed,
        "expected_row_counts": {orders_path.name: order_count, items_path.name: item_count},
        "sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                   for path in (orders_path, items_path)},
    }
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8", newline="\n")


def generate_batch(batch_id, business_date, seed, order_count, incoming=DATA / "incoming"):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", batch_id):
        raise ValueError("Batch ID must start with a letter or digit and contain only letters, digits, - or _")
    if order_count < 1:
        raise ValueError("Order count must be positive")
    incoming.mkdir(parents=True, exist_ok=True)
    target = incoming / batch_id
    with TemporaryDirectory(dir=incoming, prefix=".generating-") as temporary:
        staged = Path(temporary) / "batch"
        staged.mkdir()
        write_batch(staged, batch_id, business_date, seed, order_count)
        if not target.exists() and not target.is_symlink():
            try:
                staged.rename(target)
                return f"Published {target}"
            except OSError:
                if not target.exists():
                    raise
                # A concurrent generator may have published the same batch first.
        if target.is_dir() and not target.is_symlink():
            names = {p.name for p in staged.iterdir()}
            if names == {p.name for p in target.iterdir()} and all(
                (target / name).is_file() and not (target / name).is_symlink()
                and (target / name).read_bytes() == (staged / name).read_bytes()
                for name in names
            ):
                return f"Identical batch already published: {target}"
        raise ValueError(f"Existing batch has different contents: {target}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--business-date", type=date.fromisoformat, required=True)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--orders", type=int, default=10)
    args = parser.parse_args()
    try:
        print(generate_batch(args.batch_id, args.business_date, args.seed, args.orders))
    except (ValueError, OSError) as error:
        parser.exit(1, f"{error}\n")
