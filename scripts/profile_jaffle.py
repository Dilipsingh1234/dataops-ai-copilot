"""Profile the five local Jaffle Shop CSVs. Run from any directory."""

import csv
import re
from collections import Counter
from datetime import datetime
from pathlib import Path


RAW = Path(__file__).resolve().parents[1] / "data/jaffle_shop/raw"
# Parents come first so their keys are available when checking child rows.
PRIMARY = {"customers": "id", "stores": "id", "products": "sku", "orders": "id", "items": "id"}
REFERENCES = {
    "orders": {"customer": "customers", "store_id": "stores"},
    "items": {"order_id": "orders", "sku": "products"},
}


def value_type(value):
    """Recognize the formats used by the source, falling back to text."""
    if re.fullmatch(r"[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}", value):
        return "UUID"
    if re.fullmatch(r"[+-]?[0-9]+", value):
        return "integer"
    if re.fullmatch(r"[+-]?[0-9]+\.[0-9]+", value):
        return "decimal"
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}", value):
        try:
            datetime.fromisoformat(value)
            return "timestamp"
        except ValueError:
            pass
    return "text"


def profile(directory):
    keys = {}
    orders_with_items = set()
    earliest = latest = None
    for table, primary in PRIMARY.items():
        filename = f"raw_{table}.csv"
        keys[table] = set()
        references = REFERENCES.get(table, {})
        missing, padded, unmatched = Counter(), Counter(), Counter()
        duplicates = rows = 0
        with (directory / filename).open(encoding="utf-8", newline="") as source:
            reader = csv.DictReader(source, strict=True)
            types = {column: Counter() for column in reader.fieldnames}
            examples = {column: {} for column in types}
            for row in reader:
                rows += 1
                if None in row or None in row.values():
                    raise ValueError(f"{filename}, line {reader.line_num}: wrong field count")
                for column, value in row.items():
                    if not value.strip():
                        missing[column] += 1
                        continue
                    padded[column] += value != value.strip()
                    kind = value_type(value)
                    types[column][kind] += 1
                    examples[column].setdefault(kind, (reader.line_num, value))
                    if table == "orders" and column == "ordered_at" and kind == "timestamp":
                        earliest = min(earliest or value, value)
                        latest = max(latest or value, value)

                key = row[primary]
                if key.strip():
                    duplicates += key in keys[table]
                    keys[table].add(key)
                for column, parent in references.items():
                    if row[column].strip() and row[column] not in keys[parent]:
                        unmatched[column] += 1
                if table == "items":
                    orders_with_items.add(row["order_id"])

        print(f"{filename}: {rows} rows, duplicate {primary} occurrences: {duplicates}")
        for column, counts in types.items():
            inferred = counts.most_common(1)[0][0] if counts else "unknown"
            # Integers also fit a decimal column.
            if "decimal" in counts and inferred == "integer":
                inferred = "decimal"
            allowed = {"integer", "decimal"} if inferred == "decimal" else {inferred}
            invalid = [kind for kind in counts if kind not in allowed and inferred != "text"]
            errors = sum(counts[kind] for kind in invalid)
            print(f"  {column}: {inferred}, missing={missing[column]}, invalid={errors}, "
                  f"outer whitespace={padded[column]}")
            for kind in invalid:
                line, value = examples[column][kind]
                print(f"    example at line {line}: {value!r}")
        for column, parent in references.items():
            print(f"  {column} -> {parent}.{PRIMARY[parent]}: "
                  f"{unmatched[column]} unmatched, {missing[column]} missing")
    print(f"Order timestamps: {earliest} to {latest}")
    print(f"Orders without items: {len(keys['orders'] - orders_with_items)}")


if __name__ == "__main__":
    profile(RAW)
