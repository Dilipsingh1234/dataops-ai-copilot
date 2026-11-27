import csv
import hashlib
import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from scripts.generate_orders import generate_batch
from scripts.validate_batch import BatchValidationError, extract_batch_files, read_verified_batch


class BatchExtractionTest(unittest.TestCase):
    def test_missing_column_is_left_for_validation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            batch_id = "test-missing-sku"
            generate_batch(batch_id, date(2017, 9, 1), 42, 2, root)
            batch = root / batch_id
            items = batch / "raw_items.csv"
            with items.open(newline="") as source:
                rows = list(csv.reader(source))
            with items.open("w", newline="") as output:
                csv.writer(output).writerows(row[:-1] for row in rows)
            manifest_path = batch / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["sha256"][items.name] = hashlib.sha256(items.read_bytes()).hexdigest()
            manifest_path.write_text(json.dumps(manifest))
            extract_batch_files(batch_id, root)
            with self.assertRaisesRegex(BatchValidationError, "missing column sku"):
                read_verified_batch(batch_id, root)


if __name__ == "__main__":
    unittest.main()
