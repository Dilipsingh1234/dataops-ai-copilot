# Jaffle Shop snapshot

dbt Labs generated this sample for the fictional Jaffle Shop using its Jaffle Shop generator. The five local CSVs are byte-for-byte copies of files in the upstream `jaffle-data/` directory at commit `87154139ff1c5af2c522da4e14e38b502541823e`. Each download was verified against the Git blob identifier in the pinned commit. The local SHA-256 digests are in `SHA256SUMS`. [Pinned source](https://github.com/dbt-labs/jaffle-shop-data/tree/87154139ff1c5af2c522da4e14e38b502541823e/jaffle-data)

The upstream README gives instructions for downloading and using the CSVs in the dbt tutorial. The pinned tree has no license file, and GitHub reports no detected repository license. Explicit terms for redistributing the dataset remain unclear. [Pinned README](https://github.com/dbt-labs/jaffle-shop-data/blob/87154139ff1c5af2c522da4e14e38b502541823e/README.md)

## Reproduce from the project root

For a fresh checkout, fetch the five files into the ignored raw directory and verify their checksums.

```sh
(
  set -e
  cd data/jaffle_shop
  mkdir -p raw
  base=https://raw.githubusercontent.com/dbt-labs/jaffle-shop-data/87154139ff1c5af2c522da4e14e38b502541823e/jaffle-data
  for table in customers stores products orders items; do
    curl --fail --location "$base/raw_${table}.csv" -o "raw/raw_${table}.csv"
  done
  shasum -a 256 -c SHA256SUMS
)
```

Run `.venv/bin/python scripts/profile_jaffle.py` to profile the local copies offline.
