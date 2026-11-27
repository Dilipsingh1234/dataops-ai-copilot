BEGIN;

CREATE TABLE public.ingestion_batches (
    batch_id          TEXT PRIMARY KEY CHECK (batch_id ~ '[^[:space:]]'),
    batch_fingerprint TEXT NOT NULL CHECK (batch_fingerprint ~ '^[0-9a-f]{64}$'),
    order_count       INTEGER NOT NULL CHECK (order_count > 0),
    item_count        INTEGER NOT NULL CHECK (item_count > 0),
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    airflow_run_id    TEXT NULL
);

COMMIT;
