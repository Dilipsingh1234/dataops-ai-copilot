BEGIN;

CREATE TABLE public.customers (
    customer_id   UUID PRIMARY KEY,
    customer_name TEXT NOT NULL
);

CREATE TABLE public.stores (
    store_id   UUID PRIMARY KEY,
    store_name TEXT NOT NULL,
    opened_at  TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    tax_rate   NUMERIC NOT NULL CHECK (tax_rate BETWEEN 0 AND 1)
);

CREATE TABLE public.products (
    product_sku         TEXT PRIMARY KEY,
    product_name        TEXT NOT NULL,
    product_type        TEXT NOT NULL,
    price_cents         INTEGER NOT NULL CHECK (price_cents >= 0),
    product_description TEXT NOT NULL
);

CREATE TABLE public.orders (
    order_id          UUID PRIMARY KEY,
    customer_id       UUID NOT NULL REFERENCES public.customers (customer_id),
    store_id          UUID NOT NULL REFERENCES public.stores (store_id),
    ordered_at        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    subtotal_cents    INTEGER NOT NULL CHECK (subtotal_cents >= 0),
    tax_paid_cents    INTEGER NOT NULL CHECK (tax_paid_cents >= 0),
    order_total_cents INTEGER NOT NULL CHECK (order_total_cents >= 0),
    source_batch_id   TEXT NOT NULL,

    CONSTRAINT orders_total_matches_parts
        CHECK (order_total_cents = subtotal_cents::bigint + tax_paid_cents)
);

CREATE TABLE public.order_items (
    order_item_id UUID PRIMARY KEY,
    order_id      UUID NOT NULL REFERENCES public.orders (order_id),
    product_sku   TEXT NOT NULL REFERENCES public.products (product_sku)
);

CREATE INDEX orders_customer_id_idx ON public.orders (customer_id);
CREATE INDEX orders_store_id_idx ON public.orders (store_id);
CREATE INDEX order_items_order_id_idx ON public.order_items (order_id);
CREATE INDEX order_items_product_sku_idx ON public.order_items (product_sku);

COMMIT;
