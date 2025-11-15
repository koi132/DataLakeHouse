TRUNCATE TABLE public.olist_customers RESTART IDENTITY;
\copy public.olist_customers FROM '/tmp/olist_customers_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.olist_orders RESTART IDENTITY;
\copy public.olist_orders FROM '/tmp/olist_orders_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.olist_order_items RESTART IDENTITY;
\copy public.olist_order_items FROM '/tmp/olist_order_items_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.olist_products RESTART IDENTITY;
\copy public.olist_products FROM '/tmp/olist_products_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.olist_sellers RESTART IDENTITY;
\copy public.olist_sellers FROM '/tmp/olist_sellers_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.olist_geolocation RESTART IDENTITY;
\copy public.olist_geolocation FROM '/tmp/olist_geolocation_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.olist_order_payments RESTART IDENTITY;
\copy public.olist_order_payments FROM '/tmp/olist_order_payments_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.olist_order_reviews RESTART IDENTITY;
\copy public.olist_order_reviews FROM '/tmp/olist_order_reviews_dataset.csv' DELIMITER ',' CSV HEADER;

TRUNCATE TABLE public.product_category_translation RESTART IDENTITY;
\copy public.product_category_translation FROM '/tmp/product_category_name_translation.csv' DELIMITER ',' CSV HEADER;