# Getting the Olist data

The dataset is licensed CC BY-NC-SA 4.0 and is never committed here.

1. Open https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce (free Kaggle login).
2. Download the dataset (the "Download" button gives `archive.zip`, about 45 MB zipped).
3. Extract it so these files are in `data/olist/` inside the project:

   ```
   data/olist/olist_orders_dataset.csv
   data/olist/olist_order_items_dataset.csv
   data/olist/olist_customers_dataset.csv
   data/olist/olist_products_dataset.csv
   data/olist/product_category_name_translation.csv
   ```

   The other files in the archive (payments, reviews, sellers, geolocation) are not used and
   may be left out or ignored.
4. Verify and record the checksums:

   ```
   uv run orderledger check-data data/olist
   ```

   Sizes are compared with those Kaggle's metadata reported on 2026-11-06 (version 2).
   The SHA-256 values printed are what *this* download contained; Kaggle publishes no
   checksum to compare them with, so they are recorded as observations, not as upstream
   truth (see `docs/evidence-report.md`).

An alternative, with a Kaggle API token: `kaggle datasets download -d olistbr/brazilian-ecommerce`.
