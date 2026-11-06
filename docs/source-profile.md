# Source profile

Measured directly from the source CSVs by `orderledger profile`.

## Row counts

| table | rows |
|---|---|
| orders | 99441 |
| items | 112650 |
| customers | 99441 |
| products | 32951 |

## Orders: null counts per timestamp

| column | nulls | of |
|---|---|---|
| order_purchase_timestamp | 0 | 99441 |
| order_approved_at | 160 | 99441 |
| order_delivered_carrier_date | 1783 | 99441 |
| order_delivered_customer_date | 2965 | 99441 |
| order_estimated_delivery_date | 0 | 99441 |

## Order status

| status | orders |
|---|---|
| delivered | 96478 |
| shipped | 1107 |
| canceled | 625 |
| unavailable | 609 |
| invoiced | 314 |
| processing | 301 |
| created | 5 |
| approved | 2 |

Purchase timestamps range from 2016-09-04 21:15:19 to 2018-10-17 17:30:18.

## Contradictions in the source

| observation | orders |
|---|---|
| status canceled but a customer delivery time exists | 6 |
| status delivered but no customer delivery time | 8 |
| delivery time exists but status is neither delivered nor canceled | 0 |
| approved before purchase | 0 |
| carrier handover before approval | 1359 |
| carrier handover before purchase | 166 |
| delivery before carrier handover | 23 |
| delivery before purchase | 0 |
| order with no item rows | 775 |
| item whose order is missing | 0 |

Cancellation has no timestamp in the source, so none is derived.

## Customers and addresses (decides whether SCD2 is justified)

| measure | value |
|---|---|
| customer_id rows (one per order) | 99441 |
| distinct customer_unique_id (people) | 96096 |
| people with more than one order | 2997 |
| people observed with more than one city/state | 122 |
| people observed in more than one state | 39 |

2997 of 96096 people (3.1%) have more than one order; 122 (0.13%) were observed at more than one city/state. An address here is the one observed on a particular order, not verified residence history.

## Items and products

| measure | value |
|---|---|
| orders with at least one item | 98666 |
| items per order, max | 21 |
| items with null price | 0 |
| products with no category | 610 |
| items whose product is missing | 0 |
