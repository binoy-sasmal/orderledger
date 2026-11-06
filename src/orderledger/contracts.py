"""Event contract (schema version 1) shared by the simulator and the loader.

Identity, ordering and time are separate fields:
  event_id     identity of one change record (re-delivery of the same change reuses it)
  entity_id    the business object the change describes (order, order item, product)
  source_seq   version order of changes for one entity: the higher number supersedes
  effective_ts business time of the change; NULL when the source records no time
  arrival_ts   when the change reached the warehouse. ALWAYS SIMULATED (Olist has no
               arrival data); arrival_seq is its global order.
Origins label where content and effective time came from (see docs/contracts.md).
"""

from __future__ import annotations

SCHEMA_VERSION = 1

EVENT_COLUMNS: dict[str, str] = {
    "event_id": "VARCHAR",
    "entity_type": "VARCHAR",
    "entity_id": "VARCHAR",
    "event_type": "VARCHAR",
    "source_seq": "BIGINT",
    "effective_ts": "TIMESTAMP",
    "arrival_ts": "TIMESTAMP",
    "arrival_seq": "BIGINT",
    "payload": "VARCHAR",
    "content_origin": "VARCHAR",
    "timing_origin": "VARCHAR",
    "scenario_tag": "VARCHAR",
}

EVENT_TYPE_ENTITY: dict[str, str] = {
    "order_placed": "order",
    "order_approved": "order",
    "order_handed_to_carrier": "order",
    "order_delivered": "order",
    "order_status_observed": "order",
    "order_deleted": "order",
    "order_item_added": "order_item",
    "order_item_price_corrected": "order_item",
    "product_observed": "product",
}

CONTENT_ORIGINS = ("source-field", "synthetic-perturbation")
TIMING_ORIGINS = ("source-timestamp", "not-applicable", "synthetic")

# Source-origin event types that must be present in the source inventory.
SOURCE_EVENT_TYPES = (
    "order_placed",
    "order_approved",
    "order_handed_to_carrier",
    "order_delivered",
    "order_status_observed",
    "order_item_added",
    "product_observed",
)
