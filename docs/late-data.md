# Late data, corrections and the lookback

Two different things can be late, and the lookback only matters for one of them.

| Case | Example | What happens | Lookback needed? |
|---|---|---|---|
| **Late first arrival** | The delivery event for an order arrives 25 days after it happened | The order has an event past the watermark, so it is *touched*, rebuilt from its full history, and its **new** date is restated | No. Applied however late it is (tested: `test_first_arrival_is_applied_however_late_it_is`) |
| **Late correction that moves an order off an older date** | A delivery time is corrected two days later, 25 days after the original was published | The order is rebuilt and its *new* date is restated, but the *old* date's published row still counts it. The run's reconciliation (`facts_to_marts`) finds the stale date, **fails, and publishes nothing** | Yes. Inside the window the old date is restated automatically (AT-05). Outside it the run fails visibly and `orderledger backfill --from D --to D` (or `--auto`) repairs it (AT-06) |
| **Late deletion** | An order is deleted 5 days after it was placed | The order is rebuilt as deleted; its dates are its own touched dates, so both aggregates are restated | No |
| **Out-of-order versions** | A price correction arrives before the item it corrects | The higher `source_seq` stays current whatever the arrival order; the item appears once its `order_item_added` arrives | No |

## Why the failure is deliberate

Detecting "this order used to be on date A" needs the old state, which an incremental model that
only sees the new state does not have. Rather than silently publish a wrong number for date A,
the reconciliation compares the incremental aggregates with a full recomputation from the facts
and refuses to publish on any difference, naming the dates and the command to run. In a real
system the same check would page someone; here it makes the run exit non-zero.

## What a backfill does

`backfill --from A --to B` does not load new batches. For orders whose purchase or delivery date
falls in `[A, B]` it forces a rebuild from full history, restates every aggregate date in the
window, then runs the same tests, reconciliation and publish as a normal run. Equivalence tests
therefore compare states **after the backfills reconciliation requires**.

## Anchoring

The lookback is measured from the simulated `checkpoint_date`, never wall-clock or load time, so
the same input history always produces the same published result.
