# MysterriaDelivery audit events

MysterriaDelivery emits best-effort events through the optional COI `MysterriaAudit`
service. If the service is absent or unavailable, delivery continues unchanged.
Events are written to the shared SQLite audit ledger. The purchase ID is retained as
`businessId`; a deterministic UUID derived from it is used as `correlationId`, so queue,
restart, retry, and delivery events remain linked.

Queued deliveries also retain completed purchase IDs in `completed-queue.json`. This
tombstone file prevents a queue entry whose cleanup failed from being delivered again after
a restart. It contains purchase IDs only and uses the same atomic UTF-8 persistence as the
pending queue. Malformed queue files are moved aside with a `.corrupt-<timestamp>` suffix.
If the completed tombstones are malformed, queued delivery remains disabled across restarts
through `completed-queue.blocked` until an operator reconciles the quarantined file and removes
the marker.

| Event | Meaning | Metadata |
| --- | --- | --- |
| `mysterria-delivery.purchase.received` | A purchase request entered delivery processing. | `delivery_kind`, `service_name` or `source` |
| `mysterria-delivery.purchase.queued` | An offline delivery was persisted to the queue. | `delivery_kind`, `service_name`/`state` |
| `mysterria-delivery.purchase.duplicate-rejected` | A previously delivered or queued purchase was received again. | `delivery_kind`, `state` |
| `mysterria-delivery.purchase.discord-role-requested` | A Discord-role request was observed; this plugin does not claim external role delivery. | `delivery_kind`, `service_name` |
| `mysterria-delivery.purchase.delivered` | Delivery commands or entitlement mutation completed at the plugin commit point. | `delivery_kind`, `state` |
| `mysterria-delivery.purchase.failed` | Delivery configuration, command execution, or entitlement mutation failed. | `delivery_kind`, `reason`, `state` |
| `mysterria-delivery.purchase.recovered` | A queued delivery succeeded after one or more retries. | `delivery_kind`, `retry_count`, `state` |
| `mysterria-delivery.purchase.queue-cleanup-failed` | Delivery succeeded, but the persisted queue entry could not be removed. | `delivery_kind`, `reason`, `state` |
| `mysterria-delivery.purchase.retry-persistence-failed` | A failed delivery's updated retry count could not be persisted. | `delivery_kind`, `reason`, `state` |

No command text, payment-provider payload, announcement rendering, or queue polling is
recorded. Audit calls are guarded and never decide gameplay or API outcomes.
