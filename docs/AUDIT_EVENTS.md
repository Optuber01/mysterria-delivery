# MysterriaDelivery audit events

`completed-queue.json` now stores purchase IDs mapped to `DELIVERED`, `PARTIAL`, or `LEGACY`.
Partial execution remains replay-blocked but never returns a successful completion receipt.
Old ID-only arrays load conservatively as `LEGACY`: they prevent re-execution but require
reconciliation before anyone can claim full success. Unknown states block replay protection
through the existing quarantine/blocked-marker path. State and ID are written atomically in
one file. A repeat completion response does not invent an original delivery timestamp.

Queued and already-queued responses carry `queued=true` and no completion timestamp.
The backend must inspect the inner receipt and matching purchase ID, not treat HTTP 2xx as delivered.
Purchase payloads accept both `quantity` and the backend's `amount` alias; omitted quantities
default to one for both JSON and builder construction. Routine queue/grant console chatter is
debug-level; replay guards, pending queue and failure diagnostics remain active.

MysterriaDelivery emits best-effort events through its shaded neutral audit client. The optional local engine ingests spool segments into SQLite. The purchase ID is retained as `business_id`; its deterministic correlation UUID links queue, restart, retry, and delivery events. Audit failures do not change delivery behavior.

The optional per-server audit engine owns SQLite and local staff searches. Each producer writes to its own bounded spool directory even when the engine is absent. Existing gameplay dependencies remain separate from audit transport.

Online and queued deliveries retain completed purchase IDs in `completed-queue.json`. This
tombstone file prevents a queue entry whose cleanup failed from being delivered again after
a restart. It contains purchase IDs only and uses the same atomic UTF-8 persistence as the
pending queue. Malformed queue files are moved aside with a `.corrupt-<timestamp>` suffix.
If the completed tombstones are malformed, queued delivery remains disabled across restarts
through `completed-queue.blocked` until an operator reconciles the quarantined file and removes
the marker.

Online completion writes run on a bounded background worker before the response is
acknowledged. Partial command execution also retains a tombstone to prevent automatic
replay. A failed or rejected completion write returns a reconciliation error and keeps
the purchase blocked in memory. This is not an atomic transaction with Minecraft commands
or LuckPerms: a process crash after effects but before the tombstone is saved can still
require manual reconciliation. The `delivered` event describes effects, not tombstone durability.

| Event | Meaning | Metadata |
| --- | --- | --- |
| `mysterria-delivery.purchase.received` | A purchase request entered delivery processing. | `delivery_kind`, `service_name` or `source` |
| `mysterria-delivery.purchase.queued` | An offline delivery was persisted to the queue. | `delivery_kind`, `service_name`/`state` |
| `mysterria-delivery.purchase.duplicate-rejected` | A previously delivered or queued purchase was received again. | `delivery_kind`, `state` |
| `mysterria-delivery.purchase.discord-role-requested` | A Discord-role request was observed; this plugin does not claim external role delivery. | `delivery_kind`, `service_name` |
| `mysterria-delivery.purchase.delivered` | Delivery commands or entitlement mutation completed at the plugin commit point. | `delivery_kind`, `state` |
| `mysterria-delivery.purchase.failed` | Delivery configuration, command execution, or entitlement mutation failed. | `delivery_kind`, `reason`, `state` |
| `mysterria-delivery.purchase.completion-persistence-failed` | Effects occurred, but completion storage failed or its bounded worker rejected admission. Automatic replay remains blocked in memory; reconcile before restart. | `delivery_kind`, `reason`, `requires_reconciliation` |
| `mysterria-delivery.purchase.recovered` | A queued delivery succeeded after one or more retries. | `delivery_kind`, `retry_count`, `state` |
| `mysterria-delivery.purchase.queue-cleanup-failed` | Delivery succeeded, but the persisted queue entry could not be removed. | `delivery_kind`, `reason`, `state` |
| `mysterria-delivery.purchase.retry-persistence-failed` | A failed delivery's updated retry count could not be persisted. | `delivery_kind`, `reason`, `state` |

No command text, payment-provider payload, announcement rendering, or queue polling is
recorded. Audit calls are guarded and never decide gameplay or API outcomes.

## Entitlement validation

An explicit expiry must be a valid future date; malformed or expired values fail before
LuckPerms is changed. Invalid date arrays cannot silently become an omitted expiry.
If expiry is absent, the configured positive day duration (or the existing 30-day default)
applies. Complete permission lists and nodes are validated before applying any entry;
mixed or blank values cannot leave an earlier permission partially applied.
