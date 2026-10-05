# MysterriaDelivery audit events

Rows are written by the shaded audit client (producer `mysterria-delivery`) under
`plugins/mysterria-audit-spool`, privacy `STAFF_RESTRICTED`. `business_id` is the purchase ID and the
correlation ID is a UUID derived from it. Command text is never recorded (only `command_count`), except a
rejected vote command on its `failed` row. The commands are not hashed: the details are built from the request
on the main thread for most deliveries, so `commands_sha256` was dropped.

Purchase rows come from the store's REST call or its queued replay. The actor UUID is the recipient (the payload
has no caller identity), so every purchase row also carries `actor_name=store-api`, and `received` rows for purchases add
`store_user_id` (the request's `userId`) when the payload has one. Vote rewards carry no separate store
account (their `userId` is the player's UUID), so their rows have `actor_name` only.

Event types are prefixed `mysterria-delivery.purchase.`. `delivery_kind` is `purchase`, `discord_role`,
`vote_reward`, `subscription` or `permission`; it names the kind only. Queue clean-up rows for entries that were
already completed use `queued_purchase` or `completed_queue_replay` instead. Rows written before this change may
label subscription and permission purchases as `purchase`.

| Event | Outcome | Key facts |
| --- | --- | --- |
| `received` | ATTEMPTED | Once per purchase ID; `service_name` or `source`. |
| `discord-role-requested` | ATTEMPTED | Discord-role purchase observed; the role itself is granted elsewhere. |
| `duplicate-rejected` | DENIED | `state` = `replay_blocked` or `in_progress`. |
| `queued` | COMMITTED | Player offline; emitted after `queue.json` is saved. |
| `delivered` | COMMITTED / FAILED | COMMITTED only after the completion tombstone is saved; FAILED with `reason=completion_unpersisted`, `requires_reconciliation=true` when it is not. Carries the entitlement details (service, quantity, group, permissions or vote source/amount, command count). |
| `failed` | FAILED | `reason`: `invalid_player_uuid`, `invalid_vote_request`, `invalid_quantity`, `invalid_delivery_metadata`, `missing_group`, `missing_permissions`, `no_delivery_commands`, `replay_guard_unavailable`, `intent_not_persisted`, `player_offline`, `scheduling_failed`, `delivery_exception`, `command_rejected`, `command_execution_failed`, `partial_command_delivery`, `partial_permission_delivery`, `queue_persistence_failed`, `queue_writer_saturated`. Command failures add `attempted_count`/`dispatched_count`. |
| `completion-persistence-failed` | FAILED | Effects ran but the tombstone was not saved; `requires_reconciliation=true`. |
| `recovered` | COMMITTED | Queued delivery succeeded after retries; `retry_count`. |
| `retries-exhausted` | FAILED | Once per queue entry; `state` = `removed_from_queue`, `queue_removal_failed` or `retained_in_queue` (vote rewards). |
| `queue-cleanup-failed` | FAILED | Delivered, but the tombstone or queue removal was not saved; `state=delivered_queue_retained`. `partial` (boolean) is true when only part of the delivery was applied. |
| `retry-persistence-failed` | FAILED | Updated retry count could not be saved. |

## Admin events

Prefixed `mysterria-delivery.admin.`. The actor UUID is the staff player; console and RCON have no UUID and are
recorded by `actor_name` (the sender name, for example `CONSOLE`). `command` is the full command line.

| Event | Outcome | Key facts |
| --- | --- | --- |
| `config-reload` | COMMITTED / FAILED | `/delivery reload`. Emitted after the reload finishes; FAILED carries `reason` (exception class) and the exception still propagates. `old_`/`new_` pairs for `announcements_enabled`, `announcement_global`, `max_retries` and `delivery_delay_ticks`. Translation files are reloaded too but not diffed. |
