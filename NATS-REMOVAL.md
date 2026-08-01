# NATS removal — operator notes (worker)

The NATS command path is gone (noetl/ai-meta#212).

- **`NOETL_FEED_FILTER_SUBJECT` is required.** The legacy `NATS_FILTER_SUBJECT`
  fallback has been removed. This value is what the worker's **EHDB pool** is
  derived from (`noetl.commands.system.>` → pool `system`), and a worker that
  cannot resolve a pool now **refuses to start** rather than silently joining
  `shared` — which is what previously stalled every execution with nothing in
  the logs (noetl/ai-meta#218).
- **`NOETL_COMMAND_BUS` must be `ehdb`.** Any other value is a startup error.
- Consumer lag no longer comes from a NATS poller; read it from the writer's
  `/metrics` (`ehdb_feed_shard_lag`, `ehdb_events_group_lag`), which is where
  KEDA already reads it.

`CommandNotification`, `segment_from_filter` and `claim_outcome` moved to
`src/dispatch.rs` — they were NATS-named but always on the live path.
