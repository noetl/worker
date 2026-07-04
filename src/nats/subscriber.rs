//! NATS JetStream subscriber for command notifications.

use anyhow::Result;
use async_nats::jetstream::{
    self, consumer::pull::Config as ConsumerConfig, consumer::Consumer, Context,
};
use async_nats::ConnectOptions;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::Mutex;
use url::Url;

/// The durable pull consumer this subscriber drains command notifications
/// from. `Consumer` is an `Arc`-backed handle — cloning it does not re-hit
/// the server, so the cached handle is cheap to share across `receive` calls.
type PullConsumer = Consumer<ConsumerConfig>;

/// Default server-blocking claim wait. A published command is delivered the
/// instant it lands; this only bounds how long an idle worker blocks before
/// looping. Overridable via `NOETL_NATS_CLAIM_EXPIRES_MS`.
const DEFAULT_CLAIM_EXPIRES_MS: u64 = 2_000;

/// Command notification received from NATS.
///
/// This is a lightweight notification that triggers command fetching.
///
/// `command_id` is normalised to `String` in memory but the wire
/// format accepts either a JSON string OR a JSON integer — the
/// Python broker switched the `noetl.command.command_id` column to
/// `bigint` snowflake and now serialises it as a JSON number on
/// the publish path.  See `deserialize_command_id` below.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandNotification {
    /// Execution ID this command belongs to.
    pub execution_id: i64,

    /// Event ID containing the full command details.
    pub event_id: i64,

    /// Unique command identifier for atomic claiming.  Accepts
    /// JSON string OR integer on the wire; stored as `String` so
    /// downstream call sites (logging, tracing, executor `Command`)
    /// don't need to handle both shapes.
    #[serde(deserialize_with = "deserialize_command_id")]
    pub command_id: String,

    /// Step name this command is for.
    pub step: String,

    /// Server URL for fetching command details.
    pub server_url: String,

    /// Target worker-pool segment the server routed this command to
    /// (`shared` / `system` / a subscription override), mirroring the NATS
    /// subject `noetl.commands.<segment>.<execution_id>` (noetl/ai-meta#108).
    /// `None` for legacy notifications that predate pool stamping. The worker
    /// uses it to decline commands that aren't for its pool — defence-in-depth
    /// against a JetStream consumer whose `filter_subject` drifted broad and so
    /// delivers another pool's commands.
    #[serde(default)]
    pub execution_pool: Option<String>,
}

/// Accept either a JSON string OR a JSON integer for `command_id`;
/// stringify the integer form so the in-memory representation is
/// always `String`.  The Python broker now sends `command_id` as a
/// `bigint` snowflake (numeric JSON literal) but the worker wasn't
/// updated to deserialize it — the `invalid type: integer ...,
/// expected a string` error surfaced this during the EE-3 kind
/// validation pass.
fn deserialize_command_id<'de, D>(deserializer: D) -> std::result::Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{Error, Unexpected};
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(s) => Ok(s),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        other => Err(D::Error::invalid_type(
            match &other {
                serde_json::Value::Null => Unexpected::Unit,
                serde_json::Value::Bool(b) => Unexpected::Bool(*b),
                serde_json::Value::Array(_) => Unexpected::Seq,
                serde_json::Value::Object(_) => Unexpected::Map,
                _ => Unexpected::Other("non-string non-number"),
            },
            &"a JSON string or a JSON integer",
        )),
    }
}

/// NATS JetStream subscriber for command notifications.
pub struct NatsSubscriber {
    /// JetStream context.
    js: Context,

    /// Stream name.
    stream: String,

    /// Consumer name.
    consumer: String,

    /// Subject to subscribe to.
    subject: String,

    /// Cached durable pull-consumer handle (noetl/ai-meta#130). Before this,
    /// every `receive()` re-ran `get_stream` + `get_consumer` (two NATS
    /// round-trips) to rebuild the handle on each command-claim hop. The
    /// handle is resolved once and reused; reset to `None` on any fetch error
    /// so a server restart / consumer recreation self-heals on the next claim.
    consumer_handle: Mutex<Option<PullConsumer>>,

    /// When true (default), `receive()` issues a server-blocking bounded pull
    /// (`batch().expires(claim_expires)`) that returns the instant a command
    /// is published — instead of the legacy NO-WAIT `fetch()` that returns
    /// empty immediately and forces the caller to sleep and re-poll, adding up
    /// to one poll-interval (~100ms) of latency to every claim hop. Set
    /// `NOETL_NATS_BLOCKING_CLAIM=0` (or `false`) to restore the legacy path.
    blocking_claim: bool,

    /// How long a blocking claim waits before returning empty when idle.
    claim_expires: Duration,
}

impl NatsSubscriber {
    /// Connect to NATS and create a subscriber.
    ///
    /// Auth precedence (the Python worker convention matches the
    /// first two):
    /// 1. `NATS_USER` + `NATS_PASSWORD` env vars (explicit; never
    ///    serialised to log lines).
    /// 2. Inline credentials in the URL (`nats://user:pass@host`).
    ///    `async_nats::connect()` only parses the addr portion and
    ///    silently drops URL creds — so we extract them ourselves
    ///    and feed `ConnectOptions::with_user_and_password`.
    /// 3. Anonymous connect (the existing PR-2d-2 behaviour).
    ///
    /// `subject` is the base NATS subject the stream is configured
    /// for; the stream is widened to accept both the bare subject
    /// AND `<subject>.>` to match the Python PR-2a widening
    /// (noetl/ai-meta#42 PR-2a).
    ///
    /// `filter_subject` is the consumer-side filter — if the
    /// deployment env sets `NATS_FILTER_SUBJECT=noetl.commands.shared.>`
    /// the Rust worker only sees shared-segment commands; if unset
    /// it defaults to `subject` (single-consumer behaviour).
    pub async fn connect(
        nats_url: &str,
        stream: &str,
        consumer: &str,
        subject: &str,
        filter_subject: &str,
    ) -> Result<Self> {
        let (clean_url, user, pass) = parse_nats_credentials(nats_url)?;

        let client = if let (Some(u), Some(p)) = (user, pass) {
            tracing::info!(nats_url = %clean_url, "Connecting to NATS with user/password auth");
            ConnectOptions::with_user_and_password(u, p)
                .connect(&clean_url)
                .await?
        } else {
            tracing::info!(nats_url = %clean_url, "Connecting to NATS anonymously");
            async_nats::connect(&clean_url).await?
        };

        let js = jetstream::new(client);

        // Ensure stream exists.  Widen subjects per noetl/ai-meta#42
        // PR-2a parity with the Python side: stream accepts both the
        // bare subject and the hierarchical wildcard so PR-5's
        // cutover (publisher emits `noetl.commands.shared.X`) lands
        // on a configured subject.
        let stream_config = jetstream::stream::Config {
            name: stream.to_string(),
            subjects: vec![subject.to_string(), format!("{}.>", subject)],
            ..Default::default()
        };

        // Try to get existing stream or create new one
        match js.get_stream(stream).await {
            Ok(_) => {
                tracing::debug!(stream = %stream, "Using existing NATS stream");
            }
            Err(_) => {
                js.create_stream(stream_config).await?;
                tracing::info!(stream = %stream, "Created NATS stream");
            }
        }

        // noetl/ai-meta#130: default to the server-blocking claim path so a
        // published command is delivered in ms; opt out to the legacy no-wait
        // fetch + caller-side poll-sleep with NOETL_NATS_BLOCKING_CLAIM=0.
        let blocking_claim = !matches!(
            std::env::var("NOETL_NATS_BLOCKING_CLAIM")
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "0" | "false" | "no" | "off"
        );
        let claim_expires = Duration::from_millis(
            std::env::var("NOETL_NATS_CLAIM_EXPIRES_MS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .filter(|ms| *ms > 0)
                .unwrap_or(DEFAULT_CLAIM_EXPIRES_MS),
        );
        tracing::info!(
            blocking_claim,
            claim_expires_ms = claim_expires.as_millis() as u64,
            "NATS command-claim mode"
        );

        Ok(Self {
            js,
            stream: stream.to_string(),
            consumer: consumer.to_string(),
            subject: filter_subject.to_string(),
            consumer_handle: Mutex::new(None),
            blocking_claim,
            claim_expires,
        })
    }

    /// Return the durable pull-consumer handle, resolving it once and reusing
    /// the cached clone thereafter (noetl/ai-meta#130).
    ///
    /// The handle is cloned out of the cache before the lock is released, so a
    /// blocking drain on it never holds the cache lock.
    async fn cached_consumer(&self) -> Result<PullConsumer> {
        let mut guard = self.consumer_handle.lock().await;
        if let Some(consumer) = guard.as_ref() {
            return Ok(consumer.clone());
        }
        let consumer = self.ensure_consumer().await?;
        *guard = Some(consumer.clone());
        Ok(consumer)
    }

    /// Drop the cached consumer handle so the next claim re-resolves it. Called
    /// after a fetch error — a stale handle (server restart, consumer
    /// recreated) must not be reused or every subsequent claim fails.
    async fn invalidate_consumer(&self) {
        *self.consumer_handle.lock().await = None;
    }

    /// Create or get the durable consumer.
    async fn ensure_consumer(&self) -> Result<PullConsumer> {
        let stream = self.js.get_stream(&self.stream).await?;

        let consumer_config = ConsumerConfig {
            durable_name: Some(self.consumer.clone()),
            filter_subject: self.subject.clone(),
            ..Default::default()
        };

        // Try to get existing consumer or create new one
        match stream.get_consumer(&self.consumer).await {
            Ok(consumer) => Ok(consumer),
            Err(_) => {
                let consumer = stream.create_consumer(consumer_config).await?;
                tracing::info!(consumer = %self.consumer, "Created NATS consumer");
                Ok(consumer)
            }
        }
    }

    /// Receive the next command notification.
    ///
    /// In the default blocking-claim mode this issues a server-blocking
    /// bounded pull that returns the instant a command is published (or empty
    /// after `claim_expires` when idle). In legacy mode it issues a no-wait
    /// fetch that returns empty immediately, leaving the caller to poll-sleep.
    /// Either way it claims at most one message so the worker's per-message
    /// dispatch + ack flow is unchanged.
    pub async fn receive(
        &self,
    ) -> Result<Option<(CommandNotification, async_nats::jetstream::Message)>> {
        let consumer = self.cached_consumer().await?;

        // Server-blocking bounded pull (`batch`) vs legacy no-wait `fetch`.
        let fetched = if self.blocking_claim {
            consumer
                .batch()
                .max_messages(1)
                .expires(self.claim_expires)
                .messages()
                .await
        } else {
            consumer.fetch().max_messages(1).messages().await
        };

        let mut messages = match fetched {
            Ok(m) => m,
            Err(e) => {
                // The cached consumer handle may be stale (server restart /
                // consumer recreated). Drop it so the next claim re-resolves.
                self.invalidate_consumer().await;
                return Err(anyhow::anyhow!("Failed to pull command: {}", e));
            }
        };

        match messages.next().await {
            Some(Ok(msg)) => {
                let notification: CommandNotification = serde_json::from_slice(&msg.payload)?;
                Ok(Some((notification, msg)))
            }
            Some(Err(e)) => {
                self.invalidate_consumer().await;
                Err(anyhow::anyhow!("Failed to receive message: {}", e))
            }
            None => Ok(None),
        }
    }

    /// Acknowledge a message.
    pub async fn ack(&self, msg: &async_nats::jetstream::Message) -> Result<()> {
        msg.ack()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to ack message: {}", e))?;
        Ok(())
    }

    /// Negatively acknowledge a message (will be redelivered).
    pub async fn nack(&self, msg: &async_nats::jetstream::Message) -> Result<()> {
        msg.ack_with(async_nats::jetstream::AckKind::Nak(None))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to nack message: {}", e))?;
        Ok(())
    }

    /// Negatively acknowledge a message with an explicit redelivery delay
    /// (noetl/ai-meta#166 Phase 4). Used by execution-affinity steering: a
    /// replica that pulls a drive command it does not own NAKs it back to
    /// the shared durable consumer with a small delay so the **owning**
    /// replica gets a window to pull the redelivery (and the non-owner does
    /// not hot-spin re-grabbing its own NAK). The delay is advisory —
    /// correctness never depends on it; a `Nak(None)` (immediate) redelivery
    /// would still be correct, just noisier.
    pub async fn nack_with_delay(
        &self,
        msg: &async_nats::jetstream::Message,
        delay: std::time::Duration,
    ) -> Result<()> {
        msg.ack_with(async_nats::jetstream::AckKind::Nak(Some(delay)))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to nack (delayed) message: {}", e))?;
        Ok(())
    }

    /// Stream name this subscriber is bound to.  Exposed so the lag
    /// poller can label its gauges with the same names the consumer
    /// + KEDA see.
    pub fn stream_name(&self) -> &str {
        &self.stream
    }

    /// Consumer name this subscriber is bound to.
    pub fn consumer_name(&self) -> &str {
        &self.consumer
    }

    /// Fetch a snapshot of the consumer's lag state from JetStream.
    ///
    /// Returns `(pending, ack_pending)` — `pending` is the count of
    /// messages still in the stream not yet delivered to any
    /// consumer; `ack_pending` is the count of messages delivered
    /// but awaiting ack.  Together they're the queue-depth signal
    /// KEDA + the dashboard read to decide whether to scale.
    ///
    /// `info()` requires `&mut Consumer`; this method takes the
    /// short path of building a fresh consumer handle each call —
    /// the lag-poll cadence (seconds, not millis) makes the
    /// allocation noise irrelevant.
    pub async fn consumer_lag(&self) -> Result<ConsumerLag> {
        self.consumer_lag_for(&self.stream, &self.consumer).await
    }

    /// Fetch a lag snapshot for an *arbitrary* stream + consumer over
    /// this subscriber's JetStream connection.
    ///
    /// The materializer drains a different stream/consumer pair
    /// (`noetl_events` / `noetl_materializer`) than the command
    /// dispatch loop, and it has no independent lag observer: a stuck
    /// or dead materializer loop can't report its own growing
    /// backlog.  The lag poller — an independent task on the same
    /// worker — calls this with the materializer pair so the
    /// `noetl_worker_nats_consumer_pending{consumer="noetl_materializer"}`
    /// gauge keeps climbing even when the materializer loop has
    /// stalled.  That gauge is the earliest signal that, under the
    /// `NOETL_EVENT_INGEST_PUBLISH_ONLY` gate, published events are
    /// piling up un-materialized (noetl/ai-meta#103 flip guardrail).
    ///
    /// Same JetStream account/connection as the command consumer —
    /// both streams live in the worker's NATS account — so no extra
    /// connection is opened.
    pub async fn consumer_lag_for(
        &self,
        stream_name: &str,
        consumer_name: &str,
    ) -> Result<ConsumerLag> {
        let stream = self.js.get_stream(stream_name).await?;
        // The pull-consumer type is what the subscriber created in
        // `ensure_consumer`; reusing it here keeps the consumer
        // handle compatible with the same generic instantiation.
        let mut consumer: jetstream::consumer::Consumer<jetstream::consumer::pull::Config> = stream
            .get_consumer(consumer_name)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch consumer: {}", e))?;
        let info = consumer
            .info()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to fetch consumer info: {}", e))?;
        Ok(ConsumerLag {
            pending: info.num_pending,
            ack_pending: info.num_ack_pending,
        })
    }
}

/// Parse user/password from the URL OR from
/// `NATS_USER` + `NATS_PASSWORD` env vars and return the URL with
/// inline credentials stripped (so `async_nats::connect` sees a
/// bare addr).
///
/// Resolution order:
/// 1. `NATS_USER` + `NATS_PASSWORD` env (both must be set).
/// 2. Inline `nats://user:pass@host` URL credentials.
/// 3. None → caller falls back to anonymous `async_nats::connect`.
///
/// Returns `(clean_url, user, password)`.  The clean URL drops the
/// userinfo portion so passing it to `async_nats::connect` doesn't
/// silently expose creds via `Debug`-printed `ServerAddr` (the
/// inline form survives URL parsing in `ServerAddr::from_str` even
/// though connector.rs ignores it; stripping is defence-in-depth).
fn parse_nats_credentials(nats_url: &str) -> Result<(String, Option<String>, Option<String>)> {
    // Env-var override first.  Matches the Python worker's behaviour
    // where `NATS_USER` / `NATS_PASSWORD` are the deployment-manifest
    // knobs.
    let env_user = std::env::var("NATS_USER").ok();
    let env_pass = std::env::var("NATS_PASSWORD").ok();
    if let (Some(u), Some(p)) = (env_user.as_ref(), env_pass.as_ref()) {
        if !u.is_empty() && !p.is_empty() {
            let clean = strip_url_credentials(nats_url)?;
            return Ok((clean, Some(u.clone()), Some(p.clone())));
        }
    }

    // Fall back to URL-inline credentials.
    let parsed = Url::parse(nats_url)
        .map_err(|e| anyhow::anyhow!("Invalid NATS_URL '{}': {}", nats_url, e))?;
    let user_in_url = parsed.username();
    let pass_in_url = parsed.password();
    if !user_in_url.is_empty() && pass_in_url.is_some() {
        let user = urlencoding::decode(user_in_url)
            .map(|c| c.into_owned())
            .unwrap_or_else(|_| user_in_url.to_string());
        let pass = urlencoding::decode(pass_in_url.unwrap_or(""))
            .map(|c| c.into_owned())
            .unwrap_or_else(|_| pass_in_url.unwrap_or("").to_string());
        let clean = strip_url_credentials(nats_url)?;
        return Ok((clean, Some(user), Some(pass)));
    }

    // No credentials configured — anonymous connect.  Roundtrip
    // through `strip_url_credentials` for shape consistency with
    // the auth paths (canonical URL form, defence-in-depth).
    let clean = strip_url_credentials(nats_url)?;
    Ok((clean, None, None))
}

/// Strip the `user:password@` userinfo from a URL.  Idempotent.
fn strip_url_credentials(nats_url: &str) -> Result<String> {
    let mut parsed = Url::parse(nats_url)
        .map_err(|e| anyhow::anyhow!("Invalid NATS_URL '{}': {}", nats_url, e))?;
    // Url::set_username("") / set_password(None) drops the userinfo
    // segment so the rendered URL is `nats://host:port/...`.
    let _ = parsed.set_username("");
    let _ = parsed.set_password(None);
    Ok(parsed.to_string())
}

/// Snapshot of a JetStream consumer's lag.  Returned by
/// [`NatsSubscriber::consumer_lag`].
#[derive(Debug, Clone, Copy)]
pub struct ConsumerLag {
    /// Messages still in the stream not yet delivered to any
    /// consumer.  The backlog the worker hasn't seen yet.
    pub pending: u64,

    /// Messages delivered to a consumer but not yet ack'd.
    /// Live in-flight work the worker is processing.
    pub ack_pending: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// URL-inline `user:pass@host` credentials get extracted into
    /// the returned tuple and the URL passed to `async_nats::connect`
    /// has them stripped.  Matches what the Python worker accepts
    /// via the `NATS_URL` env var.
    #[test]
    fn parse_credentials_extracts_inline_url_auth() {
        let url = "nats://noetl:s3cret@nats.example.com:4222";
        let (clean, user, pass) = parse_nats_credentials(url).unwrap();
        assert_eq!(user.as_deref(), Some("noetl"));
        assert_eq!(pass.as_deref(), Some("s3cret"));
        assert!(!clean.contains("noetl:s3cret"));
        assert!(clean.contains("nats.example.com:4222"));
    }

    /// Anonymous URLs surface no credentials and the URL is
    /// preserved (modulo the `url::Url::to_string()` normalisation,
    /// which keeps the addr portion intact — what `async_nats`
    /// actually consumes).
    #[test]
    fn parse_credentials_anonymous_when_no_userinfo() {
        let url = "nats://nats.example.com:4222";
        let (clean, user, pass) = parse_nats_credentials(url).unwrap();
        assert!(user.is_none());
        assert!(pass.is_none());
        assert!(clean.starts_with("nats://nats.example.com:4222"));
        assert!(!clean.contains('@'));
    }

    /// Bad URLs surface a clear error instead of panicking.
    #[test]
    fn parse_credentials_rejects_malformed_url() {
        let err = parse_nats_credentials("not-a-url").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Invalid NATS_URL"), "got: {}", msg);
    }

    /// `%`-encoded URL credentials decode correctly (passwords with
    /// `@` or `:` characters round-trip via percent-encoding).
    #[test]
    fn parse_credentials_decodes_percent_encoded_password() {
        let url = "nats://noetl:s%3Acret%40@nats.example.com:4222";
        let (_, user, pass) = parse_nats_credentials(url).unwrap();
        assert_eq!(user.as_deref(), Some("noetl"));
        assert_eq!(pass.as_deref(), Some("s:cret@"));
    }

    /// `strip_url_credentials` is idempotent — running it on a
    /// URL that already has no userinfo doesn't corrupt it.
    #[test]
    fn strip_url_credentials_idempotent() {
        let stripped = strip_url_credentials("nats://host:4222").unwrap();
        assert_eq!(strip_url_credentials(&stripped).unwrap(), stripped);
    }

    #[test]
    fn test_command_notification_serialization() {
        let notification = CommandNotification {
            execution_id: 12345,
            event_id: 67890,
            command_id: "cmd-abc123".to_string(),
            step: "process_data".to_string(),
            server_url: "http://localhost:8082".to_string(),
            execution_pool: None,
        };

        let json = serde_json::to_string(&notification).unwrap();
        assert!(json.contains("12345"));
        assert!(json.contains("cmd-abc123"));

        let parsed: CommandNotification = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.execution_id, 12345);
    }

    /// `command_id` deserializes from a JSON integer (the current
    /// Python broker wire form — `bigint` snowflake serialised as a
    /// numeric literal).  EE-3 kind validation surfaced this — the
    /// worker was failing with `invalid type: integer ..., expected
    /// a string` on every command notification.
    #[test]
    fn command_notification_deserialises_numeric_command_id() {
        let wire = serde_json::json!({
            "execution_id": 12345,
            "event_id": 67890,
            "command_id": 638756237806404289i64,
            "step": "greet",
            "server_url": "http://localhost:8082"
        });
        let parsed: CommandNotification = serde_json::from_value(wire).unwrap();
        assert_eq!(parsed.command_id, "638756237806404289");
        assert_eq!(parsed.execution_id, 12345);
    }

    /// String form still works (backward compat for any older broker
    /// builds + the worker's own serialised wire format).
    #[test]
    fn command_notification_deserialises_string_command_id() {
        let wire = serde_json::json!({
            "execution_id": 12345,
            "event_id": 67890,
            "command_id": "cmd-abc123",
            "step": "greet",
            "server_url": "http://localhost:8082"
        });
        let parsed: CommandNotification = serde_json::from_value(wire).unwrap();
        assert_eq!(parsed.command_id, "cmd-abc123");
    }

    /// Anything other than string/number on `command_id` produces
    /// a clear deserialization error.
    #[test]
    fn command_notification_rejects_non_string_non_number_command_id() {
        let wire = serde_json::json!({
            "execution_id": 12345,
            "event_id": 67890,
            "command_id": null,
            "step": "greet",
            "server_url": "http://localhost:8082"
        });
        let err = serde_json::from_value::<CommandNotification>(wire).unwrap_err();
        assert!(err.to_string().contains("string or"), "got: {}", err);
    }

    /// Live-server proof (noetl/ai-meta#130) that the default blocking-claim
    /// path delivers a command shortly after it is published — not after a
    /// fixed poll interval — AND that the consumer handle is cached across the
    /// claim (no per-receive `get_stream` + `get_consumer` rebuild).
    ///
    /// Set `NOETL_TEST_NATS_URL=nats://localhost:4222` to run.
    #[tokio::test]
    async fn blocking_claim_delivers_published_command_promptly() {
        let url = match std::env::var("NOETL_TEST_NATS_URL") {
            Ok(u) => u,
            Err(_) => return, // skip without a live NATS
        };

        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let stream = format!("claim_test_{suffix}");
        let subject = format!("noetl.commands.claim_{suffix}");
        let consumer = format!("claim_c_{suffix}");

        // Blocking mode is the default; be explicit so the test is robust to a
        // leaked opt-out env var from another test in the same process.
        std::env::set_var("NOETL_NATS_BLOCKING_CLAIM", "1");

        let sub = NatsSubscriber::connect(&url, &stream, &consumer, &subject, &subject)
            .await
            .expect("connect subscriber");

        // Publish a command notification ~50ms after we start blocking, so the
        // claim is genuinely waiting when it lands.
        let pub_url = url.clone();
        let pub_subject = subject.clone();
        let publisher = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let client = async_nats::connect(&pub_url).await.unwrap();
            let js = jetstream::new(client);
            let payload = serde_json::to_vec(&serde_json::json!({
                "execution_id": 1i64,
                "event_id": 2i64,
                "command_id": 3i64,
                "step": "s",
                "server_url": "http://localhost:8082",
            }))
            .unwrap();
            js.publish(pub_subject, payload.into())
                .await
                .unwrap()
                .await
                .unwrap();
        });

        let start = std::time::Instant::now();
        let got = sub.receive().await.expect("receive");
        let elapsed = start.elapsed();
        publisher.await.unwrap();

        let (notification, msg) = got.expect("blocking claim should deliver the command");
        assert_eq!(notification.command_id, "3");
        msg.ack().await.expect("ack");

        // Returned promptly after the ~50ms publish, not after a long poll.
        assert!(
            elapsed < Duration::from_millis(800),
            "claim took {elapsed:?}; blocking pull should return shortly after publish"
        );
        // Handle cached for reuse on the next claim hop.
        assert!(
            sub.consumer_handle.lock().await.is_some(),
            "consumer handle must be cached after a claim"
        );

        // Cleanup.
        let client = async_nats::connect(&url).await.unwrap();
        let _ = jetstream::new(client).delete_stream(&stream).await;
    }
}
