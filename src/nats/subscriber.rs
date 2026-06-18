//! NATS JetStream subscriber for command notifications.

use anyhow::Result;
use async_nats::jetstream::{self, consumer::pull::Config as ConsumerConfig, Context};
use async_nats::ConnectOptions;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use url::Url;

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

        Ok(Self {
            js,
            stream: stream.to_string(),
            consumer: consumer.to_string(),
            subject: filter_subject.to_string(),
        })
    }

    /// Create or get the durable consumer.
    async fn ensure_consumer(
        &self,
    ) -> Result<jetstream::consumer::Consumer<jetstream::consumer::pull::Config>> {
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
    /// This blocks until a message is available or the operation times out.
    pub async fn receive(
        &self,
    ) -> Result<Option<(CommandNotification, async_nats::jetstream::Message)>> {
        let consumer = self.ensure_consumer().await?;

        // Fetch one message with a timeout
        let mut messages = consumer.fetch().max_messages(1).messages().await?;

        if let Some(msg) = messages.next().await {
            let msg = msg.map_err(|e| anyhow::anyhow!("Failed to receive message: {}", e))?;
            let notification: CommandNotification = serde_json::from_slice(&msg.payload)?;
            return Ok(Some((notification, msg)));
        }

        Ok(None)
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
        let stream = self.js.get_stream(&self.stream).await?;
        // The pull-consumer type is what the subscriber created in
        // `ensure_consumer`; reusing it here keeps the consumer
        // handle compatible with the same generic instantiation.
        let mut consumer: jetstream::consumer::Consumer<jetstream::consumer::pull::Config> = stream
            .get_consumer(&self.consumer)
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
}
