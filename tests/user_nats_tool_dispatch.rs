//! **The user-facing `nats` tool must still dispatch after the internal-NATS
//! removal (noetl/ai-meta#212, #220).**
//!
//! The internal command / event / KV bus is EHDB and no longer speaks NATS. But
//! `nats` is also a **user-facing tool kind** — a playbook step can point it at
//! the user's *own* broker to store or publish business data, exactly as the
//! `kafka`, `postgres`, and object-store kinds do. That path is a different
//! thing from the bus and had to survive the removal intact.
//!
//! The risk this test pins down: the worker builds its tool registry with
//! [`create_default_registry`], the same call `executor::command` makes. If the
//! removal had amputated the shared client code the tool leans on, the kind
//! would silently stop being registered, or fail at the first connect — and a
//! playbook using it would break with no compile error to warn anyone.
//!
//! Run the live half with a user-provided broker:
//!
//! ```text
//! NOETL_TEST_NATS_URL=nats://localhost:14223 cargo test --test user_nats_tool_dispatch
//! ```
//!
//! Without that variable the live half skips, so this stays CI-safe.

use noetl_tools::registry::ToolConfig;
use noetl_tools::result::ToolStatus;
use noetl_tools::tools::create_default_registry;
use noetl_tools::ExecutionContext;

fn cfg(op: &str, url: &str, bucket: &str, extra: serde_json::Value) -> ToolConfig {
    let mut config = serde_json::json!({
        "url": url,
        "operation": op,
        "bucket": bucket,
    });
    if let (Some(base), Some(extra)) = (config.as_object_mut(), extra.as_object()) {
        for (k, v) in extra {
            base.insert(k.clone(), v.clone());
        }
    }
    ToolConfig {
        kind: "nats".to_string(),
        config,
        timeout: None,
        retry: None,
        auth: None,
    }
}

/// The kind is registered in the worker's own registry. This is the check that
/// would have caught an accidental de-registration during the removal.
#[test]
fn nats_kind_is_registered_alongside_the_other_user_data_tools() {
    let registry = create_default_registry();
    assert!(
        registry.has("nats"),
        "the user-facing `nats` tool kind must stay registered after the \
         internal NATS bus removal"
    );
    // Its peers — the point being that `nats` is one of several user-supplied
    // external data/queue services, not platform infrastructure.
    for peer in ["postgres", "http", "duckdb"] {
        assert!(registry.has(peer), "{peer} should be registered");
    }
}

/// Full round trip against a **user-provided** broker: write business data,
/// read it back, and enumerate the bucket.
#[tokio::test]
async fn nats_tool_round_trips_business_data_against_a_user_endpoint() {
    let Ok(url) = std::env::var("NOETL_TEST_NATS_URL") else {
        eprintln!("skipping: set NOETL_TEST_NATS_URL to run the live half");
        return;
    };

    // The user creates their own bucket in their own broker; the tool opens it.
    let nc = async_nats::connect(&url)
        .await
        .expect("connect to user broker");
    let js = async_nats::jetstream::new(nc);
    let bucket = format!("worker_dispatch_{}", std::process::id());
    js.create_key_value(async_nats::jetstream::kv::Config {
        bucket: bucket.clone(),
        ..Default::default()
    })
    .await
    .expect("create bucket");

    let registry = create_default_registry();
    let ctx = ExecutionContext::default();

    let put = registry
        .execute(
            "nats",
            &cfg(
                "kv_put",
                &url,
                &bucket,
                serde_json::json!({"key": "order-42", "value": {"order_id": 42, "customer": "acme-corp"}}),
            ),
            &ctx,
        )
        .await
        .expect("kv_put through the worker's registry");
    assert_eq!(
        put.status,
        ToolStatus::Success,
        "kv_put should succeed: {put:?}"
    );

    let got = registry
        .execute(
            "nats",
            &cfg(
                "kv_get",
                &url,
                &bucket,
                serde_json::json!({"key": "order-42"}),
            ),
            &ctx,
        )
        .await
        .expect("kv_get through the worker's registry");
    assert_eq!(
        got.status,
        ToolStatus::Success,
        "kv_get should succeed: {got:?}"
    );

    // The value must come back intact — a tool that connects but round-trips
    // nothing is the failure mode a success flag alone would hide.
    let body = serde_json::to_string(&got.data).unwrap_or_default();
    assert!(
        body.contains("acme-corp") && body.contains("42"),
        "the stored business data must round-trip; got: {body}"
    );

    let keys = registry
        .execute(
            "nats",
            &cfg("kv_keys", &url, &bucket, serde_json::json!({})),
            &ctx,
        )
        .await
        .expect("kv_keys through the worker's registry");
    assert_eq!(
        keys.status,
        ToolStatus::Success,
        "kv_keys should succeed: {keys:?}"
    );
    assert!(
        serde_json::to_string(&keys.data)
            .unwrap_or_default()
            .contains("order-42"),
        "the bucket listing should contain the key we wrote"
    );

    let _ = js.delete_key_value(&bucket).await;
}
