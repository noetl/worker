//! noetl/ai-meta#249 — publish a fabricated command-bus notification.
//!
//! Usage: poison_inject <ingest_addr> <execution_id> <command_id> <step>
//!
//! Subject derivation (ehdb_feed::d1_command_subject): pool comes from
//! `execution_pool` INSIDE the payload JSON (default "shared"); shard comes from
//! `shard_for_execution(record.execution_id, shard_count)` — the RECORD field,
//! not the payload one.  Both must be right or the record lands on a subject the
//! worker does not watch, which is indistinguishable from "the poison did not
//! wedge".
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a: Vec<String> = env::args().collect();
    if a.len() < 5 {
        eprintln!("usage: poison_inject <addr> <execution_id> <command_id> <step>");
        std::process::exit(2);
    }
    let (addr, exec_id, command_id, step) = (&a[1], &a[2], &a[3], &a[4]);

    let payload = serde_json::json!({
        "execution_id": exec_id.parse::<i64>()?,
        "event_id": exec_id.parse::<i64>()?,
        "command_id": command_id,
        "step": step,
        "server_url": "http://noetl.noetl.svc.cluster.local:8082",
        "execution_pool": "shared",
    })
    .to_string();

    let rec = ehdb_l0::EventRecord::new(
        0,                // writer assigns the real sort key (noetl/ai-meta#203)
        exec_id.clone(),  // -> shard_for_execution(...) -> shard 0 when shard_count=1
        "command.issued", // transaction_id
        payload.clone(),
    );

    let mut client = ehdb_feed::PublishClient::connect(addr.as_str()).await?;
    let sort_key = client.publish(&rec).await?;
    println!("PUBLISHED sort_key={sort_key} execution_id={exec_id} command_id={command_id}");
    println!("PAYLOAD {payload}");
    Ok(())
}
