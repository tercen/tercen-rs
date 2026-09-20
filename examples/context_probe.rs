//! Load a production context for a task, printing what it found.
//!
//!     TERCEN_URI=http://host:50051 TERCEN_TOKEN=… \
//!     WORKFLOW_ID=… STEP_ID=… cargo run --example context_probe -- <taskId>
//!
//! `STEP_ID` is deliberately settable: pointing it at a step the workflow does not contain
//! reproduces what an operator sees when the saved workflow has not caught up with an edit.
use std::sync::Arc;
use tercen_rs::{ProductionContext, TercenClient, TercenContext};

#[tokio::main]
async fn main() {
    let task_id = std::env::args().nth(1).expect("usage: context_probe <taskId>");
    let client = Arc::new(TercenClient::from_env().await.expect("connect"));
    let data_only = std::env::var("DATA_ONLY").is_ok();
    let loaded = if data_only {
        ProductionContext::from_task_id_data_only(client, &task_id).await
    } else {
        ProductionContext::from_task_id(client, &task_id).await
    };
    match loaded {
        Ok(ctx) => {
            println!(
                "OK  namespace={} qt={} schemas={} colors={}",
                ctx.namespace(),
                ctx.cube_query().qt_hash,
                ctx.schema_ids().len(),
                ctx.color_infos().len()
            );
            if let Some(why) = ctx.visuals_error() {
                println!("    no presentation settings: {why}");
            }
        }
        Err(e) => println!("ERR {e}"),
    }
}
