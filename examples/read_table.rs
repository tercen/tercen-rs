//! Read a table through `tson_to_dataframe`, reporting rows, time and bytes.
//!
//!     TERCEN_URI=… TERCEN_TOKEN=… cargo run --example read_table -- <tableId> <cells> [chunk]
//!
//! Before the multi-document fix, a large `chunk` transferred far more than it used, because a
//! response holds one TSON document per server page and only the first was decoded.
use std::sync::Arc;
use std::time::Instant;

use tercen_rs::TercenClient;

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (table, cells) = (a[1].clone(), a[2].parse::<usize>().unwrap());
    let chunk: usize = a.get(3).and_then(|v| v.parse().ok()).unwrap_or(200_000);
    let client = Arc::new(TercenClient::from_env().await.expect("connect"));
    let streamer = tercen_rs::table::TableStreamer::new(&client);

    let (mut rows, mut bytes) = (0usize, 0usize);
    let t0 = Instant::now();
    let mut offset = 0usize;
    while offset < cells {
        let want = chunk.min(cells - offset);
        let raw = streamer
            .stream_tson(
                &table,
                Some(vec![".ri".into(), ".ci".into(), ".y".into()]),
                offset as i64,
                want as i64,
            )
            .await
            .expect("stream");
        bytes += raw.len();
        let df = tercen_rs::tson_convert::tson_to_dataframe(&raw).expect("decode");
        if df.height() == 0 {
            break;
        }
        rows += df.height();
        offset += df.height();
    }
    let secs = t0.elapsed().as_secs_f64();
    println!(
        "{rows} rows in {secs:.1}s ({:.0} rows/s), {:.1} MB over the wire ({:.0} B/row)",
        rows as f64 / secs,
        bytes as f64 / 1e6,
        bytes as f64 / rows.max(1) as f64
    );
}
