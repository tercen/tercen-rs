#![allow(dead_code)]
use crate::client::proto::ReqStreamTable;
use crate::client::TercenClient;
use crate::error::{Result, TercenError};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio_stream::StreamExt;

/// Schema cache type alias for reuse across pages
/// Key: table_id, Value: cached schema
pub type SchemaCache = Arc<Mutex<HashMap<String, crate::client::proto::ESchema>>>;

/// Create a new empty schema cache
pub fn new_schema_cache() -> SchemaCache {
    Arc::new(Mutex::new(HashMap::new()))
}

/// Tercen table data streamer
pub struct TableStreamer<'a> {
    client: &'a TercenClient,
    /// Optional schema cache (shared across multiple streamers)
    schema_cache: Option<SchemaCache>,
}

impl<'a> TableStreamer<'a> {
    /// Create a new table streamer without caching
    pub fn new(client: &'a TercenClient) -> Self {
        TableStreamer {
            client,
            schema_cache: None,
        }
    }

    /// Create a new table streamer with schema caching
    ///
    /// The cache is shared across multiple streamers, so schemas fetched
    /// for one page are reused for subsequent pages.
    pub fn with_cache(client: &'a TercenClient, cache: SchemaCache) -> Self {
        TableStreamer {
            client,
            schema_cache: Some(cache),
        }
    }

    /// Get the schema for a table to retrieve metadata like row count
    ///
    /// If a schema cache was provided via `with_cache`, this will check
    /// the cache first and only make a network request on cache miss.
    /// Cached schemas are reused across pages in multi-page plots.
    pub async fn get_schema(&self, table_id: &str) -> Result<crate::client::proto::ESchema> {
        // Check cache first
        if let Some(ref cache) = self.schema_cache {
            let guard = cache.lock().unwrap();
            if let Some(schema) = guard.get(table_id) {
                eprintln!("DEBUG: Schema cache HIT for table {}", table_id);
                return Ok(schema.clone());
            }
        }

        eprintln!("DEBUG: Schema cache MISS for table {} - fetching", table_id);

        use crate::client::proto::GetRequest;

        let mut table_service = self.client.table_service()?;
        let request = tonic::Request::new(GetRequest {
            id: table_id.to_string(),
            ..Default::default()
        });

        let response = table_service
            .get(request)
            .await
            .map_err(|e| TercenError::Grpc(Box::new(e)))?;

        let schema = response.into_inner();

        // Populate cache
        if let Some(ref cache) = self.schema_cache {
            let mut guard = cache.lock().unwrap();
            guard.insert(table_id.to_string(), schema.clone());
        }

        Ok(schema)
    }

    pub async fn stream_tson(
        &self,
        table_id: &str,
        columns: Option<Vec<String>>,
        offset: i64,
        limit: i64,
    ) -> Result<Vec<u8>> {
        let mut table_service = self.client.table_service()?;

        let request = tonic::Request::new(ReqStreamTable {
            table_id: table_id.to_string(),
            cnames: columns.unwrap_or_default(),
            offset,
            limit,
            binary_format: String::new(), // Empty = TSON format (default)
        });

        let mut stream = table_service
            .stream_table(request)
            .await
            .map_err(|e| TercenError::Grpc(Box::new(e)))?
            .into_inner();

        let mut all_data = Vec::new();

        while let Some(chunk_result) = stream.next().await {
            match chunk_result {
                Ok(chunk) => {
                    all_data.extend_from_slice(&chunk.result);
                }
                Err(e) => return Err(TercenError::Grpc(Box::new(e))),
            }
        }

        Ok(all_data)
    }

    /// Stream entire table in chunks, calling callback for each chunk
    ///
    /// # Arguments
    /// * `table_id` - The Tercen table ID to stream
    /// * `columns` - Optional list of columns to fetch
    /// * `chunk_size` - Number of rows per chunk
    /// * `callback` - Function to call with each TSON chunk
    pub async fn stream_table_chunked<F>(
        &self,
        table_id: &str,
        columns: Option<Vec<String>>,
        chunk_size: i64,
        mut callback: F,
    ) -> Result<()>
    where
        F: FnMut(Vec<u8>) -> Result<()>,
    {
        let mut offset = 0;

        loop {
            let chunk = self
                .stream_tson(table_id, columns.clone(), offset, chunk_size)
                .await?;

            if chunk.is_empty() {
                break;
            }

            callback(chunk)?;

            offset += chunk_size;
        }

        Ok(())
    }
}
