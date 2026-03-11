//! Result upload module for saving operator results back to Tercen
//!
//! This module handles the complete flow of saving a generated PNG plot
//! back to Tercen so it can be displayed in the workflow UI.
//!
//! Flow (following Python client pattern):
//! 1. PNG bytes → Base64 string
//! 2. Create DataFrame with .content, filename, mimetype columns
//! 3. Convert DataFrame → Tercen Table (with TSON encoding)
//! 4. Serialize to Sarno-compatible TSON format
//! 5. Upload via TableSchemaService.uploadTable()
//! 6. Create NEW RunComputationTask with fileResultId and original query
//! 7. Submit task via TaskService.create()
//! 8. Run task via TaskService.runTask()
//! 9. Wait for completion via TaskService.waitDone()
//! 10. Server automatically creates computedRelation linking result to step

use crate::client::proto;
use crate::client::TercenClient;
use crate::table_convert;
use polars::prelude::*;
use std::sync::Arc;

/// Struct representing a single plot result for multi-page uploads
pub struct PlotResult {
    /// Page label (e.g., "female", "male")
    pub label: String,
    /// Plot image bytes (PNG or SVG)
    pub png_buffer: Vec<u8>,
    /// Plot width in pixels
    pub width: i32,
    /// Plot height in pixels
    pub height: i32,
    /// Page factor values as key-value pairs (e.g., [("Gender", "female")])
    pub page_factors: Vec<(String, String)>,
    /// File extension: "png" or "svg"
    pub output_ext: String,
    /// Base filename without extension (e.g., "plot" or "myplot")
    pub filename: String,
}

/// Get MIME type from file extension
fn mimetype_for_ext(ext: &str) -> &'static str {
    match ext {
        "svg" => "image/svg+xml",
        _ => "image/png",
    }
}

/// Save multiple PNG plot results back to Tercen
///
/// Each plot gets its own row in the result table, with page factor columns
/// to identify which page it belongs to.
///
/// # Arguments
/// * `client` - Tercen client for gRPC calls
/// * `project_id` - Project ID to upload the result to
/// * `namespace` - Operator namespace for prefixing column names
/// * `plots` - Vector of PlotResult structs
/// * `task` - Mutable reference to the task
pub async fn save_results(
    client: Arc<TercenClient>,
    project_id: &str,
    namespace: &str,
    plots: Vec<PlotResult>,
    task: &mut proto::ETask,
) -> Result<(), Box<dyn std::error::Error>> {
    use base64::Engine;

    println!("Encoding {} plots to base64...", plots.len());

    // Build vectors for each column
    let mut ci_vec: Vec<i32> = Vec::new();
    let mut ri_vec: Vec<i32> = Vec::new();
    let mut content_vec: Vec<String> = Vec::new();
    let mut filename_vec: Vec<String> = Vec::new();
    let mut mimetype_vec: Vec<String> = Vec::new();
    let mut width_vec: Vec<f64> = Vec::new();
    let mut height_vec: Vec<f64> = Vec::new();

    // Collect all unique page factor names
    let mut page_factor_names: Vec<String> = Vec::new();
    if let Some(first_plot) = plots.first() {
        for (name, _) in &first_plot.page_factors {
            page_factor_names.push(name.clone());
        }
    }

    // Page factor value columns (one vec per factor)
    let mut page_factor_values: Vec<Vec<String>> = vec![Vec::new(); page_factor_names.len()];

    for (idx, plot) in plots.iter().enumerate() {
        let base64_png = base64::engine::general_purpose::STANDARD.encode(&plot.png_buffer);
        println!(
            "  Plot {}: {} -> {} bytes (base64: {})",
            idx + 1,
            plot.label,
            plot.png_buffer.len(),
            base64_png.len()
        );

        // Add row data
        ci_vec.push(0);
        ri_vec.push(idx as i32);
        content_vec.push(base64_png);
        filename_vec.push(format!(
            "{}_{}.{}",
            plot.filename, plot.label, plot.output_ext
        ));
        mimetype_vec.push(mimetype_for_ext(&plot.output_ext).to_string());
        width_vec.push(plot.width as f64);
        height_vec.push(plot.height as f64);

        // Add page factor values
        for (i, (_, value)) in plot.page_factors.iter().enumerate() {
            if i < page_factor_values.len() {
                page_factor_values[i].push(value.clone());
            }
        }
    }

    // Create DataFrame
    println!("Creating result DataFrame with {} rows...", plots.len());
    let mut df = df! {
        ".ci" => ci_vec,
        ".ri" => ri_vec,
        ".content" => content_vec,
        &format!("{}.filename", namespace) => filename_vec,
        &format!("{}.mimetype", namespace) => mimetype_vec,
        &format!("{}.plot_width", namespace) => width_vec,
        &format!("{}.plot_height", namespace) => height_vec
    }?;

    // Add page factor columns
    for (i, name) in page_factor_names.iter().enumerate() {
        let col_name = format!("{}.{}", namespace, name);
        let values = &page_factor_values[i];
        let series = Series::new(col_name.into(), values);
        df.with_column(series)?;
    }

    println!("  DataFrame: {} rows, {} columns", df.height(), df.width());

    // Convert to Table and upload (same as single result)
    let table = dataframe_to_table(&df)?;
    let operator_result = create_operator_result(table)?;
    let result_bytes = serialize_operator_result(&operator_result)?;
    let file_doc = create_file_document(project_id, result_bytes.len() as i32);

    let existing_file_result_id = get_task_file_result_id(task)?;

    if existing_file_result_id.is_empty() {
        println!("Uploading result file (webapp scenario)...");
        let file_doc_id = upload_result_file(&client, file_doc, result_bytes).await?;
        println!("  Uploaded file with ID: {}", file_doc_id);

        update_task_file_result_id(task, &file_doc_id)?;
        let mut task_service = client.task_service()?;
        task_service.update(task.clone()).await?;
        println!("Result uploaded - exiting for server to process");
    } else {
        println!(
            "Uploading to existing result file: {}",
            existing_file_result_id
        );
        let mut file_service = client.file_service()?;
        let get_req = proto::GetRequest {
            id: existing_file_result_id.clone(),
            ..Default::default()
        };
        let e_file_doc = file_service.get(get_req).await?.into_inner();
        use proto::e_file_document;
        let file_doc_obj = e_file_doc.object.ok_or("EFileDocument has no object")?;
        let e_file_document::Object::Filedocument(file_doc) = file_doc_obj;
        upload_result_file(&client, file_doc, result_bytes).await?;
        println!("Result uploaded - exiting normally");
    }

    Ok(())
}

/// Save a tabular result (DataFrame) back to Tercen
///
/// Used by computation operators (mean, sum, PCA, etc.) that produce
/// numeric/string columns rather than image files.
///
/// The DataFrame should contain `.ci` and `.ri` columns (cell indices)
/// plus any computed output columns (e.g., `value`, `mean_y`, `mean_x`).
///
/// # Arguments
/// * `client` - Tercen client for gRPC calls
/// * `project_id` - Project ID to upload the result to
/// * `df` - Polars DataFrame with the computed results
/// * `task` - Mutable reference to the task
pub async fn save_table(
    client: Arc<TercenClient>,
    project_id: &str,
    df: &polars::frame::DataFrame,
    task: &mut proto::ETask,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Saving table result: {} rows, {} columns",
        df.height(),
        df.width()
    );

    let table = dataframe_to_table(df)?;
    let operator_result = create_operator_result(table)?;
    let result_bytes = serialize_operator_result(&operator_result)?;
    println!("  TSON size: {} bytes", result_bytes.len());

    let file_doc = create_file_document(project_id, result_bytes.len() as i32);
    let existing_file_result_id = get_task_file_result_id(task)?;

    if existing_file_result_id.is_empty() {
        let file_doc_id = upload_result_file(&client, file_doc, result_bytes).await?;
        update_task_file_result_id(task, &file_doc_id)?;
        let mut task_service = client.task_service()?;
        task_service.update(task.clone()).await?;
        println!("Table result uploaded (file ID: {})", file_doc_id);
    } else {
        let mut file_service = client.file_service()?;
        let get_req = proto::GetRequest {
            id: existing_file_result_id.clone(),
            ..Default::default()
        };
        let e_file_doc = file_service.get(get_req).await?.into_inner();
        use proto::e_file_document;
        let file_doc_obj = e_file_doc.object.ok_or("EFileDocument has no object")?;
        let e_file_document::Object::Filedocument(file_doc) = file_doc_obj;
        upload_result_file(&client, file_doc, result_bytes).await?;
        println!("Table result uploaded to existing file: {}", existing_file_result_id);
    }

    Ok(())
}

/// Save a PNG plot result back to Tercen
///
/// Takes the generated PNG buffer, converts it to Tercen's result format,
/// uploads it, updates the existing task with the fileResultId, and waits
/// for the server to process the result and link it to the workflow step.
///
/// This follows the Python production client pattern (OperatorContext.save):
/// 1. Upload file via FileService.upload() → get FileDocument ID
/// 2. Update EXISTING task's fileResultId field
/// 3. Call TaskService.update() to save the updated task
/// 4. Call TaskService.waitDone() to wait for server processing
/// 5. Server (Sarno) processes file and creates computedRelation automatically
///
/// # Arguments
/// * `client` - Tercen client for gRPC calls
/// * `project_id` - Project ID to upload the result to
/// * `namespace` - Operator namespace for prefixing column names
/// * `png_buffer` - Raw PNG bytes from the renderer
/// * `plot_width` - Width of the plot in pixels
/// * `plot_height` - Height of the plot in pixels
/// * `task` - Mutable reference to the task (will be updated with fileResultId)
///
/// # Returns
/// Result indicating success or error during upload
#[allow(clippy::too_many_arguments)]
pub async fn save_result(
    client: Arc<TercenClient>,
    project_id: &str,
    namespace: &str,
    png_buffer: Vec<u8>,
    plot_width: i32,
    plot_height: i32,
    output_ext: &str,
    filename: &str,
    task: &mut proto::ETask,
) -> Result<(), Box<dyn std::error::Error>> {
    use base64::Engine;

    println!("Encoding plot to base64...");
    // 1. Encode to base64
    let base64_png = base64::engine::general_purpose::STANDARD.encode(&png_buffer);
    println!(
        "  Plot size: {} bytes, base64 size: {} bytes",
        png_buffer.len(),
        base64_png.len()
    );

    // 2. Create result DataFrame with namespace-prefixed columns
    println!("Creating result DataFrame...");
    let filename = format!("{}.{}", filename, output_ext);
    let mimetype = mimetype_for_ext(output_ext);
    let result_df = create_result_dataframe(
        base64_png,
        namespace,
        plot_width,
        plot_height,
        &filename,
        mimetype,
    )?;
    println!(
        "  DataFrame: {} rows, {} columns",
        result_df.height(),
        result_df.width()
    );

    // 3. Convert to Table
    println!("Converting DataFrame to Table...");
    let table = dataframe_to_table(&result_df)?;
    println!(
        "  Table: {} rows, {} columns",
        table.n_rows,
        table.columns.len()
    );

    // 4. Wrap table in OperatorResult structure
    println!("Creating OperatorResult...");
    let operator_result = create_operator_result(table)?;

    // 5. Serialize OperatorResult to TSON
    println!("Serializing OperatorResult to TSON...");
    let result_bytes = serialize_operator_result(&operator_result)?;
    println!("  TSON size: {} bytes", result_bytes.len());

    // 5. Create FileDocument
    println!("Creating FileDocument...");
    let file_doc = create_file_document(project_id, result_bytes.len() as i32);

    // 6. Check if task already has a fileResultId (normal operator flow)
    let existing_file_result_id = get_task_file_result_id(task)?;

    if existing_file_result_id.is_empty() {
        // Webapp/test scenario: Create new file and update task
        println!("Uploading result file (webapp scenario)...");
        let file_doc_id = upload_result_file(&client, file_doc, result_bytes).await?;
        println!("  Uploaded file with ID: {}", file_doc_id);

        println!("Updating task with fileResultId...");
        update_task_file_result_id(task, &file_doc_id)?;
        println!("  Task fileResultId set to: {}", file_doc_id);

        println!("Saving updated task...");
        let mut task_service = client.task_service()?;
        let update_response = task_service.update(task.clone()).await?;
        let _updated_task = update_response.into_inner();
        println!("  Task updated");

        // Note: Python calls waitDone() here in webapp scenario
        // We should exit cleanly and let the task runner process the result
        println!("Result uploaded - exiting for server to process");
    } else {
        // Normal operator scenario: Upload to existing file
        println!(
            "Uploading to existing result file: {}",
            existing_file_result_id
        );

        // Get the existing FileDocument
        let mut file_service = client.file_service()?;
        let get_req = proto::GetRequest {
            id: existing_file_result_id.clone(),
            ..Default::default()
        };
        let e_file_doc = file_service.get(get_req).await?.into_inner();

        // Extract FileDocument
        use proto::e_file_document;
        let file_doc_obj = e_file_doc.object.ok_or("EFileDocument has no object")?;
        let e_file_document::Object::Filedocument(file_doc) = file_doc_obj;

        // Upload to existing file (overwrites content)
        upload_result_file(&client, file_doc, result_bytes).await?;
        println!("  Uploaded to existing file");

        // No update(), no waitDone() - just exit cleanly
        println!("Result uploaded - exiting normally");
    }

    Ok(())
}

/// Create a result DataFrame with base64-encoded PNG
///
/// Creates a DataFrame with columns matching R plot_operator output:
/// - .ci: Column facet index (int32, value 0 for single plot)
/// - .ri: Row facet index (int32, value 0 for single plot)
/// - .content: Base64-encoded PNG bytes (chunked if > 1MB)
/// - {namespace}.filename: "plot.png" (namespace-prefixed by operator)
/// - {namespace}.mimetype: "image/png" (namespace-prefixed by operator)
/// - {namespace}.plot_width: plot width in pixels (namespace-prefixed by operator)
/// - {namespace}.plot_height: plot height in pixels (namespace-prefixed by operator)
///
/// If the base64 string is larger than 1MB, it will be split into multiple rows
/// with the same .ci and .ri values.
///
/// Note: .ci, .ri, and .content have leading dots. Other columns get namespace prefix.
fn create_result_dataframe(
    png_base64: String,
    namespace: &str,
    plot_width: i32,
    plot_height: i32,
    filename: &str,
    mimetype: &str,
) -> Result<DataFrame, Box<dyn std::error::Error>> {
    const CHUNK_SIZE: usize = 1_000_000; // 1MB chunks

    let base64_len = png_base64.len();

    // Check if we need to chunk
    if base64_len <= CHUNK_SIZE {
        // Single row - no chunking needed
        let df = df! {
            ".ci" => [0i32],
            ".ri" => [0i32],
            ".content" => [png_base64],
            &format!("{}.filename", namespace) => [filename],
            &format!("{}.mimetype", namespace) => [mimetype],
            &format!("{}.plot_width", namespace) => [plot_width as f64],
            &format!("{}.plot_height", namespace) => [plot_height as f64]
        }?;
        Ok(df)
    } else {
        // Multiple rows - chunk the base64 string
        let chunks: Vec<String> = png_base64
            .as_bytes()
            .chunks(CHUNK_SIZE)
            .map(|chunk| String::from_utf8(chunk.to_vec()).unwrap())
            .collect();

        let n_chunks = chunks.len();
        println!(
            "  Chunking large image: {} bytes into {} chunks",
            base64_len, n_chunks
        );

        // Create vectors for each column (all chunks have same .ci/.ri)
        let ci_vec = vec![0i32; n_chunks];
        let ri_vec = vec![0i32; n_chunks];
        let filename_vec = vec![filename; n_chunks];
        let mimetype_vec = vec![mimetype; n_chunks];
        let width_vec = vec![plot_width as f64; n_chunks];
        let height_vec = vec![plot_height as f64; n_chunks];

        let df = df! {
            ".ci" => ci_vec,
            ".ri" => ri_vec,
            ".content" => chunks,
            &format!("{}.filename", namespace) => filename_vec,
            &format!("{}.mimetype", namespace) => mimetype_vec,
            &format!("{}.plot_width", namespace) => width_vec,
            &format!("{}.plot_height", namespace) => height_vec
        }?;
        Ok(df)
    }
}

/// Convert DataFrame to Tercen Table with TSON encoding
///
/// This is delegated to the table_convert module
fn dataframe_to_table(df: &DataFrame) -> Result<proto::Table, Box<dyn std::error::Error>> {
    table_convert::dataframe_to_table(df)
}

/// Create an OperatorResult wrapping the table
///
/// OperatorResult structure (full Tercen model format):
/// ```json
/// {
///   "kind": "OperatorResult",
///   "tables": [
///     {
///       "kind": "Table",
///       "nRows": ...,
///       "properties": {"kind": "TableProperties", "name": "...", ...},
///       "columns": [...]
///     }
///   ],
///   "joinOperators": []
/// }
/// ```
fn create_operator_result(
    table: proto::Table,
) -> Result<rustson::Value, Box<dyn std::error::Error>> {
    use rustson::Value as TsonValue;
    use std::collections::HashMap;

    // Convert Table to full Tercen model TSON format (NOT simplified Sarno format)
    let table_tson = table_to_tercen_tson(&table)?;

    // Create OperatorResult structure
    let mut operator_result = HashMap::new();
    operator_result.insert(
        "kind".to_string(),
        TsonValue::STR("OperatorResult".to_string()),
    );
    operator_result.insert("tables".to_string(), TsonValue::LST(vec![table_tson]));
    operator_result.insert("joinOperators".to_string(), TsonValue::LST(vec![]));

    Ok(TsonValue::MAP(operator_result))
}

/// Serialize OperatorResult to TSON bytes
fn serialize_operator_result(
    operator_result: &rustson::Value,
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let bytes = rustson::encode(operator_result)
        .map_err(|e| format!("Failed to encode OperatorResult to TSON: {:?}", e))?;
    Ok(bytes)
}

/// Convert Table proto to full Tercen model TSON format
///
/// Creates a complete Table object with kind, properties, and columns:
/// ```json
/// {
///   "kind": "Table",
///   "nRows": ...,
///   "properties": {
///     "kind": "TableProperties",
///     "name": "uuid",
///     "sortOrder": [],
///     "ascending": false
///   },
///   "columns": [
///     {
///       "kind": "Column",
///       "name": "...",
///       "type": "...",
///       "nRows": ...,
///       "size": ...,
///       "values": <tson-encoded-data>
///     }
///   ]
/// }
/// ```
fn table_to_tercen_tson(
    table: &proto::Table,
) -> Result<rustson::Value, Box<dyn std::error::Error>> {
    use rustson::Value as TsonValue;
    use std::collections::HashMap;

    let mut table_map = HashMap::new();

    // Add kind
    table_map.insert("kind".to_string(), TsonValue::STR("Table".to_string()));

    // Add nRows
    table_map.insert("nRows".to_string(), TsonValue::I32(table.n_rows));

    // Add properties
    if let Some(props) = &table.properties {
        let mut props_map = HashMap::new();
        props_map.insert(
            "kind".to_string(),
            TsonValue::STR("TableProperties".to_string()),
        );
        props_map.insert("name".to_string(), TsonValue::STR(props.name.clone()));
        props_map.insert(
            "sortOrder".to_string(),
            TsonValue::LST(
                props
                    .sort_order
                    .iter()
                    .map(|s| TsonValue::STR(s.clone()))
                    .collect(),
            ),
        );
        props_map.insert("ascending".to_string(), TsonValue::BOOL(props.ascending));

        table_map.insert("properties".to_string(), TsonValue::MAP(props_map));
    }

    // Add columns
    let mut cols_list = Vec::new();
    for col in &table.columns {
        let mut col_map = HashMap::new();

        col_map.insert("kind".to_string(), TsonValue::STR("Column".to_string()));
        col_map.insert("name".to_string(), TsonValue::STR(col.name.clone()));
        col_map.insert("type".to_string(), TsonValue::STR(col.r#type.clone()));
        col_map.insert("nRows".to_string(), TsonValue::I32(col.n_rows));
        col_map.insert("size".to_string(), TsonValue::I32(col.size));

        // Decode the TSON-encoded values to get the actual data
        let col_values = rustson::decode_bytes(&col.values)
            .map_err(|e| format!("Failed to decode column values for '{}': {:?}", col.name, e))?;
        col_map.insert("values".to_string(), col_values);

        cols_list.push(TsonValue::MAP(col_map));
    }
    table_map.insert("columns".to_string(), TsonValue::LST(cols_list));

    Ok(TsonValue::MAP(table_map))
}

/// Create FileDocument for result upload
fn create_file_document(project_id: &str, size: i32) -> proto::FileDocument {
    // Set file metadata
    let file_metadata = proto::FileMetadata {
        content_type: "application/octet-stream".to_string(),
        ..Default::default()
    };

    let e_metadata = proto::EFileMetadata {
        object: Some(proto::e_file_metadata::Object::Filemetadata(file_metadata)),
    };

    // Note: ACL will be assigned by the server based on projectId
    proto::FileDocument {
        name: "result".to_string(),
        project_id: project_id.to_string(),
        size,
        metadata: Some(e_metadata),
        ..Default::default()
    }
}

/// Upload result file via FileService.upload()
///
/// This uploads an OperatorResult (TSON-encoded table) as a FileDocument.
/// The returned FileDocument ID is what goes into task.fileResultId.
/// The server (Sarno) will then process this file to create the actual schemas
/// and computedRelation.
///
/// Note: We use FileService.upload() (NOT TableSchemaService.uploadTable())
/// because we need a FileDocument with a dataUri, not just a Schema.
async fn upload_result_file(
    client: &TercenClient,
    file_doc: proto::FileDocument,
    result_bytes: Vec<u8>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut file_service = client.file_service()?;

    // Create EFileDocument wrapper
    let e_file_doc = proto::EFileDocument {
        object: Some(proto::e_file_document::Object::Filedocument(file_doc)),
    };

    // Create upload request (single message in a stream)
    let request = proto::ReqUpload {
        file: Some(e_file_doc),
        bytes: result_bytes,
    };

    // Wrap in stream (even though it's just one message)
    use futures::stream;
    let request_stream = stream::iter(vec![request]);

    // Send request
    let response = file_service.upload(request_stream).await?;
    let resp_upload = response.into_inner();

    // Extract FileDocument ID from response
    let e_file_doc = resp_upload
        .result
        .ok_or("Upload response missing file document")?;

    // Extract the actual FileDocument from the wrapper
    use proto::e_file_document;
    let file_doc_obj = e_file_doc.object.ok_or("EFileDocument has no object")?;

    // EFileDocument only has one variant: filedocument
    let file_doc_id = match file_doc_obj {
        e_file_document::Object::Filedocument(fd) => fd.id,
    };

    Ok(file_doc_id)
}

/// Get the task's fileResultId if it exists
///
/// Returns empty string if fileResultId is not set.
fn get_task_file_result_id(task: &proto::ETask) -> Result<String, Box<dyn std::error::Error>> {
    use proto::e_task;

    let task_obj = task.object.as_ref().ok_or("Task has no object field")?;

    match task_obj {
        e_task::Object::Runcomputationtask(rct) => Ok(rct.file_result_id.clone()),
        _ => Err("Expected RunComputationTask".into()),
    }
}

/// Update the task's fileResultId field
///
/// This updates the EXISTING task (following Python OperatorContext pattern).
/// The server will process the file and create the computedRelation automatically.
fn update_task_file_result_id(
    task: &mut proto::ETask,
    file_doc_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    use proto::e_task;

    let task_obj = task.object.as_mut().ok_or("Task has no object field")?;

    match task_obj {
        e_task::Object::Runcomputationtask(rct) => {
            rct.file_result_id = file_doc_id.to_string();
            Ok(())
        }
        _ => Err("Expected RunComputationTask".into()),
    }
}
