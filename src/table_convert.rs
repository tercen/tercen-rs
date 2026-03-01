//! DataFrame to Tercen Table conversion
//!
//! Converts Polars DataFrames to Tercen Table proto messages with TSON encoding.
//! This is the core transformation needed to save results back to Tercen.

use crate::client::proto;
use polars::prelude::*;
use rustson::Value as TsonValue;

/// Convert a Polars DataFrame to a Tercen Table
///
/// This function:
/// 1. Infers Tercen types from Polars DataTypes
/// 2. Encodes column values using TSON format
/// 3. Constructs the Table proto message
///
/// # Arguments
/// * `df` - Polars DataFrame to convert
///
/// # Returns
/// Tercen Table proto message ready for upload
pub fn dataframe_to_table(df: &DataFrame) -> Result<proto::Table, Box<dyn std::error::Error>> {
    let nrows = df.height() as i32;

    // Create TableProperties with a unique name (required by Sarno)
    let properties = proto::TableProperties {
        name: uuid::Uuid::new_v4().to_string(),
        sort_order: vec![],
        ascending: false,
    };

    // Convert each column
    let mut columns = Vec::new();

    for col in df.get_columns() {
        let series = col.as_materialized_series();
        let values = encode_column_values(series)?;

        let column = proto::Column {
            name: series.name().to_string(),
            r#type: infer_column_type(series.dtype()),
            n_rows: nrows,
            size: nrows,
            values,
            ..Default::default()
        };

        columns.push(column);
    }

    // Create Table
    let table = proto::Table {
        n_rows: nrows,
        properties: Some(properties),
        columns,
    };

    Ok(table)
}

/// Infer Tercen column type from Polars DataType
///
/// Maps Polars types to Tercen type strings:
/// - String/Utf8 → "string"
/// - Float64 → "double"
/// - Int32 → "int32"
/// - Int64 → "int64"
fn infer_column_type(dtype: &DataType) -> String {
    match dtype {
        DataType::String => "string".to_string(),
        DataType::Float64 => "double".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        _ => "string".to_string(), // Default to string for unknown types
    }
}

/// Encode column values to TSON binary format
///
/// Uses the rustson crate to encode column data in Tercen's binary format.
/// The TSON format for a column is a MAP with structure:
/// ```json
/// {
///   "name": "column_name",
///   "type": "s"|"d"|"i",
///   "data": [values...]
/// }
/// ```
fn encode_column_values(series: &Series) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    // Convert series to TSON Value
    // CRITICAL: Must use typed lists (LSTSTR, LSTF64, LSTI32, LSTI64)
    // NOT generic LST with wrapped values - Sarno expects TypedData!
    let tson_data = match series.dtype() {
        DataType::String => {
            // String column - use LSTSTR for CStringList
            let str_vec: Vec<String> = series
                .str()?
                .into_iter()
                .map(|opt| opt.map(|s| s.to_string()).unwrap_or_else(String::new)) // TODO: Handle nulls properly
                .collect();
            TsonValue::LSTSTR(str_vec.into())
        }
        DataType::Float64 => {
            // Double column - use LSTF64 for Float64List
            let f64_vec: Vec<f64> = series
                .f64()?
                .into_iter()
                .map(|opt| opt.unwrap_or(0.0)) // TODO: Handle nulls properly
                .collect();
            TsonValue::LSTF64(f64_vec)
        }
        DataType::Int32 => {
            // Int32 column - use LSTI32 for Int32List
            let i32_vec: Vec<i32> = series
                .i32()?
                .into_iter()
                .map(|opt| opt.unwrap_or(0)) // TODO: Handle nulls properly
                .collect();
            TsonValue::LSTI32(i32_vec)
        }
        DataType::Int64 => {
            // Int64 column - use LSTI64 for Int64List
            let i64_values: Vec<i64> = series
                .i64()?
                .into_iter()
                .map(|opt| opt.unwrap_or(0)) // TODO: Handle nulls properly
                .collect();
            TsonValue::LSTI64(i64_values)
        }
        _ => {
            return Err(format!("Unsupported column type: {:?}", series.dtype()).into());
        }
    };

    // Encode to TSON binary
    let bytes =
        rustson::encode(&tson_data).map_err(|e| format!("Failed to encode TSON: {:?}", e))?;

    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_result_dataframe_to_table() {
        // Create a simple result DataFrame
        let df = df! {
            ".content" => ["base64encodedstring"],
            "filename" => ["plot.png"],
            "mimetype" => ["image/png"]
        }
        .unwrap();

        // Convert to Table
        let table = dataframe_to_table(&df).unwrap();

        // Verify structure
        assert_eq!(table.n_rows, 1);
        assert_eq!(table.columns.len(), 3);

        // Check column names and types
        assert_eq!(table.columns[0].name, ".content");
        assert_eq!(table.columns[0].r#type, "string");
        assert_eq!(table.columns[1].name, "filename");
        assert_eq!(table.columns[1].r#type, "string");
        assert_eq!(table.columns[2].name, "mimetype");
        assert_eq!(table.columns[2].r#type, "string");

        // Verify TSON encoding produced bytes
        assert!(!table.columns[0].values.is_empty());
        assert!(!table.columns[1].values.is_empty());
        assert!(!table.columns[2].values.is_empty());
    }
}
