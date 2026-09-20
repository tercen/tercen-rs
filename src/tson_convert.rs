//! TSON to Polars DataFrame conversion
//!
//! Converts TSON binary format (Tercen's native format) to Polars DataFrame.
//! This module uses Polars for efficient columnar data processing.
//!
//! TSON (Tercen Serialized Object Notation) is a binary format used by Tercen
//! for efficient data transfer. See: https://github.com/tercen/rustson

use crate::error::{Result, TercenError};
use polars::prelude::*;
use rustson::Value as TsonValue;

/// Convert TSON bytes to Polars DataFrame
///
/// This function decodes TSON binary format and converts it to a Polars DataFrame.
/// The TSON data is expected to represent a table structure.
pub fn tson_to_dataframe(tson_bytes: &[u8]) -> Result<polars::frame::DataFrame> {
    if tson_bytes.is_empty() {
        return Ok(polars::frame::DataFrame::empty());
    }

    // A response is not one document. `TableStreamer::stream_tson` concatenates every gRPC
    // message, and the server pages its answer — about 15,000 rows a page — so the buffer holds
    // one complete TSON document per page, back to back. Decoding only the first silently
    // discards the rest, which makes a large request pathological: asking for a million rows
    // transfers a million rows, uses fifteen thousand, and the caller asks again from a slightly
    // later offset. Measured on a 19.5 M-cell crosstab, that cost 800 bytes on the wire per cell
    // of three columns that need sixteen, and 647 s where reading every document takes 8 s.
    let mut frames: Vec<polars::frame::DataFrame> = Vec::new();
    let mut cursor = std::io::Cursor::new(tson_bytes);
    while (cursor.position() as usize) < tson_bytes.len() {
        let start = cursor.position();
        let tson_value = rustson::decode(cursor.clone())
            .map_err(|e| TercenError::Other(format!("Failed to decode TSON: {:?}", e)))?;
        // `rustson::decode` takes the cursor by value and does not report how far it read, so
        // the document's length comes from re-encoding it. A `decode_from(&mut Cursor)` in
        // rustson would remove this.
        let consumed = rustson::encode(&tson_value)
            .map_err(|e| TercenError::Other(format!("Failed to size TSON document: {:?}", e)))?
            .len();
        if consumed == 0 {
            break;
        }
        cursor.set_position(start + consumed as u64);
        let df = tson_value_to_dataframe(&tson_value)?;
        if !df.is_empty() {
            frames.push(df);
        }
    }

    let mut frames = frames.into_iter();
    let Some(mut out) = frames.next() else {
        return Ok(polars::frame::DataFrame::empty());
    };
    for df in frames {
        out = out
            .vstack(&df)
            .map_err(|e| TercenError::Other(format!("Failed to concatenate TSON pages: {e}")))?;
    }
    Ok(out)
}

/// Convert a TSON value to Polars DataFrame
///
/// Expects the TSON value to be a Tercen table structure:
/// ```json
/// {
///   "cols": [
///     {name: "col1", type: "d", data: [...]},
///     {name: "col2", type: "s", data: [...]},
///   ]
/// }
/// ```
fn tson_value_to_dataframe(tson: &TsonValue) -> Result<polars::frame::DataFrame> {
    // TSON tables are represented as MAP
    let map = match tson {
        TsonValue::MAP(m) => m,
        _ => {
            return Err(TercenError::Other(
                "Expected TSON MAP, got different type".to_string(),
            ))
        }
    };

    // Extract column definitions from 'cols'
    let cols = map
        .get("cols")
        .ok_or_else(|| TercenError::Other("Missing 'cols' field in TSON".to_string()))?;

    let col_defs = match cols {
        TsonValue::LST(defs) => defs,
        _ => return Err(TercenError::Other("Expected 'cols' to be LST".to_string())),
    };

    if col_defs.is_empty() {
        // Empty table
        return Ok(polars::frame::DataFrame::empty());
    }

    // Extract column names and data from each column definition
    let mut col_names = Vec::new();
    let mut col_data_arrays = Vec::new();

    for col_def in col_defs {
        if let TsonValue::MAP(col_map) = col_def {
            // Extract name
            if let Some(TsonValue::STR(name)) = col_map.get("name") {
                col_names.push(name.clone());

                // Extract data array
                if let Some(data) = col_map.get("data") {
                    col_data_arrays.push(data);
                } else {
                    return Err(TercenError::Other(format!(
                        "Column '{}' missing 'data' field",
                        name
                    )));
                }
            }
        }
    }

    // Determine number of rows from first column (unused but kept for future validation)
    let _nrows = if let Some(first_col_data) = col_data_arrays.first() {
        get_column_length(first_col_data)?
    } else {
        0
    };

    // Convert TSON columnar arrays directly to Polars Columns (STAY COLUMNAR!)
    let mut columns_vec = Vec::new();

    for (col_name, col_data) in col_names.iter().zip(col_data_arrays.iter()) {
        let series = tson_column_to_polars_series(col_name, col_data)?;
        // Convert Series to Column for Polars 0.44 API
        columns_vec.push(series.into_column());
    }

    // Create Polars DataFrame directly from columns
    let polars_df = polars::frame::DataFrame::new(columns_vec)
        .map_err(|e| TercenError::Other(format!("Failed to create Polars DataFrame: {}", e)))?;

    Ok(polars_df)
}

/// Convert TSON column array directly to Polars Series (COLUMNAR - NO RECORDS!)
fn tson_column_to_polars_series(col_name: &str, col_data: &TsonValue) -> Result<Series> {
    match col_data {
        TsonValue::LSTF64(values) => {
            // f64 array - direct conversion
            Ok(Series::new(col_name.into(), values.as_slice()))
        }
        TsonValue::LSTI32(values) => {
            Ok(Series::new(col_name.into(), values.as_slice()))
        }
        TsonValue::LSTI64(values) => {
            Ok(Series::new(col_name.into(), values.as_slice()))
        }
        TsonValue::LSTI16(values) => {
            let i32_values: Vec<i32> = values.iter().map(|&v| v as i32).collect();
            Ok(Series::new(col_name.into(), i32_values))
        }
        TsonValue::LSTU16(values) => {
            let i32_values: Vec<i32> = values.iter().map(|&v| v as i32).collect();
            Ok(Series::new(col_name.into(), i32_values))
        }
        TsonValue::LSTU32(values) => {
            let i64_values: Vec<i64> = values.iter().map(|&v| v as i64).collect();
            Ok(Series::new(col_name.into(), i64_values))
        }
        TsonValue::LSTU64(values) => {
            let i64_values: Vec<i64> = values.iter().map(|&v| v as i64).collect();
            Ok(Series::new(col_name.into(), i64_values))
        }
        TsonValue::LSTU8(values) => {
            let i32_values: Vec<i32> = values.iter().map(|&v| v as i32).collect();
            Ok(Series::new(col_name.into(), i32_values))
        }
        TsonValue::LSTI8(values) => {
            let i32_values: Vec<i32> = values.iter().map(|&v| v as i32).collect();
            Ok(Series::new(col_name.into(), i32_values))
        }
        TsonValue::LSTF32(values) => {
            Ok(Series::new(col_name.into(), values.as_slice()))
        }
        TsonValue::LSTSTR(strvec) => {
            // String array
            let strings = strvec
                .try_to_vec()
                .map_err(|e| TercenError::Other(format!("Failed to parse LSTSTR: {:?}", e)))?;
            Ok(Series::new(col_name.into(), strings))
        }
        TsonValue::LST(values) => {
            // Mixed-type list - convert each element
            let mut any_values = Vec::with_capacity(values.len());
            for val in values {
                let any_val = tson_value_to_any_value(val)?;
                any_values.push(any_val);
            }
            Ok(Series::new(col_name.into(), any_values))
        }
        _ => {
            // Single value or unsupported type
            Err(TercenError::Other(format!(
                "Unsupported TSON column type for column: {}",
                col_name
            )))
        }
    }
}

/// Convert a single TSON value to Polars AnyValue
fn tson_value_to_any_value(tson: &TsonValue) -> Result<AnyValue<'static>> {
    match tson {
        TsonValue::NULL => Ok(AnyValue::Null),
        TsonValue::BOOL(b) => Ok(AnyValue::Boolean(*b)),
        TsonValue::I32(i) => Ok(AnyValue::Int64(*i as i64)),
        TsonValue::F64(f) => Ok(AnyValue::Float64(*f)),
        TsonValue::STR(s) => {
            // Convert to owned string to satisfy 'static lifetime
            Ok(AnyValue::StringOwned(s.clone().into()))
        }
        _ => {
            // For complex types, convert to owned string
            let owned_str = format!("{:?}", tson);
            Ok(AnyValue::StringOwned(owned_str.into()))
        }
    }
}

/// Get the length of a TSON column array
fn get_column_length(col_data: &TsonValue) -> Result<usize> {
    match col_data {
        TsonValue::LST(v) => Ok(v.len()),
        TsonValue::LSTU8(v) => Ok(v.len()),
        TsonValue::LSTI8(v) => Ok(v.len()),
        TsonValue::LSTU16(v) => Ok(v.len()),
        TsonValue::LSTI16(v) => Ok(v.len()),
        TsonValue::LSTU32(v) => Ok(v.len()),
        TsonValue::LSTI32(v) => Ok(v.len()),
        TsonValue::LSTU64(v) => Ok(v.len()),
        TsonValue::LSTI64(v) => Ok(v.len()),
        TsonValue::LSTF32(v) => Ok(v.len()),
        TsonValue::LSTF64(v) => Ok(v.len()),
        TsonValue::LSTSTR(strvec) => strvec
            .try_to_vec()
            .map(|v| v.len())
            .map_err(|e| TercenError::Other(format!("Failed to get LSTSTR length: {:?}", e))),
        _ => Ok(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_tson() {
        let result = tson_to_dataframe(&[]);
        assert!(result.is_ok());
        let df = result.unwrap();
        assert_eq!(df.height(), 0);
    }
}
