//! Page factor extraction from operator spec
//!
//! Pages allow splitting a plot into multiple separate plots based on factor values.
//! Unlike facets (which create a grid of panels in one plot), pages create separate
//! output files - one per unique page value.
//!
//! ## How it works:
//! 1. Page factors are defined in the operator's input spec (MetaFactor with name="Page")
//! 2. Page factor columns are stored in the row facet table
//! 3. We extract unique values from the page columns
//! 4. Generate one plot per unique page value combination

use crate::client::proto::{e_meta_factor, e_operator_input_spec, OperatorSettings};
use crate::{tson_to_dataframe, TableStreamer, TercenClient};
use std::collections::HashMap;

/// Extract page factor names from operator settings
///
/// Returns a vector of column names that should be used for pagination.
/// Empty vector means no pages (generate single plot).
///
/// # Algorithm
/// 1. Get operatorRef.operatorSpec from operator settings
/// 2. Get inputSpecs[0].metaFactors[] (array of MetaFactor)
/// 3. Find MetaFactor where name matches "Page" or "page"
/// 4. Extract factors[].name from that MetaFactor
pub fn extract_page_factors(operator_settings: Option<&OperatorSettings>) -> Vec<String> {
    let operator_settings = match operator_settings {
        Some(os) => os,
        None => return Vec::new(),
    };

    // Get operator_ref
    let operator_ref = match operator_settings.operator_ref.as_ref() {
        Some(or_ref) => or_ref,
        None => return Vec::new(),
    };

    // Get operator_spec
    let operator_spec = match operator_ref.operator_spec.as_ref() {
        Some(spec) => spec,
        None => return Vec::new(),
    };

    // Get first input spec (crosstab spec)
    if operator_spec.input_specs.is_empty() {
        return Vec::new();
    }

    let first_input_spec = &operator_spec.input_specs[0];

    // Extract CrosstabSpec from EOperatorInputSpec
    let crosstab_spec = match first_input_spec.object.as_ref() {
        Some(e_operator_input_spec::Object::Crosstabspec(cs)) => cs,
        _ => return Vec::new(),
    };

    // Find MetaFactor with name="Page" or "page"
    for e_meta_factor in &crosstab_spec.meta_factors {
        let meta_factor = match e_meta_factor.object.as_ref() {
            Some(e_meta_factor::Object::Metafactor(mf)) => mf,
            _ => continue,
        };

        // Check if this is the "Page" metafactor
        if meta_factor.name.to_lowercase() == "page" {
            // Extract factor names
            return meta_factor.factors.iter().map(|f| f.name.clone()).collect();
        }
    }

    Vec::new()
}

/// Page value - represents one unique combination of page factor values
///
/// For single page factor (e.g., Country):
///   PageValue { values: {"Country": "USA"}, label: "USA" }
///
/// For multiple page factors (e.g., Country + Year):
///   PageValue { values: {"Country": "USA", "Year": "2020"}, label: "USA_2020" }
#[derive(Debug, Clone)]
pub struct PageValue {
    /// Map of page factor names to their values
    pub values: HashMap<String, String>,
    /// Human-readable label for this page (used in filename)
    pub label: String,
}

/// Extract unique page values from row facet table
///
/// Returns a vector of PageValue objects representing each unique page.
/// If page_factors is empty, returns a single PageValue with empty values (no pagination).
///
/// # Arguments
/// * `client` - Tercen client
/// * `row_table_id` - Row facet table ID
/// * `page_factors` - Page factor column names (from extract_page_factors)
///
/// # Algorithm
/// 1. Stream entire row facet table (it's small - just unique facet combinations)
/// 2. Parse to Polars DataFrame
/// 3. Extract unique combinations of page factor values
/// 4. Build PageValue for each unique combination
pub async fn extract_page_values(
    client: &TercenClient,
    row_table_id: &str,
    page_factors: &[String],
) -> Result<Vec<PageValue>, Box<dyn std::error::Error>> {
    // If no page factors, return single "page" (no pagination)
    if page_factors.is_empty() {
        return Ok(vec![PageValue {
            values: HashMap::new(),
            label: "all".to_string(),
        }]);
    }

    // Stream row facet table
    let streamer = TableStreamer::new(client);

    // Get schema to know row count
    let schema = streamer.get_schema(row_table_id).await?;

    use crate::client::proto::e_schema;
    let n_rows = match &schema.object {
        Some(e_schema::Object::Cubequerytableschema(cqts)) => cqts.n_rows as usize,
        Some(e_schema::Object::Tableschema(ts)) => ts.n_rows as usize,
        Some(e_schema::Object::Computedtableschema(cts)) => cts.n_rows as usize,
        _ => return Err("Unknown schema type for row table".into()),
    };

    println!("Extracting page values from row table ({} rows)...", n_rows);

    // Stream entire table with only page factor columns
    let tson_data = streamer
        .stream_tson(row_table_id, Some(page_factors.to_vec()), 0, n_rows as i64)
        .await?;
    eprintln!("DEBUG: TSON data size: {} bytes", tson_data.len());

    // Parse to DataFrame
    let df = tson_to_dataframe(&tson_data)?;

    println!("  Found {} total rows", df.height());

    // Extract unique combinations using simple HashSet (avoids Polars lazy init overhead)
    // For small page tables (typically < 100 rows), this is much more efficient
    use std::collections::HashSet;

    let mut seen: HashSet<Vec<String>> = HashSet::new();
    let mut page_values = Vec::new();

    for row_idx in 0..df.height() {
        // Build key from all page factor values for this row
        let mut key = Vec::with_capacity(page_factors.len());
        let mut values = HashMap::new();
        let mut label_parts = Vec::new();

        for factor_name in page_factors {
            let value = df
                .column(factor_name)?
                .get(row_idx)?
                .to_string()
                .trim_matches('"')
                .to_string();
            key.push(value.clone());
            values.insert(factor_name.clone(), value.clone());
            label_parts.push(value);
        }

        // Only add if we haven't seen this combination before
        if seen.insert(key) {
            let label = label_parts.join("_");
            page_values.push(PageValue { values, label });
        }
    }

    println!("  Found {} unique page combinations", page_values.len());

    Ok(page_values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_page_factors_none() {
        // No operator settings
        let result = extract_page_factors(None);
        assert!(result.is_empty());
    }
}
