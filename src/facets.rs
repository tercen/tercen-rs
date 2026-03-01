//! Facet metadata loading and management
//!
//! This module handles loading and parsing facet tables (column.csv and row.csv)
//! which define the structure of faceted plots.

use crate::error::Result;
use crate::table::TableStreamer;
use crate::tson_convert::tson_to_dataframe;
use crate::TercenClient;
use crate::extract_column_names_from_schema;
use std::collections::HashMap;

/// Represents a single facet group
#[derive(Debug, Clone)]
pub struct FacetGroup {
    /// Index of this facet group (0-based, for GGRS)
    pub index: usize,
    /// Original index from the table (before filtering/remapping)
    pub original_index: usize,
    /// Label for display (combination of all column values)
    pub label: String,
    /// Raw column values for this facet
    pub values: HashMap<String, String>,
}

/// Collection of facet groups for one dimension (column or row)
#[derive(Debug, Clone)]
pub struct FacetMetadata {
    /// All facet groups in order
    pub groups: Vec<FacetGroup>,
    /// Column names in the facet table
    pub column_names: Vec<String>,
}

impl FacetMetadata {
    /// Load facet metadata from a Tercen table
    pub async fn load(client: &TercenClient, table_id: &str) -> Result<Self> {
        let streamer = TableStreamer::new(client);

        // Get row count from schema
        let schema = streamer.get_schema(table_id).await?;

        use crate::client::proto::e_schema;
        let n_rows = match &schema.object {
            Some(e_schema::Object::Cubequerytableschema(cqts)) => {
                eprintln!("DEBUG: CubeQueryTableSchema nRows={}", cqts.n_rows);
                cqts.n_rows as usize
            }
            Some(e_schema::Object::Tableschema(ts)) => {
                eprintln!("DEBUG: TableSchema nRows={}", ts.n_rows);
                ts.n_rows as usize
            }
            Some(e_schema::Object::Computedtableschema(cts)) => {
                eprintln!("DEBUG: ComputedTableSchema nRows={}", cts.n_rows);
                cts.n_rows as usize
            }
            other => {
                eprintln!("DEBUG: Unknown schema type: {:?}", other);
                0
            }
        };

        if n_rows == 0 {
            return Ok(FacetMetadata {
                groups: vec![],
                column_names: vec![],
            });
        }

        // Get column names from schema first
        let column_names = match extract_column_names_from_schema(&schema) {
            Ok(cols) => cols,
            Err(e) => {
                eprintln!("DEBUG: Failed to extract column names: {}", e);
                vec![]
            }
        };
        eprintln!("DEBUG: Facet table has columns: {:?}", column_names);

        // Stream TSON data to get actual facet values
        // Request specific columns (not None) to ensure data is materialized
        let columns_to_fetch = if column_names.is_empty() {
            None
        } else {
            Some(column_names.clone())
        };

        let tson_data = streamer
            .stream_tson(table_id, columns_to_fetch, 0, n_rows as i64)
            .await?;

        // If no data, return placeholder labels
        if tson_data.is_empty() || tson_data.len() < 30 {
            eprintln!(
                "DEBUG: Facet table has no data ({} bytes), using index labels",
                tson_data.len()
            );
            let groups: Vec<FacetGroup> = (0..n_rows)
                .map(|index| FacetGroup {
                    index,
                    original_index: index,
                    label: format!("{}", index),
                    values: Default::default(),
                })
                .collect();

            return Ok(FacetMetadata {
                groups,
                column_names: vec![],
            });
        }

        // Parse TSON to DataFrame
        let df = tson_to_dataframe(&tson_data)?;
        eprintln!(
            "DEBUG: Parsed facet table: {} rows × {} columns",
            df.height(),
            df.width()
        );

        let column_names: Vec<String> = df.get_column_names().iter().map(|s| s.to_string()).collect();
        eprintln!("DEBUG: Facet columns: {:?}", column_names);

        // Create groups from parsed data
        let mut groups = Vec::new();
        for index in 0..df.height() {
            let mut values = HashMap::new();
            let mut label_parts = Vec::new();

            // Collect all column values for this row
            for col_name in &column_names {
                if let Ok(col) = df.column(col_name) {
                    if let Ok(value) = col.get(index) {
                        let value_str = format!("{}", value).trim_matches('"').to_string();
                        values.insert(col_name.clone(), value_str.clone());
                        label_parts.push(value_str);
                    }
                }
            }

            // Join all values with ", " to create label
            let label = if label_parts.is_empty() {
                format!("{}", index)
            } else {
                label_parts.join(", ")
            };

            groups.push(FacetGroup {
                index,
                original_index: index,
                label,
                values,
            });
        }

        eprintln!("DEBUG: Created {} facet groups", groups.len());
        for (i, group) in groups.iter().enumerate() {
            eprintln!("DEBUG: Facet[{}] label='{}'", i, group.label);
        }

        Ok(FacetMetadata {
            groups,
            column_names,
        })
    }

    /// Load facet metadata with filtering by page values
    ///
    /// # Arguments
    /// * `filter` - Map of column names to values (e.g., {"Gender": "male"})
    ///
    /// Only facet groups matching ALL filter criteria will be loaded.
    /// Note: Indices are NOT remapped - they keep their original values from the table.
    pub async fn load_with_filter(
        client: &TercenClient,
        table_id: &str,
        filter: &HashMap<String, String>,
    ) -> Result<Self> {
        // Load all facets first
        let mut metadata = Self::load(client, table_id).await?;

        let original_count = metadata.groups.len();

        // Filter groups to only those matching all criteria
        metadata.groups.retain(|group| {
            filter.iter().all(|(col_name, expected_value)| {
                group
                    .values
                    .get(col_name)
                    .map(|actual_value| actual_value == expected_value)
                    .unwrap_or(false)
            })
        });

        eprintln!(
            "DEBUG: Filtered facets from {} to {} groups",
            original_count,
            metadata.groups.len()
        );

        // CRITICAL: Remap facet indices to 0-based for GGRS grid positioning
        // GGRS expects facet groups with indices 0..N for rendering grid
        // The original indices are preserved in group.original_index for data matching
        //
        // Data flow:
        // 1. Operator loads only male facets (original_index=12-23, index=0-11)
        // 2. Operator streams raw data with .ri=12-23 (no filtering/remapping)
        // 3. GGRS uses original_index to route data[.ri=12] → panel[index=0]
        for (new_idx, group) in metadata.groups.iter_mut().enumerate() {
            eprintln!(
                "  Remapping facet {} from original_index {} to index {}",
                group.label, group.original_index, new_idx
            );
            group.index = new_idx;
            // original_index is NOT changed - it keeps the value from the full table
        }

        Ok(metadata)
    }

    /// Get number of facet groups
    pub fn len(&self) -> usize {
        self.groups.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Get a specific facet group by index
    pub fn get(&self, index: usize) -> Option<&FacetGroup> {
        self.groups.get(index)
    }
}

/// Complete faceting information for a plot
#[derive(Debug, Clone)]
pub struct FacetInfo {
    /// Column facet metadata
    pub col_facets: FacetMetadata,
    /// Row facet metadata
    pub row_facets: FacetMetadata,
}

impl FacetInfo {
    /// Load both column and row facet metadata
    pub async fn load(
        client: &TercenClient,
        col_table_id: &str,
        row_table_id: &str,
    ) -> Result<Self> {
        // Load both facet tables in parallel
        let (col_result, row_result) = tokio::join!(
            FacetMetadata::load(client, col_table_id),
            FacetMetadata::load(client, row_table_id)
        );

        let row_facets = row_result?;

        // Follow Tercen's natural ordering (smallest → largest, top to bottom)
        // No reversal - preserve the table order for correct axis range mapping
        // Reversal was previously done to match ggplot2 convention but broke axis range lookups

        Ok(FacetInfo {
            col_facets: col_result?,
            row_facets,
        })
    }

    /// Load facet metadata with filtering on row facets
    ///
    /// # Arguments
    /// * `row_filter` - Filter to apply to row facets (e.g., {"Gender": "male"})
    ///
    /// Column facets are loaded normally, row facets are filtered.
    pub async fn load_with_filter(
        client: &TercenClient,
        col_table_id: &str,
        row_table_id: &str,
        row_filter: &HashMap<String, String>,
    ) -> Result<Self> {
        // Load column facets normally, row facets with filter
        let (col_result, row_result) = tokio::join!(
            FacetMetadata::load(client, col_table_id),
            FacetMetadata::load_with_filter(client, row_table_id, row_filter)
        );

        let row_facets = row_result?;

        // Follow Tercen's natural ordering (smallest → largest, top to bottom)
        // No reversal - preserve the table order for correct axis range mapping

        Ok(FacetInfo {
            col_facets: col_result?,
            row_facets,
        })
    }

    /// Get total number of column facets
    pub fn n_col_facets(&self) -> usize {
        if self.col_facets.is_empty() {
            1 // No faceting = 1 facet
        } else {
            self.col_facets.len()
        }
    }

    /// Get total number of row facets
    pub fn n_row_facets(&self) -> usize {
        if self.row_facets.is_empty() {
            1 // No faceting = 1 facet
        } else {
            self.row_facets.len()
        }
    }

    /// Get total number of facet cells (col × row)
    pub fn total_facets(&self) -> usize {
        self.n_col_facets() * self.n_row_facets()
    }

    /// Check if plot has any faceting
    pub fn has_faceting(&self) -> bool {
        !self.col_facets.is_empty() || !self.row_facets.is_empty()
    }
}

// Tests removed - CSV parsing replaced with TSON format
