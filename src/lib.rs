//! Tercen SDK for Rust
//!
//! Provides gRPC client, data streaming, operator context, color handling,
//! facets, pages, and more for building Tercen operators in Rust.

// Module declarations
pub mod error;
pub mod client;
pub mod logger;
pub mod table;
pub mod tson_convert;
pub mod facets;
pub mod result;
pub mod table_convert;
pub mod properties;
pub mod color_processor;
pub mod colors;
pub mod palettes;
pub mod pages;
pub mod context;

// Re-exports for convenience
pub use client::TercenClient;
pub use color_processor::{add_color_columns, add_layer_colors, add_mixed_layer_colors, pack_rgb, unpack_rgb};
pub use colors::{
    extract_chart_kind_from_step, extract_color_info_from_step, extract_crosstab_palette_name,
    extract_per_layer_color_info, extract_point_size_from_step, interpolate_color, parse_palette,
    CategoryColorMap, ChartKind, ColorInfo, ColorMapping, ColorPalette, ColorStop,
    LayerColorConfig, PerLayerColorConfig,
};
pub use context::{DevContext, ProductionContext, TercenContext};
#[allow(unused_imports)]
pub use error::{Result, TercenError};
#[allow(unused_imports)]
pub use facets::{FacetGroup, FacetInfo, FacetMetadata};
pub use logger::TercenLogger;
pub use pages::{extract_page_factors, extract_page_values, PageValue};
pub use palettes::{
    categorical_color_from_level, get_palette_colors, PaletteRegistry, PALETTE_REGISTRY,
};
pub use properties::{PlotDimension, PropertyReader};
pub use result::PlotResult;
#[allow(unused_imports)]
pub use table::{new_schema_cache, SchemaCache, TableStreamer};
pub use tson_convert::tson_to_dataframe;

/// Extract column names from a CubeQueryTableSchema
///
/// This function only handles the CubeQueryTableSchema variant.
/// Returns an error for other schema types.
pub fn extract_column_names_from_schema(
    schema: &crate::client::proto::ESchema,
) -> std::result::Result<Vec<String>, Box<dyn std::error::Error>> {
    use crate::client::proto::e_schema;

    if let Some(e_schema::Object::Cubequerytableschema(cqts)) = &schema.object {
        let mut column_names = Vec::new();
        for col in &cqts.columns {
            if let Some(crate::client::proto::e_column_schema::Object::Columnschema(cs)) =
                &col.object
            {
                column_names.push(cs.name.clone());
            }
        }
        Ok(column_names)
    } else {
        Err("Schema is not a CubeQueryTableSchema".into())
    }
}
