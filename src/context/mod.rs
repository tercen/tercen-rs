//! TercenContext trait and implementations
//!
//! This module provides a unified interface for accessing Tercen task/query data
//! regardless of whether we're in production mode (with task_id) or dev mode
//! (with workflow_id + step_id).
//!
//! This mirrors Python's OperatorContext / OperatorContextDev pattern.

use crate::client::proto::{CubeQuery, OperatorSettings};
use crate::colors::{ChartKind, ColorInfo};
use crate::TercenClient;
use std::sync::Arc;

mod base;
mod dev_context;
mod helpers;
mod production_context;

pub use base::{ContextBase, ContextBaseBuilder};

pub use dev_context::DevContext;
pub use production_context::ProductionContext;

/// Trait for accessing Tercen context data
///
/// Implementations:
/// - `ProductionContext`: Initialized from task_id (production mode)
/// - `DevContext`: Initialized from workflow_id + step_id (dev/test mode)
pub trait TercenContext: Send + Sync {
    /// Get the CubeQuery containing table hashes
    fn cube_query(&self) -> &CubeQuery;

    /// Get the schema IDs (table IDs for Y-axis, colors, etc.)
    fn schema_ids(&self) -> &[String];

    /// Get the workflow ID
    fn workflow_id(&self) -> &str;

    /// Get the step ID
    fn step_id(&self) -> &str;

    /// Get the project ID
    fn project_id(&self) -> &str;

    /// Get the namespace
    fn namespace(&self) -> &str;

    /// Get the operator settings (if available)
    fn operator_settings(&self) -> Option<&OperatorSettings>;

    /// Get the color information extracted from the workflow
    fn color_infos(&self) -> &[ColorInfo];

    /// Get the page factor names
    fn page_factors(&self) -> &[String];

    /// Get the Y-axis table ID (if available)
    fn y_axis_table_id(&self) -> Option<&str>;

    /// Get the X-axis table ID (if available)
    fn x_axis_table_id(&self) -> Option<&str>;

    /// Get the point size from crosstab model (UI scale 1-10, None = use default)
    fn point_size(&self) -> Option<i32>;

    /// Get the chart kind (Point, Heatmap, Line, Bar)
    fn chart_kind(&self) -> ChartKind;

    /// Get the crosstab dimensions from the model (cellSize × nRows for each axis)
    /// Returns (width, height) in pixels, or None if not available
    fn crosstab_dimensions(&self) -> Option<(i32, i32)>;

    /// Get the Y-axis transform type (e.g., "log", "asinh", "sqrt")
    /// Returns None if no transform is applied (identity)
    fn y_transform(&self) -> Option<&str>;

    /// Get the X-axis transform type (e.g., "log", "asinh", "sqrt")
    /// Returns None if no transform is applied (identity) or if no X-axis is defined
    fn x_transform(&self) -> Option<&str>;

    /// Get the Tercen client
    fn client(&self) -> &Arc<TercenClient>;

    /// Get per-layer color configuration (for mixed-layer scenarios)
    ///
    /// Returns None for legacy configurations that use uniform colors across all layers.
    /// When Some, this takes precedence over color_infos() for color processing.
    fn per_layer_colors(&self) -> Option<&crate::PerLayerColorConfig>;

    /// Get Y-axis factor names per layer
    ///
    /// Used for legend entries when layers don't have explicit color factors.
    /// Each name comes from axis_queries[i].yAxis.graphical_factor.factor.name
    fn layer_y_factor_names(&self) -> &[String];

    // Convenience methods with default implementations

    /// Get the main table hash (qt_hash)
    fn qt_hash(&self) -> &str {
        &self.cube_query().qt_hash
    }

    /// Get the column facet table hash
    fn column_hash(&self) -> &str {
        &self.cube_query().column_hash
    }

    /// Get the row facet table hash
    fn row_hash(&self) -> &str {
        &self.cube_query().row_hash
    }

    // === Factor name accessors (from CubeQuery.axisQueries[0]) ===

    /// Get the color factor names from the first axis query
    fn colors(&self) -> Vec<&str> {
        self.cube_query()
            .axis_queries
            .first()
            .map(|aq| aq.colors.iter().map(|f| f.name.as_str()).collect())
            .unwrap_or_default()
    }

    /// Get the label factor names from the first axis query
    fn labels(&self) -> Vec<&str> {
        self.cube_query()
            .axis_queries
            .first()
            .map(|aq| aq.labels.iter().map(|f| f.name.as_str()).collect())
            .unwrap_or_default()
    }

    /// Get the error factor names from the first axis query
    fn errors(&self) -> Vec<&str> {
        self.cube_query()
            .axis_queries
            .first()
            .map(|aq| aq.errors.iter().map(|f| f.name.as_str()).collect())
            .unwrap_or_default()
    }

    /// Get the number of layers (axis queries)
    ///
    /// Each axis query represents a layer in the plot. When there are multiple layers
    /// and no colors are specified, we can use layer-based coloring.
    fn n_layers(&self) -> usize {
        self.cube_query().axis_queries.len().max(1)
    }

    /// Get the palette name for layer-based coloring
    ///
    /// Returns the palette name from the crosstab configuration, used when
    /// coloring points by layer when no explicit color factors are defined.
    fn layer_palette_name(&self) -> Option<&str>;
}
