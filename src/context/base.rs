//! ContextBase - Common implementation for TercenContext
//!
//! Contains all shared fields and methods used by both ProductionContext and DevContext.
//! The specific contexts wrap this struct and provide different constructors.

use crate::client::proto::{CubeQuery, ETask, OperatorSettings};
use crate::colors::{ChartKind, ColorInfo, PerLayerColorConfig};
use crate::result::PlotResult;
use crate::table::{SchemaCache, TableStreamer};
use crate::TercenClient;

use std::sync::Arc;

/// Common base containing all TercenContext data
///
/// Both ProductionContext and DevContext wrap this struct using the newtype pattern.
/// This eliminates field duplication and provides a single place for common methods.
pub struct ContextBase {
    // Core
    pub(super) client: Arc<TercenClient>,
    pub(super) cube_query: CubeQuery,
    pub(super) schema_ids: Vec<String>,

    // Identifiers
    pub(super) workflow_id: String,
    pub(super) step_id: String,
    pub(super) project_id: String,
    pub(super) namespace: String,

    // Configuration
    pub(super) operator_settings: Option<OperatorSettings>,
    pub(super) color_infos: Vec<ColorInfo>,
    pub(super) page_factors: Vec<String>,

    // Axis tables
    pub(super) y_axis_table_id: Option<String>,
    pub(super) x_axis_table_id: Option<String>,

    // UI settings
    pub(super) point_size: Option<i32>,
    pub(super) chart_kind: ChartKind,
    pub(super) crosstab_dimensions: Option<(i32, i32)>,

    // Transforms
    pub(super) y_transform: Option<String>,
    pub(super) x_transform: Option<String>,

    // Layer coloring
    /// Palette name from crosstab for layer-based coloring (when no color factors)
    pub(super) layer_palette_name: Option<String>,

    /// Per-layer color configuration (for mixed-layer scenarios)
    pub(super) per_layer_colors: Option<PerLayerColorConfig>,

    /// Y-axis factor names per layer (for legend entries)
    pub(super) layer_y_factor_names: Vec<String>,
}

impl ContextBase {
    // === Getters (used by TercenContext trait implementations) ===

    pub fn cube_query(&self) -> &CubeQuery {
        &self.cube_query
    }

    pub fn schema_ids(&self) -> &[String] {
        &self.schema_ids
    }

    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }

    pub fn step_id(&self) -> &str {
        &self.step_id
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn operator_settings(&self) -> Option<&OperatorSettings> {
        self.operator_settings.as_ref()
    }

    pub fn color_infos(&self) -> &[ColorInfo] {
        &self.color_infos
    }

    pub fn page_factors(&self) -> &[String] {
        &self.page_factors
    }

    pub fn y_axis_table_id(&self) -> Option<&str> {
        self.y_axis_table_id.as_deref()
    }

    pub fn x_axis_table_id(&self) -> Option<&str> {
        self.x_axis_table_id.as_deref()
    }

    pub fn point_size(&self) -> Option<i32> {
        self.point_size
    }

    pub fn chart_kind(&self) -> ChartKind {
        self.chart_kind
    }

    pub fn crosstab_dimensions(&self) -> Option<(i32, i32)> {
        self.crosstab_dimensions
    }

    pub fn y_transform(&self) -> Option<&str> {
        self.y_transform.as_deref()
    }

    pub fn x_transform(&self) -> Option<&str> {
        self.x_transform.as_deref()
    }

    pub fn layer_palette_name(&self) -> Option<&str> {
        self.layer_palette_name.as_deref()
    }

    pub fn per_layer_colors(&self) -> Option<&PerLayerColorConfig> {
        self.per_layer_colors.as_ref()
    }

    pub fn layer_y_factor_names(&self) -> &[String] {
        &self.layer_y_factor_names
    }

    pub fn client(&self) -> &Arc<TercenClient> {
        &self.client
    }

    // === Convenience methods (same as trait defaults but available directly) ===

    pub fn qt_hash(&self) -> &str {
        &self.cube_query.qt_hash
    }

    pub fn column_hash(&self) -> &str {
        &self.cube_query.column_hash
    }

    pub fn row_hash(&self) -> &str {
        &self.cube_query.row_hash
    }

    // === Table Streamer Factory ===

    /// Create a TableStreamer for accessing Tercen tables
    pub fn streamer(&self) -> TableStreamer<'_> {
        TableStreamer::new(&self.client)
    }

    /// Create a TableStreamer with schema caching for multi-page plots
    pub fn streamer_with_cache(&self, cache: SchemaCache) -> TableStreamer<'_> {
        TableStreamer::with_cache(&self.client, cache)
    }

    // === Data Access Methods (async) ===

    /// Fetch data from the main table (qt_hash)
    ///
    /// # Arguments
    /// * `columns` - Optional list of column names to fetch (None = all columns)
    /// * `offset` - Number of rows to skip
    /// * `limit` - Maximum number of rows to fetch
    pub async fn select(
        &self,
        columns: Option<Vec<String>>,
        offset: i64,
        limit: i64,
    ) -> Result<polars::frame::DataFrame, Box<dyn std::error::Error>> {
        let streamer = self.streamer();
        let tson_data = streamer
            .stream_tson(&self.cube_query.qt_hash, columns, offset, limit)
            .await?;
        let df = crate::tson_to_dataframe(&tson_data)?;
        Ok(df)
    }

    /// Fetch data from the column facet table (column_hash)
    ///
    /// # Arguments
    /// * `columns` - Optional list of column names to fetch (None = all columns)
    pub async fn cselect(
        &self,
        columns: Option<Vec<String>>,
    ) -> Result<polars::frame::DataFrame, Box<dyn std::error::Error>> {
        if self.cube_query.column_hash.is_empty() {
            return Ok(polars::frame::DataFrame::empty());
        }
        let streamer = self.streamer();
        let tson_data = streamer
            .stream_tson(&self.cube_query.column_hash, columns, 0, -1)
            .await?;
        let df = crate::tson_to_dataframe(&tson_data)?;
        Ok(df)
    }

    /// Fetch data from the row facet table (row_hash)
    ///
    /// # Arguments
    /// * `columns` - Optional list of column names to fetch (None = all columns)
    pub async fn rselect(
        &self,
        columns: Option<Vec<String>>,
    ) -> Result<polars::frame::DataFrame, Box<dyn std::error::Error>> {
        if self.cube_query.row_hash.is_empty() {
            return Ok(polars::frame::DataFrame::empty());
        }
        let streamer = self.streamer();
        let tson_data = streamer
            .stream_tson(&self.cube_query.row_hash, columns, 0, -1)
            .await?;
        let df = crate::tson_to_dataframe(&tson_data)?;
        Ok(df)
    }

    // === Column Name Methods (async) ===

    /// Get column names from the main table schema
    pub async fn names(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        self.get_table_column_names(&self.cube_query.qt_hash).await
    }

    /// Get column names from the column facet table schema
    pub async fn cnames(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        if self.cube_query.column_hash.is_empty() {
            return Ok(Vec::new());
        }
        self.get_table_column_names(&self.cube_query.column_hash)
            .await
    }

    /// Get column names from the row facet table schema
    pub async fn rnames(&self) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        if self.cube_query.row_hash.is_empty() {
            return Ok(Vec::new());
        }
        self.get_table_column_names(&self.cube_query.row_hash).await
    }

    /// Internal helper to get column names from a table schema
    async fn get_table_column_names(
        &self,
        table_id: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let streamer = self.streamer();
        let schema = streamer.get_schema(table_id).await?;

        // Extract column names from schema
        use crate::client::proto::{e_column_schema, e_schema};

        // Helper to extract name from EColumnSchema
        let extract_name = |c: &crate::client::proto::EColumnSchema| -> Option<String> {
            if let Some(e_column_schema::Object::Columnschema(cs)) = &c.object {
                Some(cs.name.clone())
            } else {
                None
            }
        };

        let names = match schema.object.as_ref() {
            Some(e_schema::Object::Schema(s)) => {
                s.columns.iter().filter_map(extract_name).collect()
            }
            Some(e_schema::Object::Cubequerytableschema(cqts)) => {
                cqts.columns.iter().filter_map(extract_name).collect()
            }
            Some(e_schema::Object::Computedtableschema(cts)) => {
                cts.columns.iter().filter_map(extract_name).collect()
            }
            Some(e_schema::Object::Tableschema(ts)) => {
                ts.columns.iter().filter_map(extract_name).collect()
            }
            None => Vec::new(),
        };
        Ok(names)
    }

    // === Result Save Methods (async) ===

    /// Save a single PNG plot result back to Tercen
    ///
    /// # Arguments
    /// * `png_buffer` - Raw PNG bytes from the renderer
    /// * `width` - Plot width in pixels
    /// * `height` - Plot height in pixels
    /// * `task` - Mutable reference to the task
    pub async fn save_result(
        &self,
        png_buffer: Vec<u8>,
        width: i32,
        height: i32,
        output_ext: &str,
        filename: &str,
        task: &mut ETask,
    ) -> Result<(), Box<dyn std::error::Error>> {
        crate::result::save_result(
            Arc::clone(&self.client),
            &self.project_id,
            &self.namespace,
            png_buffer,
            width,
            height,
            output_ext,
            filename,
            task,
        )
        .await
    }

    /// Save multiple PNG plot results back to Tercen (multi-page)
    ///
    /// # Arguments
    /// * `plots` - Vector of PlotResult structs
    /// * `task` - Mutable reference to the task
    pub async fn save_results(
        &self,
        plots: Vec<PlotResult>,
        task: &mut ETask,
    ) -> Result<(), Box<dyn std::error::Error>> {
        crate::result::save_results(
            Arc::clone(&self.client),
            &self.project_id,
            &self.namespace,
            plots,
            task,
        )
        .await
    }
}

/// Builder for constructing ContextBase
///
/// Used by ProductionContext and DevContext constructors to build the common base.
pub struct ContextBaseBuilder {
    client: Option<Arc<TercenClient>>,
    cube_query: Option<CubeQuery>,
    schema_ids: Vec<String>,
    workflow_id: String,
    step_id: String,
    project_id: String,
    namespace: String,
    operator_settings: Option<OperatorSettings>,
    color_infos: Vec<ColorInfo>,
    page_factors: Vec<String>,
    y_axis_table_id: Option<String>,
    x_axis_table_id: Option<String>,
    point_size: Option<i32>,
    chart_kind: ChartKind,
    crosstab_dimensions: Option<(i32, i32)>,
    y_transform: Option<String>,
    x_transform: Option<String>,
    layer_palette_name: Option<String>,
    per_layer_colors: Option<PerLayerColorConfig>,
    layer_y_factor_names: Vec<String>,
}

impl Default for ContextBaseBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ContextBaseBuilder {
    pub fn new() -> Self {
        Self {
            client: None,
            cube_query: None,
            schema_ids: Vec::new(),
            workflow_id: String::new(),
            step_id: String::new(),
            project_id: String::new(),
            namespace: String::new(),
            operator_settings: None,
            color_infos: Vec::new(),
            page_factors: Vec::new(),
            y_axis_table_id: None,
            x_axis_table_id: None,
            point_size: None,
            chart_kind: ChartKind::Point,
            crosstab_dimensions: None,
            y_transform: None,
            x_transform: None,
            layer_palette_name: None,
            per_layer_colors: None,
            layer_y_factor_names: Vec::new(),
        }
    }

    pub fn client(mut self, client: Arc<TercenClient>) -> Self {
        self.client = Some(client);
        self
    }

    pub fn cube_query(mut self, cube_query: CubeQuery) -> Self {
        self.cube_query = Some(cube_query);
        self
    }

    pub fn schema_ids(mut self, schema_ids: Vec<String>) -> Self {
        self.schema_ids = schema_ids;
        self
    }

    pub fn workflow_id(mut self, workflow_id: String) -> Self {
        self.workflow_id = workflow_id;
        self
    }

    pub fn step_id(mut self, step_id: String) -> Self {
        self.step_id = step_id;
        self
    }

    pub fn project_id(mut self, project_id: String) -> Self {
        self.project_id = project_id;
        self
    }

    pub fn namespace(mut self, namespace: String) -> Self {
        self.namespace = namespace;
        self
    }

    pub fn operator_settings(mut self, operator_settings: Option<OperatorSettings>) -> Self {
        self.operator_settings = operator_settings;
        self
    }

    pub fn color_infos(mut self, color_infos: Vec<ColorInfo>) -> Self {
        self.color_infos = color_infos;
        self
    }

    pub fn page_factors(mut self, page_factors: Vec<String>) -> Self {
        self.page_factors = page_factors;
        self
    }

    pub fn y_axis_table_id(mut self, y_axis_table_id: Option<String>) -> Self {
        self.y_axis_table_id = y_axis_table_id;
        self
    }

    pub fn x_axis_table_id(mut self, x_axis_table_id: Option<String>) -> Self {
        self.x_axis_table_id = x_axis_table_id;
        self
    }

    pub fn point_size(mut self, point_size: Option<i32>) -> Self {
        self.point_size = point_size;
        self
    }

    pub fn chart_kind(mut self, chart_kind: ChartKind) -> Self {
        self.chart_kind = chart_kind;
        self
    }

    pub fn crosstab_dimensions(mut self, crosstab_dimensions: Option<(i32, i32)>) -> Self {
        self.crosstab_dimensions = crosstab_dimensions;
        self
    }

    pub fn y_transform(mut self, y_transform: Option<String>) -> Self {
        self.y_transform = y_transform;
        self
    }

    pub fn x_transform(mut self, x_transform: Option<String>) -> Self {
        self.x_transform = x_transform;
        self
    }

    pub fn layer_palette_name(mut self, layer_palette_name: Option<String>) -> Self {
        self.layer_palette_name = layer_palette_name;
        self
    }

    pub fn per_layer_colors(mut self, per_layer_colors: Option<PerLayerColorConfig>) -> Self {
        self.per_layer_colors = per_layer_colors;
        self
    }

    pub fn layer_y_factor_names(mut self, names: Vec<String>) -> Self {
        self.layer_y_factor_names = names;
        self
    }

    /// Build the ContextBase, returning an error if required fields are missing
    pub fn build(self) -> Result<ContextBase, Box<dyn std::error::Error>> {
        let client = self
            .client
            .ok_or("ContextBaseBuilder: client is required")?;
        let cube_query = self
            .cube_query
            .ok_or("ContextBaseBuilder: cube_query is required")?;

        Ok(ContextBase {
            client,
            cube_query,
            schema_ids: self.schema_ids,
            workflow_id: self.workflow_id,
            step_id: self.step_id,
            project_id: self.project_id,
            namespace: self.namespace,
            operator_settings: self.operator_settings,
            color_infos: self.color_infos,
            page_factors: self.page_factors,
            y_axis_table_id: self.y_axis_table_id,
            x_axis_table_id: self.x_axis_table_id,
            point_size: self.point_size,
            chart_kind: self.chart_kind,
            crosstab_dimensions: self.crosstab_dimensions,
            y_transform: self.y_transform,
            x_transform: self.x_transform,
            layer_palette_name: self.layer_palette_name,
            per_layer_colors: self.per_layer_colors,
            layer_y_factor_names: self.layer_y_factor_names,
        })
    }
}
