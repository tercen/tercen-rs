//! Color palette handling and RGB interpolation for continuous and categorical color scales
//!
//! This module provides functionality to:
//! - Parse Tercen color palettes (JetPalette, RampPalette, CategoryPalette)
//! - Interpolate color values to RGB (continuous)
//! - Map category strings to RGB (categorical)
//! - Extract color information from workflow steps

use crate::client::proto;
use crate::error::{Result, TercenError};
use std::collections::HashMap;

/// Information about a color factor and its associated palette
#[derive(Debug, Clone)]
pub struct ColorInfo {
    /// Name of the column containing color values (e.g., "Age", "Country")
    pub factor_name: String,
    /// Type of the factor (e.g., "double", "int32", "string")
    pub factor_type: String,
    /// The color mapping for this factor
    pub mapping: ColorMapping,
    /// Optional color table ID (for categorical colors with .colorLevels)
    /// This table contains the mapping from level index to category name
    pub color_table_id: Option<String>,
    /// Quartiles from the color column schema metadata.
    /// Used to rescale the palette when is_user_defined=false.
    /// Format: [Q1, Q2, Q3, min, max] as strings
    pub quartiles: Option<Vec<String>>,
    /// Number of categorical levels (from color table schema nRows)
    /// Used to generate legend labels without streaming the table
    pub n_levels: Option<usize>,
    /// Actual category labels from the color table (for categorical colors).
    /// These are the values from the factor column in the color table.
    /// When available, used instead of generic "Level N" labels.
    pub color_labels: Option<Vec<String>>,
}

/// Color mapping - either continuous interpolation or categorical lookup
#[derive(Debug, Clone)]
pub enum ColorMapping {
    /// Continuous color scale: numeric value → RGB via interpolation
    Continuous(ColorPalette),
    /// Categorical color scale: string value → RGB via lookup
    Categorical(CategoryColorMap),
}

/// A color palette with sorted color stops for interpolation
#[derive(Debug, Clone)]
pub struct ColorPalette {
    /// Sorted list of color stops (by value, ascending)
    pub stops: Vec<ColorStop>,
    /// Whether the user explicitly defined the color breakpoints.
    /// If false, the palette should be rescaled based on data quartiles.
    pub is_user_defined: bool,
}

/// A single color stop in a palette
#[derive(Debug, Clone, PartialEq)]
pub struct ColorStop {
    /// Numeric value at this stop
    pub value: f64,
    /// RGB color at this stop
    pub color: [u8; 3], // [r, g, b]
}

/// Categorical color mapping: string → RGB
#[derive(Debug, Clone)]
pub struct CategoryColorMap {
    /// Map from category string to RGB color
    pub mappings: HashMap<String, [u8; 3]>,
    /// Default color for unknown categories
    pub default_color: [u8; 3],
}

impl ColorPalette {
    /// Create a new empty palette
    pub fn new() -> Self {
        ColorPalette {
            stops: Vec::new(),
            is_user_defined: true, // Default to user-defined
        }
    }

    /// Add a color stop and maintain sorted order
    pub fn add_stop(&mut self, value: f64, color: [u8; 3]) {
        let stop = ColorStop { value, color };
        // Insert in sorted position
        match self
            .stops
            .binary_search_by(|s| s.value.partial_cmp(&value).unwrap())
        {
            Ok(pos) => self.stops[pos] = stop, // Replace if exists
            Err(pos) => self.stops.insert(pos, stop),
        }
    }

    /// Get the value range of this palette
    pub fn range(&self) -> Option<(f64, f64)> {
        if self.stops.is_empty() {
            None
        } else {
            Some((
                self.stops.first().unwrap().value,
                self.stops.last().unwrap().value,
            ))
        }
    }

    /// Rescale the palette based on quartiles.
    ///
    /// When `is_user_defined=false`, Tercen auto-scales the palette based on data quartiles.
    /// The formula is:
    /// - min = Q2 - 1.5 * IQR (where IQR = Q3 - Q1)
    /// - max = Q2 + 1.5 * IQR
    /// - middle = (min + max) / 2
    ///
    /// The existing color stops are linearly remapped from their original positions
    /// to the new [min, middle, max] range.
    ///
    /// # Arguments
    /// * `quartiles` - Array of quartile values as strings: [Q1, Q2, Q3, min, max]
    ///
    /// # Returns
    /// A new palette with rescaled stops, or the original palette if rescaling fails.
    pub fn rescale_from_quartiles(&self, quartiles: &[String]) -> Self {
        // Need at least Q1, Q2, Q3 (first 3 values)
        if quartiles.len() < 3 {
            eprintln!(
                "DEBUG rescale_from_quartiles: Not enough quartiles ({} < 3), returning unchanged",
                quartiles.len()
            );
            return self.clone();
        }

        // Parse Q1, Q2, Q3
        let q1: f64 = match quartiles[0].parse() {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "DEBUG rescale_from_quartiles: Failed to parse Q1 '{}': {}",
                    quartiles[0], e
                );
                return self.clone();
            }
        };
        let q2: f64 = match quartiles[1].parse() {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "DEBUG rescale_from_quartiles: Failed to parse Q2 '{}': {}",
                    quartiles[1], e
                );
                return self.clone();
            }
        };
        let q3: f64 = match quartiles[2].parse() {
            Ok(v) => v,
            Err(e) => {
                eprintln!(
                    "DEBUG rescale_from_quartiles: Failed to parse Q3 '{}': {}",
                    quartiles[2], e
                );
                return self.clone();
            }
        };

        // Calculate IQR and new range
        let iqr = q3 - q1;
        let new_min = q2 - 1.5 * iqr;
        let new_max = q2 + 1.5 * iqr;
        let new_middle = (new_min + new_max) / 2.0;

        eprintln!(
            "DEBUG rescale_from_quartiles: Q1={:.2}, Q2={:.2}, Q3={:.2}, IQR={:.2}",
            q1, q2, q3, iqr
        );
        eprintln!(
            "DEBUG rescale_from_quartiles: new_min={:.2}, new_middle={:.2}, new_max={:.2}",
            new_min, new_middle, new_max
        );

        // Get current palette range
        let (old_min, old_max) = match self.range() {
            Some(r) => r,
            None => return self.clone(),
        };

        eprintln!(
            "DEBUG rescale_from_quartiles: old_min={:.2}, old_max={:.2}",
            old_min, old_max
        );

        // Create new palette with rescaled stops
        let mut new_palette = ColorPalette {
            stops: Vec::with_capacity(self.stops.len()),
            is_user_defined: true, // After rescaling, it's effectively "user defined"
        };

        // Linear remap from [old_min, old_max] to [new_min, new_max]
        for stop in &self.stops {
            let t = if old_max > old_min {
                (stop.value - old_min) / (old_max - old_min)
            } else {
                0.5
            };
            let new_value = new_min + t * (new_max - new_min);

            eprintln!(
                "DEBUG rescale_from_quartiles: stop {:.2} -> {:.2} (t={:.2})",
                stop.value, new_value, t
            );

            new_palette.stops.push(ColorStop {
                value: new_value,
                color: stop.color,
            });
        }

        new_palette
    }
}

impl Default for ColorPalette {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a Tercen EPalette proto into a ColorMapping
pub fn parse_palette(e_palette: &proto::EPalette) -> Result<ColorMapping> {
    let palette_obj = e_palette
        .object
        .as_ref()
        .ok_or_else(|| TercenError::Data("EPalette has no object".to_string()))?;

    match palette_obj {
        proto::e_palette::Object::Jetpalette(jet) => {
            Ok(ColorMapping::Continuous(parse_jet_palette(jet)?))
        }
        proto::e_palette::Object::Ramppalette(ramp) => {
            Ok(ColorMapping::Continuous(parse_ramp_palette(ramp)?))
        }
        proto::e_palette::Object::Categorypalette(cat) => {
            Ok(ColorMapping::Categorical(parse_category_palette(cat)?))
        }
        proto::e_palette::Object::Palette(_) => Err(TercenError::Data(
            "Base Palette type not supported".to_string(),
        )),
    }
}

/// Parse a JetPalette into a ColorPalette
fn parse_jet_palette(jet: &proto::JetPalette) -> Result<ColorPalette> {
    eprintln!(
        "DEBUG parse_jet_palette: is_user_defined = {}",
        jet.is_user_defined
    );
    parse_double_color_elements(&jet.double_color_elements, jet.is_user_defined, "Jet")
}

/// Parse a RampPalette into a ColorPalette
fn parse_ramp_palette(ramp: &proto::RampPalette) -> Result<ColorPalette> {
    // Extract palette name from properties (property with name="name")
    let palette_name = ramp
        .properties
        .iter()
        .find(|p| p.name == "name")
        .map(|p| p.value.as_str())
        .unwrap_or("Spectral"); // Fallback to Spectral if not specified

    // "Divergent" is a special type where user defines min/middle/max colors manually.
    // Always use element colors for Divergent, regardless of is_user_defined flag.
    let use_element_colors = ramp.is_user_defined || palette_name.eq_ignore_ascii_case("divergent");

    eprintln!(
        "DEBUG parse_ramp_palette: is_user_defined = {}, palette_name = '{}', use_element_colors = {}",
        ramp.is_user_defined, palette_name, use_element_colors
    );

    parse_double_color_elements(
        &ramp.double_color_elements,
        use_element_colors,
        palette_name,
    )
}

/// Parse a CategoryPalette into a CategoryColorMap
///
/// For categorical colors, Tercen stores color levels (indices) in the `.colorLevels` column
/// of the main data table. The actual category strings are in a separate color table.
///
/// If the palette has `stringColorElements`, use those explicit mappings.
/// Otherwise, we'll create mappings later from the data (using `.colorLevels`).
fn parse_category_palette(cat: &proto::CategoryPalette) -> Result<CategoryColorMap> {
    let mut mappings = HashMap::new();

    eprintln!(
        "DEBUG parse_category_palette: Processing {} string color elements",
        cat.string_color_elements.len()
    );

    // If we have explicit string→color mappings, use them
    if !cat.string_color_elements.is_empty() {
        for (i, element) in cat.string_color_elements.iter().enumerate() {
            let category = element.string_value.clone();
            let rgb = int_to_rgb(element.color);

            eprintln!(
                "DEBUG parse_category_palette: [{}] '{}' → RGB({}, {}, {})",
                i, category, rgb[0], rgb[1], rgb[2]
            );

            mappings.insert(category, rgb);
        }
    } else {
        // No explicit mappings - colors will be generated from .colorLevels in the data
        // The actual mapping happens in the stream generator when we see the data
        eprintln!(
            "DEBUG parse_category_palette: No string_color_elements, will use .colorLevels from data"
        );
        if let Some(ref color_list) = cat.color_list {
            eprintln!(
                "DEBUG parse_category_palette: ColorList name: '{}'",
                color_list.name
            );
        }
    }

    Ok(CategoryColorMap {
        mappings,
        default_color: [128, 128, 128], // Gray for unknown categories
    })
}

/// Parse DoubleColorElement array into ColorPalette
///
/// When color_int == -1, all colors from the named palette are distributed
/// across the value range [min, max] from the elements.
fn parse_double_color_elements(
    elements: &[proto::DoubleColorElement],
    is_user_defined: bool,
    default_palette_name: &str,
) -> Result<ColorPalette> {
    use crate::palettes::PALETTE_REGISTRY;

    let mut palette = ColorPalette::new();
    palette.is_user_defined = is_user_defined;

    if elements.is_empty() {
        return Err(TercenError::Data(
            "Palette has no color elements".to_string(),
        ));
    }

    // Use is_user_defined to decide whether to use element colors or named palette.
    // When is_user_defined=false, Tercen sends palette endpoint colors but we should
    // distribute ALL colors from the named palette across the range instead.
    if is_user_defined {
        // User-defined colors: use them directly from elements
        for element in elements {
            let value = element.string_value.parse::<f64>().map_err(|err| {
                TercenError::Data(format!(
                    "Invalid color value '{}': {}",
                    element.string_value, err
                ))
            })?;

            let color = int_to_rgb(element.color);
            eprintln!(
                "DEBUG parse_palette: User color at {}: RGB({}, {}, {})",
                value, color[0], color[1], color[2]
            );
            palette.add_stop(value, color);
        }
    } else {
        // No user colors: distribute named palette across [min, max]
        let values: Vec<f64> = elements
            .iter()
            .map(|e| e.string_value.parse::<f64>())
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| TercenError::Data(format!("Invalid value: {}", e)))?;

        let min_val = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_val = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        let named_palette = PALETTE_REGISTRY.get(default_palette_name).ok_or_else(|| {
            TercenError::Data(format!("Palette '{}' not found", default_palette_name))
        })?;

        let n_colors = named_palette.len();
        eprintln!(
            "DEBUG parse_palette: Distributing {} {} colors across [{}, {}]",
            n_colors, default_palette_name, min_val, max_val
        );

        for i in 0..n_colors {
            let t = if n_colors > 1 {
                i as f64 / (n_colors - 1) as f64
            } else {
                0.5
            };
            let value = min_val + t * (max_val - min_val);
            let color = named_palette.get_color(i);
            palette.add_stop(value, color);
        }
    }

    Ok(palette)
}

/// Convert Tercen color integer (AARRGGBB) to RGB array
///
/// Tercen stores colors as 32-bit integers with the format:
/// - Bits 24-31: Alpha (ignored for now)
/// - Bits 16-23: Red
/// - Bits 8-15: Green
/// - Bits 0-7: Blue
fn int_to_rgb(color_int: i32) -> [u8; 3] {
    let color = color_int as u32;
    [
        ((color >> 16) & 0xFF) as u8, // Red
        ((color >> 8) & 0xFF) as u8,  // Green
        (color & 0xFF) as u8,         // Blue
    ]
}

/// Extract color information from a workflow step
///
/// Navigates to step.model.axis.xyAxis[0].colors and extracts:
/// - Color factors (column names and types)
/// - Associated palettes
/// - Optional color table IDs (indexed by factor position, e.g., color_0, color_1)
///
/// Returns a Vec<ColorInfo> (can be empty if no colors defined)
pub fn extract_color_info_from_step(
    workflow: &proto::Workflow,
    step_id: &str,
    color_table_ids: &[Option<String>],
) -> Result<Vec<ColorInfo>> {
    // Find the step by ID
    let step = workflow
        .steps
        .iter()
        .find(|s| {
            if let Some(proto::e_step::Object::Datastep(ds)) = &s.object {
                ds.id == step_id
            } else {
                false
            }
        })
        .ok_or_else(|| TercenError::Data(format!("Step '{}' not found in workflow", step_id)))?;

    // Extract DataStep
    let data_step = match &step.object {
        Some(proto::e_step::Object::Datastep(ds)) => ds,
        _ => return Err(TercenError::Data("Step is not a DataStep".to_string())),
    };

    // Navigate to model.axis.xyAxis
    let model = data_step
        .model
        .as_ref()
        .ok_or_else(|| TercenError::Data("DataStep has no model".to_string()))?;

    let axis = model
        .axis
        .as_ref()
        .ok_or_else(|| TercenError::Data("Model has no axis".to_string()))?;

    // Get first xyAxis (usually there's only one for plot operators)
    let xy_axis = axis
        .xy_axis
        .first()
        .ok_or_else(|| TercenError::Data("Axis has no xyAxis array".to_string()))?;

    // Extract colors object
    let colors = match &xy_axis.colors {
        Some(c) => c,
        None => {
            eprintln!("DEBUG extract_color_info: No colors object in xyAxis");
            return Ok(Vec::new()); // No colors defined - this is OK
        }
    };

    eprintln!(
        "DEBUG extract_color_info: Found colors object with {} factors",
        colors.factors.len()
    );
    eprintln!(
        "DEBUG extract_color_info: Palette present: {}",
        colors.palette.is_some()
    );

    // Parse each color factor
    let mut color_infos = Vec::new();
    for (i, factor) in colors.factors.iter().enumerate() {
        eprintln!(
            "DEBUG extract_color_info: Processing factor {}: name='{}', type='{}'",
            i, factor.name, factor.r#type
        );

        // Parse the palette/mapping
        let mapping = match &colors.palette {
            Some(p) => {
                eprintln!("DEBUG extract_color_info: Calling parse_palette...");
                let parsed = parse_palette(p)?;
                match &parsed {
                    ColorMapping::Continuous(palette) => {
                        eprintln!(
                            "DEBUG extract_color_info: Continuous palette with {} stops",
                            palette.stops.len()
                        );
                    }
                    ColorMapping::Categorical(color_map) => {
                        eprintln!(
                            "DEBUG extract_color_info: Categorical palette with {} categories",
                            color_map.mappings.len()
                        );
                    }
                }
                parsed
            }
            None => {
                return Err(TercenError::Data(
                    "Color factors defined but no palette provided".to_string(),
                ))
            }
        };

        // Get the color table ID for this factor (if available)
        let color_table_id = color_table_ids.get(i).and_then(|opt| opt.clone());

        color_infos.push(ColorInfo {
            factor_name: factor.name.clone(),
            factor_type: factor.r#type.clone(),
            mapping,
            color_table_id,
            quartiles: None,    // Will be populated later from column schema metadata
            n_levels: None,     // Will be populated later from color table schema nRows
            color_labels: None, // Will be populated later from color table data
        });
    }

    eprintln!(
        "DEBUG extract_color_info: Returning {} ColorInfo objects",
        color_infos.len()
    );
    Ok(color_infos)
}

/// Configuration for how a single layer gets its colors
///
/// Each layer has exactly one color configuration:
/// - Continuous: interpolate values using a palette (e.g., Jet, Viridis)
/// - Categorical: map discrete levels to colors
/// - Constant: all points in layer get the same pre-computed color
#[derive(Debug, Clone)]
pub enum LayerColorConfig {
    /// Layer has a continuous color factor - interpolate using palette
    Continuous {
        palette: ColorPalette,
        factor_name: String,
        quartiles: Option<Vec<String>>,
        color_table_id: Option<String>,
    },
    /// Layer has a categorical color factor - map levels to colors
    Categorical {
        color_map: CategoryColorMap,
        factor_name: String,
        color_table_id: Option<String>,
    },
    /// Layer has no color factor - all points get this constant color
    Constant { color: [u8; 3] },
}

impl LayerColorConfig {
    /// Get the factor name if this config uses a color factor
    pub fn factor_name(&self) -> Option<&str> {
        match self {
            LayerColorConfig::Continuous { factor_name, .. } => Some(factor_name),
            LayerColorConfig::Categorical { factor_name, .. } => Some(factor_name),
            LayerColorConfig::Constant { .. } => None,
        }
    }

    /// Check if this is a continuous mapping
    pub fn is_continuous(&self) -> bool {
        matches!(self, LayerColorConfig::Continuous { .. })
    }

    /// Check if this is a categorical mapping
    pub fn is_categorical(&self) -> bool {
        matches!(self, LayerColorConfig::Categorical { .. })
    }

    /// Check if this is a constant color (no color factor)
    pub fn is_constant(&self) -> bool {
        matches!(self, LayerColorConfig::Constant { .. })
    }

    /// Get the palette for continuous mappings
    pub fn palette(&self) -> Option<&ColorPalette> {
        match self {
            LayerColorConfig::Continuous { palette, .. } => Some(palette),
            _ => None,
        }
    }

    /// Get quartiles for continuous mappings
    pub fn quartiles(&self) -> Option<&Vec<String>> {
        match self {
            LayerColorConfig::Continuous { quartiles, .. } => quartiles.as_ref(),
            _ => None,
        }
    }

    /// Set quartiles for continuous mappings
    pub fn set_quartiles(&mut self, q: Vec<String>) {
        if let LayerColorConfig::Continuous { quartiles, .. } = self {
            *quartiles = Some(q);
        }
    }

    /// Get color table ID if available
    pub fn color_table_id(&self) -> Option<&str> {
        match self {
            LayerColorConfig::Continuous { color_table_id, .. } => color_table_id.as_deref(),
            LayerColorConfig::Categorical { color_table_id, .. } => color_table_id.as_deref(),
            LayerColorConfig::Constant { .. } => None,
        }
    }

    /// Set color table ID
    pub fn set_color_table_id(&mut self, id: String) {
        match self {
            LayerColorConfig::Continuous { color_table_id, .. } => *color_table_id = Some(id),
            LayerColorConfig::Categorical { color_table_id, .. } => *color_table_id = Some(id),
            LayerColorConfig::Constant { .. } => {}
        }
    }
}

/// Per-layer color configuration
///
/// Every layer has exactly one LayerColorConfig that determines how its
/// points are colored. This unified structure handles all scenarios:
/// - Layers with continuous color factors (interpolated palettes)
/// - Layers with categorical color factors (discrete mappings)
/// - Layers without color factors (constant colors from layer palette)
#[derive(Debug, Clone, Default)]
pub struct PerLayerColorConfig {
    /// Color configuration for each layer. Index = layer index (axisIndex).
    pub layer_configs: Vec<LayerColorConfig>,
    /// Total number of layers
    pub n_layers: usize,
}

impl PerLayerColorConfig {
    /// Check if any layer has explicit colors (not constant)
    pub fn has_explicit_colors(&self) -> bool {
        self.layer_configs.iter().any(|c| !c.is_constant())
    }

    /// Check if any layer uses constant coloring (no color factor)
    pub fn has_constant_colors(&self) -> bool {
        self.layer_configs.iter().any(|c| c.is_constant())
    }

    /// Check if this is a mixed scenario (some layers have colors, some don't)
    pub fn is_mixed(&self) -> bool {
        self.has_explicit_colors() && self.has_constant_colors()
    }

    /// Get the config for a specific layer
    pub fn get(&self, layer_idx: usize) -> Option<&LayerColorConfig> {
        self.layer_configs.get(layer_idx)
    }

    /// Get mutable config for a specific layer
    pub fn get_mut(&mut self, layer_idx: usize) -> Option<&mut LayerColorConfig> {
        self.layer_configs.get_mut(layer_idx)
    }

    /// Get all color factor names across all layers (excludes constant-color layers)
    pub fn all_color_factor_names(&self) -> Vec<String> {
        self.layer_configs
            .iter()
            .filter_map(|c| c.factor_name().map(|s| s.to_string()))
            .collect()
    }

    /// Check if any layer uses categorical colors
    pub fn has_categorical(&self) -> bool {
        self.layer_configs.iter().any(|c| c.is_categorical())
    }

    /// Check if any layer uses continuous colors
    pub fn has_continuous(&self) -> bool {
        self.layer_configs.iter().any(|c| c.is_continuous())
    }

    // Legacy compatibility methods (to be removed after full migration)

    /// Legacy: Check if any layer needs layer-based coloring
    #[deprecated(note = "Use has_constant_colors() instead")]
    pub fn has_layers_needing_layer_colors(&self) -> bool {
        self.has_constant_colors()
    }

    /// Legacy: Get the ColorInfo for a specific layer (if any)
    /// Returns None for constant-color layers
    #[deprecated(note = "Use get() and match on LayerColorConfig instead")]
    pub fn get_color_info(&self, layer_idx: usize) -> Option<ColorInfo> {
        match self.layer_configs.get(layer_idx)? {
            LayerColorConfig::Continuous {
                palette,
                factor_name,
                quartiles,
                color_table_id,
            } => Some(ColorInfo {
                factor_name: factor_name.clone(),
                factor_type: "double".to_string(),
                mapping: ColorMapping::Continuous(palette.clone()),
                color_table_id: color_table_id.clone(),
                quartiles: quartiles.clone(),
                n_levels: None,
                color_labels: None,
            }),
            LayerColorConfig::Categorical {
                color_map,
                factor_name,
                color_table_id,
            } => Some(ColorInfo {
                factor_name: factor_name.clone(),
                factor_type: "string".to_string(),
                mapping: ColorMapping::Categorical(color_map.clone()),
                color_table_id: color_table_id.clone(),
                quartiles: None,
                n_levels: None,
                color_labels: None,
            }),
            LayerColorConfig::Constant { .. } => None,
        }
    }
}

/// Extract palette name from an EPalette
fn extract_palette_name_from_epalette(palette: &proto::EPalette) -> Option<String> {
    use proto::e_palette::Object as PaletteObject;

    match &palette.object {
        Some(PaletteObject::Categorypalette(cat)) => {
            // Try colorList.name first
            cat.color_list
                .as_ref()
                .and_then(|cl| {
                    if !cl.name.is_empty() {
                        Some(cl.name.clone())
                    } else {
                        None
                    }
                })
                // Fallback to properties["name"]
                .or_else(|| {
                    cat.properties
                        .iter()
                        .find(|p| p.name == "name")
                        .map(|p| p.value.clone())
                })
        }
        Some(PaletteObject::Ramppalette(ramp)) => ramp
            .properties
            .iter()
            .find(|p| p.name == "name")
            .map(|p| p.value.clone()),
        Some(PaletteObject::Jetpalette(_)) => Some("Jet".to_string()),
        Some(PaletteObject::Palette(p)) => p
            .properties
            .iter()
            .find(|p| p.name == "name")
            .map(|p| p.value.clone()),
        None => None,
    }
}

/// Get a constant color for a layer without color factors
///
/// Uses the layer's palette at the layer index to determine the color.
fn get_constant_color_for_layer(layer_idx: usize, palette_name: Option<&str>) -> [u8; 3] {
    use crate::palettes::{DEFAULT_CATEGORICAL_PALETTE, PALETTE_REGISTRY};

    let effective_name = palette_name.unwrap_or(DEFAULT_CATEGORICAL_PALETTE);

    let color = PALETTE_REGISTRY
        .get(effective_name)
        .map(|p| p.get_color(layer_idx))
        .unwrap_or_else(|| {
            // Fallback to default palette if named palette not found
            PALETTE_REGISTRY
                .get(DEFAULT_CATEGORICAL_PALETTE)
                .map(|p| p.get_color(layer_idx))
                .unwrap_or([128, 128, 128])
        });

    eprintln!(
        "DEBUG get_constant_color_for_layer: layer {} using palette '{}' -> RGB({},{},{})",
        layer_idx, effective_name, color[0], color[1], color[2]
    );

    color
}

/// Extract color information for each layer from a workflow step
///
/// Navigates to step.model.axis.xyAxis[i].colors for each layer and extracts:
/// - For layers with color factors: Continuous or Categorical config
/// - For layers without color factors: Constant color from layer's palette at layer_idx
///
/// Returns a PerLayerColorConfig with a LayerColorConfig for every layer.
pub fn extract_per_layer_color_info(
    workflow: &proto::Workflow,
    step_id: &str,
    color_table_ids: &[Option<String>],
) -> Result<PerLayerColorConfig> {
    // Find the step by ID
    let step = workflow
        .steps
        .iter()
        .find(|s| {
            if let Some(proto::e_step::Object::Datastep(ds)) = &s.object {
                ds.id == step_id
            } else {
                false
            }
        })
        .ok_or_else(|| TercenError::Data(format!("Step '{}' not found in workflow", step_id)))?;

    // Extract DataStep
    let data_step = match &step.object {
        Some(proto::e_step::Object::Datastep(ds)) => ds,
        _ => return Err(TercenError::Data("Step is not a DataStep".to_string())),
    };

    // Navigate to model.axis.xyAxis
    let model = data_step
        .model
        .as_ref()
        .ok_or_else(|| TercenError::Data("DataStep has no model".to_string()))?;

    let axis = model
        .axis
        .as_ref()
        .ok_or_else(|| TercenError::Data("Model has no axis".to_string()))?;

    let n_layers = axis.xy_axis.len();
    eprintln!(
        "DEBUG extract_per_layer_color_info: Found {} layers (xyAxis entries)",
        n_layers
    );

    let mut layer_configs: Vec<LayerColorConfig> = Vec::with_capacity(n_layers);

    for (layer_idx, xy_axis) in axis.xy_axis.iter().enumerate() {
        // Extract colors object for this layer
        let colors = match &xy_axis.colors {
            Some(c) => c,
            None => {
                // No colors object - use constant gray
                eprintln!(
                    "DEBUG extract_per_layer_color_info: Layer {} has no colors object, using gray",
                    layer_idx
                );
                layer_configs.push(LayerColorConfig::Constant {
                    color: [128, 128, 128],
                });
                continue;
            }
        };

        // Check if this layer has color factors
        if colors.factors.is_empty() {
            // No color factors - extract palette name and compute constant color
            let palette_name = colors
                .palette
                .as_ref()
                .and_then(extract_palette_name_from_epalette);

            eprintln!(
                "DEBUG extract_per_layer_color_info: Layer {} has no color factors, palette='{}', using constant color",
                layer_idx,
                palette_name.as_deref().unwrap_or("(none)")
            );

            let color = get_constant_color_for_layer(layer_idx, palette_name.as_deref());
            layer_configs.push(LayerColorConfig::Constant { color });
            continue;
        }

        // Layer has color factors - extract mapping
        eprintln!(
            "DEBUG extract_per_layer_color_info: Layer {} has {} color factors",
            layer_idx,
            colors.factors.len()
        );

        // For now, only use the first color factor per layer
        let factor = &colors.factors[0];
        eprintln!(
            "DEBUG extract_per_layer_color_info: Layer {} factor: name='{}', type='{}'",
            layer_idx, factor.name, factor.r#type
        );

        // Parse the palette/mapping
        let mapping = match &colors.palette {
            Some(p) => parse_palette(p)?,
            None => {
                // Has factors but no palette - use constant color as fallback
                eprintln!(
                    "DEBUG extract_per_layer_color_info: Layer {} has factors but no palette, using constant color",
                    layer_idx
                );
                let color = get_constant_color_for_layer(layer_idx, None);
                layer_configs.push(LayerColorConfig::Constant { color });
                continue;
            }
        };

        // Get the color table ID for this factor (if available)
        let color_table_id = color_table_ids.first().and_then(|opt| opt.clone());

        // Create appropriate LayerColorConfig based on mapping type
        let config = match mapping {
            ColorMapping::Continuous(palette) => {
                eprintln!(
                    "DEBUG extract_per_layer_color_info: Layer {} has continuous palette with {} stops",
                    layer_idx,
                    palette.stops.len()
                );
                LayerColorConfig::Continuous {
                    palette,
                    factor_name: factor.name.clone(),
                    quartiles: None,
                    color_table_id,
                }
            }
            ColorMapping::Categorical(color_map) => {
                eprintln!(
                    "DEBUG extract_per_layer_color_info: Layer {} has categorical palette with {} categories",
                    layer_idx,
                    color_map.mappings.len()
                );
                LayerColorConfig::Categorical {
                    color_map,
                    factor_name: factor.name.clone(),
                    color_table_id,
                }
            }
        };

        layer_configs.push(config);
    }

    let config = PerLayerColorConfig {
        layer_configs,
        n_layers,
    };

    eprintln!(
        "DEBUG extract_per_layer_color_info: Config - has_explicit={}, has_constant={}, is_mixed={}",
        config.has_explicit_colors(),
        config.has_constant_colors(),
        config.is_mixed()
    );

    Ok(config)
}

/// Extract the crosstab palette name for layer coloring
///
/// Returns the palette name from the crosstab's color configuration, even when
/// there are no color factors. This is used for layer-based coloring.
///
/// The palette name is extracted from:
/// - CategoryPalette.colorList.name
/// - RampPalette properties (name="name")
/// - JetPalette defaults to "Jet"
///
/// Returns None if no palette is configured.
pub fn extract_crosstab_palette_name(workflow: &proto::Workflow, step_id: &str) -> Option<String> {
    use proto::e_palette::Object as PaletteObject;
    use proto::e_step::Object as StepObject;

    // Find the step
    let step = workflow.steps.iter().find_map(|e_step| {
        if let Some(StepObject::Datastep(ds)) = &e_step.object {
            if ds.id == step_id {
                return Some(ds);
            }
        }
        None
    })?;

    // Navigate to xyAxis.colors.palette
    let model = step.model.as_ref()?;
    let axis = model.axis.as_ref()?;
    let xy_axis = axis.xy_axis.first()?;
    let colors = xy_axis.colors.as_ref()?;
    let palette = colors.palette.as_ref()?;
    let palette_obj = palette.object.as_ref()?;

    // Extract palette name based on type
    // Priority: colorList.name, then properties["name"]
    let name = match palette_obj {
        PaletteObject::Categorypalette(cat) => {
            // First try colorList.name
            let cl_name = cat.color_list.as_ref().and_then(|cl| {
                if !cl.name.is_empty() {
                    Some(cl.name.clone())
                } else {
                    None
                }
            });
            // Fallback to properties["name"]
            cl_name.or_else(|| {
                cat.properties
                    .iter()
                    .find(|p| p.name == "name")
                    .map(|p| p.value.clone())
            })
        }
        PaletteObject::Ramppalette(ramp) => ramp
            .properties
            .iter()
            .find(|p| p.name == "name")
            .map(|p| p.value.clone()),
        PaletteObject::Jetpalette(_) => Some("Jet".to_string()),
        PaletteObject::Palette(_) => None,
    };

    // Debug: Print available properties for CategoryPalette
    if let PaletteObject::Categorypalette(cat) = palette_obj {
        eprintln!(
            "DEBUG extract_crosstab_palette_name: CategoryPalette properties: {:?}",
            cat.properties
                .iter()
                .map(|p| format!("{}={}", p.name, p.value))
                .collect::<Vec<_>>()
        );
        if let Some(cl) = &cat.color_list {
            eprintln!(
                "DEBUG extract_crosstab_palette_name: colorList.name='{}'",
                cl.name
            );
        }
    }

    eprintln!(
        "DEBUG extract_crosstab_palette_name: palette type={}, name={:?}",
        match palette_obj {
            PaletteObject::Categorypalette(_) => "Category",
            PaletteObject::Ramppalette(_) => "Ramp",
            PaletteObject::Jetpalette(_) => "Jet",
            PaletteObject::Palette(_) => "Base",
        },
        name
    );

    name
}

/// Interpolate a color value using the palette
///
/// Uses linear interpolation between the surrounding color stops.
/// Values outside the palette range clamp to the min/max colors.
pub fn interpolate_color(value: f64, palette: &ColorPalette) -> [u8; 3] {
    if palette.stops.is_empty() {
        return [128, 128, 128]; // Gray default
    }

    let stops = &palette.stops;

    // Clamp to min
    if value <= stops.first().unwrap().value {
        return stops.first().unwrap().color;
    }

    // Clamp to max
    if value >= stops.last().unwrap().value {
        return stops.last().unwrap().color;
    }

    // Find surrounding stops using binary search
    let idx = stops.partition_point(|stop| stop.value < value);
    let lower = &stops[idx - 1];
    let upper = &stops[idx];

    // Linear interpolation
    let t = (value - lower.value) / (upper.value - lower.value);
    [
        (lower.color[0] as f64 * (1.0 - t) + upper.color[0] as f64 * t) as u8,
        (lower.color[1] as f64 * (1.0 - t) + upper.color[1] as f64 * t) as u8,
        (lower.color[2] as f64 * (1.0 - t) + upper.color[2] as f64 * t) as u8,
    ]
}

/// Extract point size from workflow step
///
/// Returns the pointSize from the chart configuration (1-10 scale from UI).
/// Returns None if not found, in which case the caller should use a default.
pub fn extract_point_size_from_step(
    workflow: &proto::Workflow,
    step_id: &str,
) -> Result<Option<i32>> {
    // Find the step
    let step = workflow
        .steps
        .iter()
        .find(|s| {
            if let Some(proto::e_step::Object::Datastep(ds)) = &s.object {
                ds.id == step_id
            } else {
                false
            }
        })
        .ok_or_else(|| TercenError::Data(format!("Step '{}' not found in workflow", step_id)))?;

    // Extract DataStep
    let data_step = match &step.object {
        Some(proto::e_step::Object::Datastep(ds)) => ds,
        _ => return Err(TercenError::Data("Step is not a DataStep".to_string())),
    };

    // Navigate to model.axis.xyAxis
    let model = match data_step.model.as_ref() {
        Some(m) => m,
        None => return Ok(None), // No model, use default
    };

    let axis = match model.axis.as_ref() {
        Some(a) => a,
        None => return Ok(None), // No axis, use default
    };

    // Get first xyAxis
    let xy_axis = match axis.xy_axis.first() {
        Some(xy) => xy,
        None => return Ok(None), // No xyAxis, use default
    };

    // Extract pointSize from chart
    let chart = match xy_axis.chart.as_ref() {
        Some(c) => c,
        None => return Ok(None), // No chart, use default
    };

    // Check the chart type and extract pointSize
    let point_size = match &chart.object {
        Some(proto::e_chart::Object::Chartpoint(cp)) => Some(cp.point_size),
        Some(proto::e_chart::Object::Chartline(cl)) => Some(cl.point_size),
        _ => None, // Other chart types don't have pointSize
    };

    eprintln!(
        "DEBUG extract_point_size: Found pointSize = {:?}",
        point_size
    );

    Ok(point_size)
}

/// Chart type variants supported by Tercen
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ChartKind {
    /// Scatter plot (default)
    #[default]
    Point,
    /// Heatmap (tile-based visualization)
    Heatmap,
    /// Line plot
    Line,
    /// Bar chart
    Bar,
}

/// Extract chart type from workflow step
///
/// Navigates: workflow.steps[step_id].model.axis.xyAxis[0].chart.object
/// Returns ChartKind based on the EChart variant.
pub fn extract_chart_kind_from_step(
    workflow: &proto::Workflow,
    step_id: &str,
) -> Result<ChartKind> {
    // Find the step - check both DataStep and CrossTabStep
    let step = workflow
        .steps
        .iter()
        .find(|s| match &s.object {
            Some(proto::e_step::Object::Datastep(ds)) => ds.id == step_id,
            Some(proto::e_step::Object::Crosstabstep(cs)) => cs.id == step_id,
            _ => false,
        })
        .ok_or_else(|| TercenError::Data(format!("Step '{}' not found in workflow", step_id)))?;

    // Extract the Crosstab model (both DataStep and CrossTabStep have it)
    let model = match &step.object {
        Some(proto::e_step::Object::Datastep(ds)) => ds.model.as_ref(),
        Some(proto::e_step::Object::Crosstabstep(cs)) => cs.model.as_ref(),
        _ => {
            return Err(TercenError::Data(
                "Step type does not have a model".to_string(),
            ))
        }
    }
    .ok_or_else(|| TercenError::Data("Step has no model".to_string()))?;

    // Navigate to model.axis.xyAxis
    let axis = match model.axis.as_ref() {
        Some(a) => a,
        None => {
            eprintln!("DEBUG extract_chart_kind: No axis in model, defaulting to Point");
            return Ok(ChartKind::Point);
        }
    };

    // Get first xyAxis
    let xy_axis = match axis.xy_axis.first() {
        Some(xy) => xy,
        None => {
            eprintln!("DEBUG extract_chart_kind: No xyAxis, defaulting to Point");
            return Ok(ChartKind::Point);
        }
    };

    // Extract chart type from EChart
    let chart = match xy_axis.chart.as_ref() {
        Some(c) => c,
        None => {
            eprintln!("DEBUG extract_chart_kind: No chart in xyAxis, defaulting to Point");
            return Ok(ChartKind::Point);
        }
    };

    // Map EChart variant to ChartKind
    let chart_kind = match &chart.object {
        Some(proto::e_chart::Object::Chartpoint(_)) => ChartKind::Point,
        Some(proto::e_chart::Object::Chartheatmap(_)) => ChartKind::Heatmap,
        Some(proto::e_chart::Object::Chartline(_)) => ChartKind::Line,
        Some(proto::e_chart::Object::Chartbar(_)) => ChartKind::Bar,
        Some(proto::e_chart::Object::Chart(_)) => ChartKind::Point, // Generic chart defaults to point
        Some(proto::e_chart::Object::Chartsize(_)) => ChartKind::Point, // Size chart treated as point
        None => ChartKind::Point,
    };

    eprintln!(
        "DEBUG extract_chart_kind: Found chart type = {:?}",
        chart_kind
    );

    Ok(chart_kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_to_rgb() {
        // White: 0xFFFFFFFF
        assert_eq!(int_to_rgb(-1), [255, 255, 255]);

        // Red: 0x00FF0000
        assert_eq!(int_to_rgb(0x00FF0000u32 as i32), [255, 0, 0]);

        // Green: 0x0000FF00
        assert_eq!(int_to_rgb(0x0000FF00u32 as i32), [0, 255, 0]);

        // Blue: 0x000000FF
        assert_eq!(int_to_rgb(0x000000FFu32 as i32), [0, 0, 255]);

        // Gray: 0x00808080
        assert_eq!(int_to_rgb(0x00808080u32 as i32), [128, 128, 128]);
    }

    #[test]
    fn test_palette_add_stop() {
        let mut palette = ColorPalette::new();
        palette.add_stop(0.0, [0, 0, 0]);
        palette.add_stop(100.0, [255, 255, 255]);
        palette.add_stop(50.0, [128, 128, 128]);

        assert_eq!(palette.stops.len(), 3);
        assert_eq!(palette.stops[0].value, 0.0);
        assert_eq!(palette.stops[1].value, 50.0);
        assert_eq!(palette.stops[2].value, 100.0);
        assert!(palette.is_user_defined); // Default is true
    }

    #[test]
    fn test_palette_rescale_from_quartiles() {
        // Create a palette with stops at 0, 50, 100
        let mut palette = ColorPalette::new();
        palette.is_user_defined = false;
        palette.add_stop(0.0, [0, 0, 255]); // Blue at min
        palette.add_stop(50.0, [255, 255, 255]); // White at middle
        palette.add_stop(100.0, [255, 0, 0]); // Red at max

        // Quartiles: Q1=40, Q2=50, Q3=60
        // IQR = 60 - 40 = 20
        // new_min = 50 - 1.5 * 20 = 20
        // new_max = 50 + 1.5 * 20 = 80
        let quartiles = vec!["40".to_string(), "50".to_string(), "60".to_string()];
        let rescaled = palette.rescale_from_quartiles(&quartiles);

        // Check that the stops have been rescaled
        assert_eq!(rescaled.stops.len(), 3);
        assert!((rescaled.stops[0].value - 20.0).abs() < 0.001); // min -> 20
        assert!((rescaled.stops[1].value - 50.0).abs() < 0.001); // middle -> 50
        assert!((rescaled.stops[2].value - 80.0).abs() < 0.001); // max -> 80

        // Colors should be preserved
        assert_eq!(rescaled.stops[0].color, [0, 0, 255]);
        assert_eq!(rescaled.stops[1].color, [255, 255, 255]);
        assert_eq!(rescaled.stops[2].color, [255, 0, 0]);
    }

    #[test]
    fn test_interpolate_color_edge_cases() {
        let mut palette = ColorPalette::new();
        palette.add_stop(0.0, [0, 0, 0]);
        palette.add_stop(100.0, [255, 255, 255]);

        // Below min - clamps to first color
        assert_eq!(interpolate_color(-10.0, &palette), [0, 0, 0]);

        // At min
        assert_eq!(interpolate_color(0.0, &palette), [0, 0, 0]);

        // At max
        assert_eq!(interpolate_color(100.0, &palette), [255, 255, 255]);

        // Above max - clamps to last color
        assert_eq!(interpolate_color(110.0, &palette), [255, 255, 255]);
    }

    #[test]
    fn test_interpolate_color_midpoint() {
        let mut palette = ColorPalette::new();
        palette.add_stop(0.0, [0, 0, 0]);
        palette.add_stop(100.0, [100, 200, 255]);

        // Midpoint
        let mid = interpolate_color(50.0, &palette);
        assert_eq!(mid, [50, 100, 127]); // (0+100)/2, (0+200)/2, (0+255)/2 rounded
    }

    #[test]
    fn test_palette_range() {
        let mut palette = ColorPalette::new();
        assert_eq!(palette.range(), None);

        palette.add_stop(10.0, [0, 0, 0]);
        palette.add_stop(50.0, [255, 255, 255]);

        assert_eq!(palette.range(), Some((10.0, 50.0)));
    }
}
