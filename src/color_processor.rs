//! Color column processing for DataFrames
//!
//! Transforms color factor values into packed RGB colors for rendering.
//! This module handles both continuous (palette interpolation) and categorical
//! (level-based) color mapping.

use crate::{
    categorical_color_from_level, interpolate_color, ColorInfo, ColorMapping, ColorPalette,
};
use polars::prelude::*;
use std::borrow::Cow;

/// Pack RGB values into a u32 (0x00RRGGBB format)
#[inline]
pub fn pack_rgb(r: u8, g: u8, b: u8) -> u32 {
    ((r as u32) << 16) | ((g as u32) << 8) | (b as u32)
}

/// Unpack a u32 into (r, g, b) components
#[inline]
pub fn unpack_rgb(packed: u32) -> (u8, u8, u8) {
    let r = ((packed >> 16) & 0xFF) as u8;
    let g = ((packed >> 8) & 0xFF) as u8;
    let b = (packed & 0xFF) as u8;
    (r, g, b)
}

/// Add packed RGB color column to DataFrame based on color factors
///
/// For continuous mapping: interpolates values using the palette
/// For categorical mapping: maps levels to default palette colors
///
/// # Arguments
/// * `df` - DataFrame with color factor column(s)
/// * `color_infos` - Color configuration (factor name, mapping, quartiles)
///
/// # Returns
/// DataFrame with `.color` column added (packed RGB as i64)
pub fn add_color_columns(
    mut df: polars::frame::DataFrame,
    color_infos: &[ColorInfo],
) -> Result<polars::frame::DataFrame, Box<dyn std::error::Error>> {
    // For now, only use the first color factor
    let color_info = &color_infos[0];

    // Generate RGB values based on mapping type
    let nrows = df.height();
    let mut r_values = Vec::with_capacity(nrows);
    let mut g_values = Vec::with_capacity(nrows);
    let mut b_values = Vec::with_capacity(nrows);

    match &color_info.mapping {
        ColorMapping::Continuous(palette) => {
            add_continuous_colors(
                &df,
                color_info,
                palette,
                &mut r_values,
                &mut g_values,
                &mut b_values,
            )?;
        }

        ColorMapping::Categorical(color_map) => {
            add_categorical_colors(
                &df,
                color_info,
                color_map,
                &mut r_values,
                &mut g_values,
                &mut b_values,
            )?;
        }
    }

    // Pack RGB values directly as u32 (stored as i64 in Polars)
    let packed_colors: Vec<i64> = (0..r_values.len())
        .map(|i| pack_rgb(r_values[i], g_values[i], b_values[i]) as i64)
        .collect();

    // Add color column as packed integers
    df.with_column(Series::new(".color".into(), packed_colors))?;

    // Debug: Print first color values
    if df.height() > 0 {
        if let Ok(color_col) = df.column(".color") {
            let int_col = color_col.i64().unwrap();
            let first_colors: Vec<String> = int_col
                .into_iter()
                .take(3)
                .map(|opt| {
                    opt.map(|v| {
                        let (r, g, b) = unpack_rgb(v as u32);
                        format!("RGB({},{},{})", r, g, b)
                    })
                    .unwrap_or_else(|| "NULL".to_string())
                })
                .collect();
            eprintln!("DEBUG: First 3 .color packed values: {:?}", first_colors);
        }
    }

    Ok(df)
}

/// Add continuous colors using palette interpolation
fn add_continuous_colors(
    polars_df: &polars::frame::DataFrame,
    color_info: &ColorInfo,
    palette: &ColorPalette,
    r_values: &mut Vec<u8>,
    g_values: &mut Vec<u8>,
    b_values: &mut Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let color_col_name = &color_info.factor_name;
    eprintln!(
        "DEBUG add_color_columns: Using continuous color mapping for '{}', is_user_defined={}",
        color_col_name, palette.is_user_defined
    );

    // Rescale palette if is_user_defined=false and quartiles are available
    let effective_palette: Cow<'_, ColorPalette> = if !palette.is_user_defined {
        if let Some(ref quartiles) = color_info.quartiles {
            eprintln!(
                "DEBUG add_color_columns: Rescaling palette using quartiles: {:?}",
                quartiles
            );
            let rescaled = palette.rescale_from_quartiles(quartiles);
            eprintln!(
                "DEBUG add_color_columns: Original range: {:?}, Rescaled range: {:?}",
                palette.range(),
                rescaled.range()
            );
            Cow::Owned(rescaled)
        } else {
            eprintln!(
                "WARN add_color_columns: is_user_defined=false but no quartiles available, using original palette"
            );
            Cow::Borrowed(palette)
        }
    } else {
        Cow::Borrowed(palette)
    };

    // Get the color factor column
    let color_series = polars_df
        .column(color_col_name)
        .map_err(|e| format!("Color column '{}' not found: {}", color_col_name, e))?;

    // Extract f64 values
    let color_values = color_series.f64().map_err(|e| {
        format!(
            "Color column '{}' is not f64 for continuous mapping: {}",
            color_col_name, e
        )
    })?;

    // Debug: Print first few color factor values to verify we're getting expected data
    let sample_values: Vec<f64> = color_values.iter().take(5).flatten().collect();
    if !sample_values.is_empty() {
        let min_val = color_values.min().unwrap_or(0.0);
        let max_val = color_values.max().unwrap_or(0.0);
        eprintln!(
            "DEBUG add_color_columns: {} values range [{:.2}, {:.2}], first 5: {:?}",
            color_col_name, min_val, max_val, sample_values
        );
    }

    // Map each value to RGB using palette interpolation
    for opt_value in color_values.iter() {
        if let Some(value) = opt_value {
            let rgb = interpolate_color(value, &effective_palette);
            r_values.push(rgb[0]);
            g_values.push(rgb[1]);
            b_values.push(rgb[2]);
        } else {
            // Handle null values with a default color (gray)
            r_values.push(128);
            g_values.push(128);
            b_values.push(128);
        }
    }

    Ok(())
}

/// Add categorical colors using level mapping or explicit category mappings
fn add_categorical_colors(
    polars_df: &polars::frame::DataFrame,
    color_info: &ColorInfo,
    color_map: &crate::CategoryColorMap,
    r_values: &mut Vec<u8>,
    g_values: &mut Vec<u8>,
    b_values: &mut Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("DEBUG add_color_columns: Using categorical color mapping");
    eprintln!(
        "DEBUG add_color_columns: Category map has {} entries",
        color_map.mappings.len()
    );

    // For categorical colors, Tercen uses .colorLevels column (int32) with level indices
    // If color_map has explicit mappings, use them; otherwise generate from levels
    let use_levels = color_map.mappings.is_empty();

    if use_levels {
        add_categorical_colors_from_levels(polars_df, r_values, g_values, b_values)?;
    } else {
        add_categorical_colors_from_mappings(
            polars_df, color_info, color_map, r_values, g_values, b_values,
        )?;
    }

    Ok(())
}

/// Map .colorLevels column to colors using default categorical palette
fn add_categorical_colors_from_levels(
    polars_df: &polars::frame::DataFrame,
    r_values: &mut Vec<u8>,
    g_values: &mut Vec<u8>,
    b_values: &mut Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("DEBUG add_color_columns: Using .colorLevels column for categorical colors");

    // Get .colorLevels column instead of the factor column
    let levels_series = polars_df
        .column(".colorLevels")
        .map_err(|e| format!("Categorical colors require .colorLevels column: {}", e))?;

    // Schema says int32 but it comes back as i64, so accept both
    let levels = levels_series
        .i64()
        .map_err(|e| format!(".colorLevels column is not i64: {}", e))?;

    // Map each level to RGB using default categorical palette
    for opt_level in levels.iter() {
        if let Some(level) = opt_level {
            let rgb = categorical_color_from_level(level as i32);
            r_values.push(rgb[0]);
            g_values.push(rgb[1]);
            b_values.push(rgb[2]);
        } else {
            // Handle null values with a default color (gray)
            r_values.push(128);
            g_values.push(128);
            b_values.push(128);
        }
    }

    Ok(())
}

/// Map categorical values using explicit category→color mappings
fn add_categorical_colors_from_mappings(
    polars_df: &polars::frame::DataFrame,
    color_info: &ColorInfo,
    color_map: &crate::CategoryColorMap,
    r_values: &mut Vec<u8>,
    g_values: &mut Vec<u8>,
    b_values: &mut Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let color_col_name = &color_info.factor_name;
    eprintln!(
        "DEBUG add_color_columns: Using explicit category mappings for '{}'",
        color_col_name
    );

    // Get the color factor column
    let color_series = polars_df
        .column(color_col_name)
        .map_err(|e| format!("Color column '{}' not found: {}", color_col_name, e))?;

    let color_values = color_series.str().map_err(|e| {
        format!(
            "Color column '{}' is not string for categorical mapping: {}",
            color_col_name, e
        )
    })?;

    for opt_value in color_values.iter() {
        if let Some(category) = opt_value {
            let rgb = color_map
                .mappings
                .get(category)
                .unwrap_or(&color_map.default_color);
            r_values.push(rgb[0]);
            g_values.push(rgb[1]);
            b_values.push(rgb[2]);
        } else {
            r_values.push(128);
            g_values.push(128);
            b_values.push(128);
        }
    }

    Ok(())
}

/// Add mixed-layer colors using the unified LayerColorConfig
///
/// Each layer has a LayerColorConfig that determines how its points are colored:
/// - Continuous: interpolate values using the layer's palette
/// - Categorical: map levels to colors
/// - Constant: all points get the pre-computed constant color
///
/// # Arguments
/// * `df` - DataFrame with `.axisIndex` column and color factor columns
/// * `per_layer_config` - Per-layer color configuration (every layer has a config)
///
/// # Returns
/// DataFrame with `.color` column added (packed RGB as i64)
pub fn add_mixed_layer_colors(
    mut df: polars::frame::DataFrame,
    per_layer_config: &crate::PerLayerColorConfig,
) -> Result<polars::frame::DataFrame, Box<dyn std::error::Error>> {
    use crate::LayerColorConfig;
    use std::borrow::Cow;

    let nrows = df.height();

    eprintln!(
        "DEBUG add_mixed_layer_colors: Processing {} rows with {} layers",
        nrows, per_layer_config.n_layers
    );

    // Get .axisIndex column (optional for single-layer case)
    // When there's only 1 layer, all rows belong to layer 0 by definition
    let axis_indices_opt = df
        .column(".axisIndex")
        .ok()
        .and_then(|col| col.i64().ok());

    // For logging
    if axis_indices_opt.is_none() && per_layer_config.n_layers == 1 {
        eprintln!("DEBUG add_mixed_layer_colors: Single layer, no .axisIndex column - all rows belong to layer 0");
    }

    // Pre-extract and rescale palettes for continuous layers
    let mut continuous_data: std::collections::HashMap<
        usize,
        (Cow<'_, crate::ColorPalette>, Vec<f64>),
    > = std::collections::HashMap::new();

    for (layer_idx, config) in per_layer_config.layer_configs.iter().enumerate() {
        if let LayerColorConfig::Continuous {
            palette,
            factor_name,
            quartiles,
            ..
        } = config
        {
            // Get the factor column for this layer
            if let Ok(col) = df.column(factor_name) {
                if let Ok(f64_col) = col.f64() {
                    let values: Vec<f64> = f64_col.iter().map(|v| v.unwrap_or(0.0)).collect();

                    // Rescale palette if quartiles available
                    let effective_palette: Cow<'_, crate::ColorPalette> = if !palette
                        .is_user_defined
                    {
                        if let Some(q) = quartiles {
                            eprintln!(
                                    "DEBUG add_mixed_layer_colors: Rescaling palette for layer {} using quartiles",
                                    layer_idx
                                );
                            Cow::Owned(palette.rescale_from_quartiles(q))
                        } else {
                            Cow::Borrowed(palette)
                        }
                    } else {
                        Cow::Borrowed(palette)
                    };

                    eprintln!(
                        "DEBUG add_mixed_layer_colors: Layer {} uses continuous factor '{}' ({} values)",
                        layer_idx, factor_name, values.len()
                    );
                    continuous_data.insert(layer_idx, (effective_palette, values));
                }
            }
        }
    }

    // Check if we need .colorLevels for any categorical layers
    let needs_color_levels = per_layer_config.has_categorical();

    let color_levels_column: Option<Vec<i64>> = if needs_color_levels {
        df.column(".colorLevels")
            .ok()
            .and_then(|col| col.i64().ok())
            .map(|i64_col| i64_col.iter().map(|v| v.unwrap_or(0)).collect())
    } else {
        None
    };

    // Build packed color values row by row
    let mut packed_colors: Vec<i64> = Vec::with_capacity(nrows);

    for row_idx in 0..nrows {
        // Get layer index: from .axisIndex if available, otherwise 0 (single layer)
        let layer_idx = axis_indices_opt
            .as_ref()
            .map(|indices| indices.get(row_idx).unwrap_or(0) as usize)
            .unwrap_or(0);

        let rgb = match per_layer_config.get(layer_idx) {
            Some(LayerColorConfig::Continuous { .. }) => {
                // Use pre-extracted continuous data
                if let Some((palette, values)) = continuous_data.get(&layer_idx) {
                    let value = values.get(row_idx).copied().unwrap_or(0.0);
                    interpolate_color(value, palette)
                } else {
                    [128, 128, 128] // Fallback gray
                }
            }
            Some(LayerColorConfig::Categorical { .. }) => {
                // Use .colorLevels
                if let Some(ref levels) = color_levels_column {
                    let level = levels.get(row_idx).copied().unwrap_or(0) as i32;
                    categorical_color_from_level(level)
                } else {
                    [128, 128, 128] // Fallback gray
                }
            }
            Some(LayerColorConfig::Constant { color }) => {
                // Use pre-computed constant color
                *color
            }
            None => {
                // No config for this layer (shouldn't happen) - use gray
                [128, 128, 128]
            }
        };

        packed_colors.push(pack_rgb(rgb[0], rgb[1], rgb[2]) as i64);
    }

    // Debug: Show color distribution by layer
    let mut layer_color_counts: std::collections::HashMap<usize, usize> =
        std::collections::HashMap::new();
    if let Some(indices) = &axis_indices_opt {
        for opt_idx in indices.iter().flatten() {
            *layer_color_counts.entry(opt_idx as usize).or_insert(0) += 1;
        }
    } else {
        // Single layer case - all rows belong to layer 0
        layer_color_counts.insert(0, nrows);
    }
    for (layer_idx, count) in layer_color_counts.iter() {
        let config_type = per_layer_config
            .get(*layer_idx)
            .map(|c| match c {
                LayerColorConfig::Continuous { .. } => "continuous",
                LayerColorConfig::Categorical { .. } => "categorical",
                LayerColorConfig::Constant { .. } => "constant",
            })
            .unwrap_or("none");
        eprintln!(
            "DEBUG add_mixed_layer_colors: Layer {} has {} points, config={}",
            layer_idx, count, config_type
        );
    }

    df.with_column(Series::new(".color".into(), packed_colors))?;

    Ok(df)
}

/// Add layer-based colors when no color factor is specified
///
/// When multiple layers exist (axis_queries > 1) and no colors are explicitly mapped,
/// this function colors points by their layer index using the specified palette.
///
/// # Arguments
/// * `df` - DataFrame with `.axisIndex` column
/// * `palette_name` - Optional palette name (defaults to Palette-1 if None)
///
/// # Returns
/// DataFrame with `.color` column added (packed RGB as i64)
pub fn add_layer_colors(
    mut df: polars::frame::DataFrame,
    palette_name: Option<&str>,
) -> Result<polars::frame::DataFrame, Box<dyn std::error::Error>> {
    use crate::palettes::{DEFAULT_CATEGORICAL_PALETTE, PALETTE_REGISTRY};

    let nrows = df.height();

    // Get .axisIndex column
    let axis_index_series = df
        .column(".axisIndex")
        .map_err(|e| format!(".axisIndex column not found: {}", e))?;

    let axis_indices = axis_index_series
        .i64()
        .map_err(|e| format!(".axisIndex column is not i64: {}", e))?;

    // Use specified palette or fallback to default categorical palette
    let effective_palette_name = palette_name.unwrap_or(DEFAULT_CATEGORICAL_PALETTE);
    let palette = PALETTE_REGISTRY
        .get(effective_palette_name)
        .ok_or_else(|| format!("Palette '{}' not found", effective_palette_name))?;

    eprintln!(
        "DEBUG add_layer_colors: Coloring {} points by layer using '{}' palette ({} colors)",
        nrows,
        effective_palette_name,
        palette.len()
    );

    // Map each axis index to a color from Palette-1
    let packed_colors: Vec<i64> = axis_indices
        .iter()
        .map(|opt_idx| {
            let idx = opt_idx.unwrap_or(0) as usize;
            let rgb = palette.get_color(idx);
            pack_rgb(rgb[0], rgb[1], rgb[2]) as i64
        })
        .collect();

    // Debug: Show which colors are used for which layers
    let mut seen_layers: std::collections::HashSet<i64> = std::collections::HashSet::new();
    for idx in axis_indices.iter().flatten() {
        if seen_layers.insert(idx) {
            let rgb = palette.get_color(idx as usize);
            eprintln!(
                "DEBUG add_layer_colors: Layer {} -> RGB({},{},{})",
                idx, rgb[0], rgb[1], rgb[2]
            );
        }
    }

    df.with_column(Series::new(".color".into(), packed_colors))?;

    Ok(df)
}
