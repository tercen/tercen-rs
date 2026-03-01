//! Palette registry for loading and accessing color palettes
//!
//! Loads palettes from palettes.json (embedded at compile time) and provides
//! access by name. Matches the R plot_operator's palette definitions for
//! consistency with Tercen's ecosystem.
//!
//! Palette types:
//! - `categorical`: Discrete colors for distinct categories (colors repeat after exhausting the list)
//! - `sequential`: Gradient from low to high values
//! - `diverging`: Gradient with a neutral midpoint (e.g., for +/- deviations)

use once_cell::sync::Lazy;
use serde::Deserialize;
use std::collections::HashMap;

/// Embedded palettes.json content (from R plot_operator)
const PALETTES_JSON: &str = include_str!("../palettes.json");

/// Global palette registry, initialized lazily on first access
pub static PALETTE_REGISTRY: Lazy<PaletteRegistry> = Lazy::new(|| {
    PaletteRegistry::from_json(PALETTES_JSON).unwrap_or_else(|e| {
        eprintln!("ERROR: Failed to load palettes.json: {}", e);
        PaletteRegistry::default()
    })
});

/// Default categorical palette name (Tercen's default)
pub const DEFAULT_CATEGORICAL_PALETTE: &str = "Palette-1";

/// Default sequential palette name
pub const DEFAULT_SEQUENTIAL_PALETTE: &str = "Viridis";

/// Default diverging palette name
pub const DEFAULT_DIVERGING_PALETTE: &str = "RdBu";

/// Palette type as defined in palettes.json
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PaletteType {
    Categorical,
    Sequential,
    Diverging,
}

/// A single palette definition from palettes.json
#[derive(Debug, Clone, Deserialize)]
pub struct PaletteDefinition {
    pub name: String,
    #[serde(rename = "type")]
    pub palette_type: PaletteType,
    pub colors: Vec<String>,
}

impl PaletteDefinition {
    /// Get a color by index (wraps around for categorical palettes)
    pub fn get_color(&self, index: usize) -> [u8; 3] {
        if self.colors.is_empty() {
            return [128, 128, 128]; // Gray fallback
        }
        let idx = index % self.colors.len();
        parse_hex_color(&self.colors[idx]).unwrap_or([128, 128, 128])
    }

    /// Get all colors as RGB arrays
    pub fn get_colors_rgb(&self) -> Vec<[u8; 3]> {
        self.colors
            .iter()
            .filter_map(|hex| parse_hex_color(hex))
            .collect()
    }

    /// Get the number of colors in this palette
    pub fn len(&self) -> usize {
        self.colors.len()
    }

    /// Check if the palette is empty
    pub fn is_empty(&self) -> bool {
        self.colors.is_empty()
    }

    /// Interpolate a color from the palette at position t ∈ [0, 1]
    ///
    /// t=0 returns the first color, t=1 returns the last color.
    /// Values in between are linearly interpolated.
    pub fn interpolate(&self, t: f64) -> [u8; 3] {
        if self.colors.is_empty() {
            return [128, 128, 128]; // Gray fallback
        }

        let t = t.clamp(0.0, 1.0);
        let n = self.colors.len();

        if n == 1 {
            return self.get_color(0);
        }

        // Map t to position in the color array
        let pos = t * (n - 1) as f64;
        let idx_low = pos.floor() as usize;
        let idx_high = (idx_low + 1).min(n - 1);
        let frac = pos - idx_low as f64;

        let color_low = self.get_color(idx_low);
        let color_high = self.get_color(idx_high);

        // Linear interpolation between the two colors
        [
            (color_low[0] as f64 * (1.0 - frac) + color_high[0] as f64 * frac) as u8,
            (color_low[1] as f64 * (1.0 - frac) + color_high[1] as f64 * frac) as u8,
            (color_low[2] as f64 * (1.0 - frac) + color_high[2] as f64 * frac) as u8,
        ]
    }
}

/// Registry of all available palettes
#[derive(Debug, Clone, Default)]
pub struct PaletteRegistry {
    /// All palettes by name (lowercase keys for case-insensitive lookup)
    palettes: HashMap<String, PaletteDefinition>,
    /// Categorical palette names (for listing)
    categorical_names: Vec<String>,
    /// Sequential palette names (for listing)
    sequential_names: Vec<String>,
    /// Diverging palette names (for listing)
    diverging_names: Vec<String>,
}

impl PaletteRegistry {
    /// Load palettes from JSON string
    pub fn from_json(json: &str) -> Result<Self, String> {
        let definitions: Vec<PaletteDefinition> = serde_json::from_str(json)
            .map_err(|e| format!("Failed to parse palettes JSON: {}", e))?;

        let mut registry = Self::default();

        for def in definitions {
            let name = def.name.clone();
            match def.palette_type {
                PaletteType::Categorical => registry.categorical_names.push(name.clone()),
                PaletteType::Sequential => registry.sequential_names.push(name.clone()),
                PaletteType::Diverging => registry.diverging_names.push(name.clone()),
            }
            // Store with lowercase key for case-insensitive lookup
            registry.palettes.insert(name.to_lowercase(), def);
        }

        eprintln!(
            "DEBUG PaletteRegistry: Loaded {} palettes ({} categorical, {} sequential, {} diverging)",
            registry.palettes.len(),
            registry.categorical_names.len(),
            registry.sequential_names.len(),
            registry.diverging_names.len()
        );

        Ok(registry)
    }

    /// Get a palette by name (case-insensitive)
    pub fn get(&self, name: &str) -> Option<&PaletteDefinition> {
        self.palettes.get(&name.to_lowercase())
    }

    /// Get the default categorical palette
    pub fn default_categorical(&self) -> Option<&PaletteDefinition> {
        self.get(DEFAULT_CATEGORICAL_PALETTE)
    }

    /// Get the default sequential palette
    pub fn default_sequential(&self) -> Option<&PaletteDefinition> {
        self.get(DEFAULT_SEQUENTIAL_PALETTE)
    }

    /// Get the default diverging palette
    pub fn default_diverging(&self) -> Option<&PaletteDefinition> {
        self.get(DEFAULT_DIVERGING_PALETTE)
    }

    /// List all categorical palette names
    pub fn categorical_palettes(&self) -> &[String] {
        &self.categorical_names
    }

    /// List all sequential palette names
    pub fn sequential_palettes(&self) -> &[String] {
        &self.sequential_names
    }

    /// List all diverging palette names
    pub fn diverging_palettes(&self) -> &[String] {
        &self.diverging_names
    }
}

/// Parse a hex color string to RGB array
///
/// Supports formats:
/// - `#RRGGBB` (6 hex digits)
/// - `#RRGGBBAA` (8 hex digits, alpha ignored)
/// - `RRGGBB` (without #)
/// - `RRGGBBAA` (without #)
fn parse_hex_color(hex: &str) -> Option<[u8; 3]> {
    let hex = hex.trim_start_matches('#');

    // Handle 6-digit (RGB) or 8-digit (RGBA) hex
    if hex.len() != 6 && hex.len() != 8 {
        eprintln!("WARN: Invalid hex color length '{}': {}", hex, hex.len());
        return None;
    }

    let r = u8::from_str_radix(&hex[0..2], 16).ok()?;
    let g = u8::from_str_radix(&hex[2..4], 16).ok()?;
    let b = u8::from_str_radix(&hex[4..6], 16).ok()?;

    Some([r, g, b])
}

/// Get a categorical color from the default palette by level index
///
/// This is the main entry point for categorical coloring. Uses "Palette-1"
/// (Tercen's default) and wraps around if the index exceeds the palette size.
pub fn categorical_color_from_level(level: i32) -> [u8; 3] {
    let palette = PALETTE_REGISTRY
        .default_categorical()
        .expect("Default categorical palette 'Palette-1' not found");

    palette.get_color(level as usize)
}

/// Get a categorical color from a named palette by level index
///
/// Falls back to the default palette if the named palette is not found.
pub fn categorical_color_from_palette(palette_name: &str, level: i32) -> [u8; 3] {
    let palette = PALETTE_REGISTRY
        .get(palette_name)
        .or_else(|| PALETTE_REGISTRY.default_categorical())
        .expect("No categorical palette available");

    palette.get_color(level as usize)
}

/// Get all colors from a palette as a Vec of RGB arrays
///
/// Falls back to the default categorical palette if the named palette is not found.
pub fn get_palette_colors(palette_name: &str) -> Vec<[u8; 3]> {
    let palette = PALETTE_REGISTRY
        .get(palette_name)
        .or_else(|| PALETTE_REGISTRY.default_categorical())
        .expect("No palette available");

    palette.get_colors_rgb()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_hex_color() {
        // 6-digit hex
        assert_eq!(parse_hex_color("#FF0000"), Some([255, 0, 0]));
        assert_eq!(parse_hex_color("#00FF00"), Some([0, 255, 0]));
        assert_eq!(parse_hex_color("#0000FF"), Some([0, 0, 255]));
        assert_eq!(parse_hex_color("#1F78B4"), Some([31, 120, 180]));

        // Without #
        assert_eq!(parse_hex_color("FF0000"), Some([255, 0, 0]));

        // 8-digit hex (with alpha, ignored)
        assert_eq!(parse_hex_color("#440154FF"), Some([68, 1, 84]));
        assert_eq!(parse_hex_color("440154FF"), Some([68, 1, 84]));

        // Invalid
        assert_eq!(parse_hex_color("#FFF"), None); // Too short
        assert_eq!(parse_hex_color("GGGGGG"), None); // Invalid hex
    }

    #[test]
    fn test_palette_registry_loads() {
        // Should load without error
        let registry = &*PALETTE_REGISTRY;

        // Should have palettes
        assert!(!registry.palettes.is_empty());

        // Should have Palette-1
        let palette1 = registry.get("Palette-1");
        assert!(palette1.is_some());
        let palette1 = palette1.unwrap();
        assert_eq!(palette1.palette_type, PaletteType::Categorical);
        assert!(!palette1.colors.is_empty());

        // First color of Palette-1 should be #1F78B4 (blue)
        assert_eq!(palette1.get_color(0), [31, 120, 180]);
    }

    #[test]
    fn test_categorical_color_from_level() {
        // First few colors from Palette-1
        assert_eq!(categorical_color_from_level(0), [31, 120, 180]); // #1F78B4
        assert_eq!(categorical_color_from_level(1), [227, 26, 28]); // #E31A1C
        assert_eq!(categorical_color_from_level(2), [51, 160, 44]); // #33A02C
    }

    #[test]
    fn test_palette_color_wrapping() {
        let palette = PALETTE_REGISTRY.get("Palette-1").unwrap();
        let len = palette.len();

        // Color at index 0 should equal color at index len
        assert_eq!(palette.get_color(0), palette.get_color(len));
        assert_eq!(palette.get_color(1), palette.get_color(len + 1));
    }

    #[test]
    fn test_palette_types() {
        let registry = &*PALETTE_REGISTRY;

        // Check categorical palettes
        assert!(registry
            .categorical_palettes()
            .contains(&"Palette-1".to_string()));
        assert!(registry
            .categorical_palettes()
            .contains(&"Set1".to_string()));

        // Check sequential palettes
        assert!(registry
            .sequential_palettes()
            .contains(&"Viridis".to_string()));
        assert!(registry.sequential_palettes().contains(&"Jet".to_string()));

        // Check diverging palettes
        assert!(registry.diverging_palettes().contains(&"RdBu".to_string()));
        assert!(registry.diverging_palettes().contains(&"PiYG".to_string()));
    }
}
