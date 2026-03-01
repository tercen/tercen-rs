//! Operator property reading and parsing
//!
//! Reads operator properties from OperatorSettings proto and provides
//! type-safe conversions with explicit defaults.

use crate::client::proto::{OperatorSettings, PropertyValue};

/// Reads operator properties from Tercen with type-safe conversions
pub struct PropertyReader {
    properties: Vec<PropertyValue>,
}

impl PropertyReader {
    /// Create from OperatorSettings (may be None if no properties set)
    pub fn from_operator_settings(settings: Option<&OperatorSettings>) -> Self {
        let properties = settings
            .and_then(|s| s.operator_ref.as_ref())
            .map(|op_ref| op_ref.property_values.clone())
            .unwrap_or_default();

        eprintln!(
            "DEBUG PropertyReader: Found {} properties",
            properties.len()
        );
        for prop in &properties {
            eprintln!("  DEBUG: '{}' = '{}'", prop.name, prop.value);
        }

        Self { properties }
    }

    /// Get raw property value (None if not set or empty)
    fn get_raw(&self, name: &str) -> Option<&str> {
        self.properties
            .iter()
            .find(|p| p.name == name)
            .and_then(|p| {
                if p.value.is_empty() {
                    None // Empty string = not set (Tercen convention)
                } else {
                    Some(p.value.as_str())
                }
            })
    }

    /// Get string property with explicit default
    pub fn get_string(&self, name: &str, default: &str) -> String {
        let value = self.get_raw(name).unwrap_or(default);
        eprintln!(
            "DEBUG PropertyReader::get_string('{}') -> '{}' (default: '{}')",
            name, value, default
        );
        value.to_string()
    }

    /// Get i32 property with validation and explicit default
    pub fn get_i32(&self, name: &str, default: i32) -> i32 {
        self.get_raw(name)
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or_else(|| {
                if let Some(raw) = self.get_raw(name) {
                    eprintln!(
                        "⚠ Invalid integer value for '{}': '{}', using default: {}",
                        name, raw, default
                    );
                }
                default
            })
    }

    /// Get boolean property (handles "true"/"false" strings) with explicit default
    pub fn get_bool(&self, name: &str, default: bool) -> bool {
        match self.get_raw(name) {
            Some("true") => true,
            Some("false") => false,
            Some(other) => {
                eprintln!(
                    "⚠ Invalid boolean value for '{}': '{}', using default: {}",
                    name, other, default
                );
                default
            }
            None => default,
        }
    }
}

/// Plot dimension - either explicit pixels or "auto" (derived from crosstab)
#[derive(Debug, Clone, PartialEq, Default)]
pub enum PlotDimension {
    #[default]
    Auto,
    Pixels(i32),
}

impl PlotDimension {
    /// Parse from string property value
    ///
    /// Valid formats:
    /// - "auto" or "" (empty) → Auto
    /// - "1500" → Pixels(1500) if in valid range [100, 128000]
    pub fn from_str(value: &str, default: PlotDimension) -> Self {
        let trimmed = value.trim();

        // Empty or "auto" → Auto
        if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("auto") {
            return PlotDimension::Auto;
        }

        // Parse as integer
        match trimmed.parse::<i32>() {
            Ok(px) if (100..=128000).contains(&px) => PlotDimension::Pixels(px),
            Ok(px) => {
                eprintln!(
                    "⚠ Plot dimension {} out of valid range [100-128000], using default: {:?}",
                    px, default
                );
                default
            }
            Err(_) => {
                eprintln!(
                    "⚠ Invalid plot dimension '{}', using default: {:?}",
                    trimmed, default
                );
                default
            }
        }
    }

    /// Resolve to actual pixels
    ///
    /// For Auto: derives from grid dimension using formula:
    /// - base_size (800px) + (n - 1) * size_per_unit (400px)
    /// - No upper limit (grows with grid size)
    ///
    /// Examples:
    /// - 1 unit → 800px
    /// - 2 units → 1200px
    /// - 3 units → 1600px
    /// - 10 units → 4400px
    /// - 50 units → 20400px
    pub fn resolve(&self, n_units: usize) -> i32 {
        match self {
            PlotDimension::Pixels(px) => *px,
            PlotDimension::Auto => {
                const BASE_SIZE: i32 = 800;
                const SIZE_PER_UNIT: i32 = 400;

                BASE_SIZE + (n_units.saturating_sub(1) as i32 * SIZE_PER_UNIT)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_property_reader_empty() {
        let reader = PropertyReader::from_operator_settings(None);
        assert_eq!(reader.get_string("foo", "default"), "default");
        assert_eq!(reader.get_i32("bar", 42), 42);
        assert!(reader.get_bool("baz", true));
    }

    #[test]
    fn test_plot_dimension_auto() {
        let dim = PlotDimension::from_str("auto", PlotDimension::Auto);
        assert_eq!(dim, PlotDimension::Auto);
        assert_eq!(dim.resolve(1), 800); // 1 facet
        assert_eq!(dim.resolve(2), 1200); // 2 facets
        assert_eq!(dim.resolve(3), 1600); // 3 facets
        assert_eq!(dim.resolve(4), 2000); // 4 facets
        assert_eq!(dim.resolve(10), 4400); // 10 facets (no cap)
    }

    #[test]
    fn test_plot_dimension_empty_string() {
        let dim = PlotDimension::from_str("", PlotDimension::Auto);
        assert_eq!(dim, PlotDimension::Auto);
    }

    #[test]
    fn test_plot_dimension_pixels() {
        let dim = PlotDimension::from_str("1500", PlotDimension::Auto);
        assert_eq!(dim, PlotDimension::Pixels(1500));
        assert_eq!(dim.resolve(10), 1500); // Ignores facet count
    }

    #[test]
    fn test_plot_dimension_invalid() {
        let dim = PlotDimension::from_str("abc", PlotDimension::Auto);
        assert_eq!(dim, PlotDimension::Auto); // Falls back to default
    }

    #[test]
    fn test_plot_dimension_out_of_range() {
        // Too small
        let dim = PlotDimension::from_str("50", PlotDimension::Auto);
        assert_eq!(dim, PlotDimension::Auto);

        // Too large
        let dim = PlotDimension::from_str("200000", PlotDimension::Auto);
        assert_eq!(dim, PlotDimension::Auto);
    }

    #[test]
    fn test_plot_dimension_edge_cases() {
        // Minimum valid
        let dim = PlotDimension::from_str("100", PlotDimension::Auto);
        assert_eq!(dim, PlotDimension::Pixels(100));

        // Maximum valid
        let dim = PlotDimension::from_str("128000", PlotDimension::Auto);
        assert_eq!(dim, PlotDimension::Pixels(128000));
    }
}
