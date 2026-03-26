//! GeoParquet and GeoArrow metadata for geometry columns.
//!
//! GeoParquet: JSON metadata in Parquet file key-value metadata that tells
//! external tools (DuckDB, QGIS, GeoPandas) which columns contain geometry
//! and what encoding/CRS is used.
//!
//! GeoArrow: Arrow extension type metadata on Binary columns so Arrow-native
//! tools can recognize NodeDB's spatial columns.
//!
//! References:
//! - GeoParquet spec: https://geoparquet.org/releases/v1.1.0/
//! - GeoArrow spec: https://geoarrow.org/extension-types

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// GeoParquet metadata for a Parquet file containing geometry columns.
///
/// Stored as JSON in the Parquet file's key-value metadata under the key "geo".
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoParquetMetadata {
    /// GeoParquet spec version.
    pub version: String,
    /// Primary geometry column name.
    pub primary_column: String,
    /// Per-column geometry metadata.
    pub columns: HashMap<String, GeoParquetColumnMeta>,
}

/// Metadata for a single geometry column in GeoParquet.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoParquetColumnMeta {
    /// Encoding: "WKB" (default).
    pub encoding: String,
    /// Geometry types present in this column.
    pub geometry_types: Vec<String>,
    /// Coordinate Reference System. "EPSG:4326" for WGS-84.
    pub crs: serde_json::Value,
    /// Bounding box of all geometries: [min_lng, min_lat, max_lng, max_lat].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bbox: Option<[f64; 4]>,
}

impl GeoParquetMetadata {
    /// Create metadata for a single geometry column.
    pub fn single_column(
        column_name: &str,
        geometry_types: Vec<String>,
        bbox: Option<[f64; 4]>,
    ) -> Self {
        let mut columns = HashMap::new();
        columns.insert(
            column_name.to_string(),
            GeoParquetColumnMeta {
                encoding: "WKB".to_string(),
                geometry_types,
                crs: serde_json::json!({
                    "type": "GeographicCRS",
                    "name": "WGS 84",
                    "id": { "authority": "EPSG", "code": 4326 }
                }),
                bbox,
            },
        );
        Self {
            version: "1.1.0".to_string(),
            primary_column: column_name.to_string(),
            columns,
        }
    }

    /// Serialize to JSON string for Parquet file metadata.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// The Parquet metadata key for GeoParquet.
    pub const PARQUET_KEY: &'static str = "geo";
}

/// GeoArrow extension type name for WKB-encoded geometry columns.
///
/// Register this on Arrow `DataType::Binary` columns so Arrow-native tools
/// (DuckDB, GeoPolars) recognize them as geometry.
pub const GEOARROW_EXTENSION_NAME: &str = "geoarrow.wkb";

/// GeoArrow extension metadata (JSON).
///
/// Stored in Arrow schema's field metadata under the key
/// `ARROW:extension:metadata`.
pub fn geoarrow_extension_metadata(crs_epsg: u32) -> String {
    serde_json::json!({
        "crs": {
            "type": "GeographicCRS",
            "name": "WGS 84",
            "id": { "authority": "EPSG", "code": crs_epsg }
        }
    })
    .to_string()
}

/// Arrow field metadata keys for extension types.
pub const ARROW_EXTENSION_NAME_KEY: &str = "ARROW:extension:name";
pub const ARROW_EXTENSION_METADATA_KEY: &str = "ARROW:extension:metadata";

/// Build Arrow field metadata for a WKB geometry column.
///
/// Returns a HashMap to set on the Arrow Field's metadata.
pub fn geoarrow_field_metadata() -> HashMap<String, String> {
    let mut meta = HashMap::new();
    meta.insert(
        ARROW_EXTENSION_NAME_KEY.to_string(),
        GEOARROW_EXTENSION_NAME.to_string(),
    );
    meta.insert(
        ARROW_EXTENSION_METADATA_KEY.to_string(),
        geoarrow_extension_metadata(4326),
    );
    meta
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geoparquet_metadata_json() {
        let meta = GeoParquetMetadata::single_column(
            "geom",
            vec!["Point".to_string(), "Polygon".to_string()],
            Some([-180.0, -90.0, 180.0, 90.0]),
        );
        let json = meta.to_json().unwrap();
        assert!(json.contains("\"version\":\"1.1.0\""));
        assert!(json.contains("\"primary_column\":\"geom\""));
        assert!(json.contains("\"encoding\":\"WKB\""));
        assert!(json.contains("EPSG"));
    }

    #[test]
    fn geoparquet_key() {
        assert_eq!(GeoParquetMetadata::PARQUET_KEY, "geo");
    }

    #[test]
    fn geoarrow_field_meta() {
        let meta = geoarrow_field_metadata();
        assert_eq!(meta[ARROW_EXTENSION_NAME_KEY], "geoarrow.wkb");
        assert!(meta[ARROW_EXTENSION_METADATA_KEY].contains("EPSG"));
    }

    #[test]
    fn geoarrow_extension_name() {
        assert_eq!(GEOARROW_EXTENSION_NAME, "geoarrow.wkb");
    }

    #[test]
    fn roundtrip_parquet_metadata() {
        let meta = GeoParquetMetadata::single_column("location", vec!["Point".into()], None);
        let json = meta.to_json().unwrap();
        let parsed: GeoParquetMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.primary_column, "location");
        assert_eq!(parsed.columns["location"].encoding, "WKB");
    }
}
