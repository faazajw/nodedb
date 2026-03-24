//! Collection type enum shared between Origin and Lite.
//!
//! Determines routing, storage format, and query execution strategy.

use serde::{Deserialize, Serialize};

/// The type of a collection, determining its storage engine and query behavior.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CollectionType {
    /// Schemaless or typed JSON document store. Default.
    #[default]
    Document,
    /// Columnar timeseries with time-partitioning, Gorilla compression,
    /// adaptive intervals, and ILP ingest support.
    Timeseries,
}

impl CollectionType {
    pub fn is_timeseries(&self) -> bool {
        matches!(self, Self::Timeseries)
    }

    pub fn is_document(&self) -> bool {
        matches!(self, Self::Document)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Document => "document",
            Self::Timeseries => "timeseries",
        }
    }
}

impl std::fmt::Display for CollectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for CollectionType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "document" | "doc" => Ok(Self::Document),
            "timeseries" | "ts" => Ok(Self::Timeseries),
            other => Err(format!("unknown collection type: '{other}'")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_document() {
        assert_eq!(CollectionType::default(), CollectionType::Document);
    }

    #[test]
    fn serde_roundtrip() {
        let ts = CollectionType::Timeseries;
        let json = serde_json::to_string(&ts).unwrap();
        assert_eq!(json, r#""timeseries""#);
        let back: CollectionType = serde_json::from_str(&json).unwrap();
        assert_eq!(back, ts);
    }

    #[test]
    fn from_str() {
        assert_eq!(
            "document".parse::<CollectionType>().unwrap(),
            CollectionType::Document
        );
        assert_eq!(
            "timeseries".parse::<CollectionType>().unwrap(),
            CollectionType::Timeseries
        );
        assert_eq!(
            "ts".parse::<CollectionType>().unwrap(),
            CollectionType::Timeseries
        );
        assert!("unknown".parse::<CollectionType>().is_err());
    }

    #[test]
    fn display() {
        assert_eq!(CollectionType::Document.to_string(), "document");
        assert_eq!(CollectionType::Timeseries.to_string(), "timeseries");
    }

    #[test]
    fn backward_compat_string_deser() {
        // Old data stored as "document" string should deserialize.
        let val: CollectionType = serde_json::from_str(r#""document""#).unwrap();
        assert_eq!(val, CollectionType::Document);
    }
}
