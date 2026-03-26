//! H3 hexagonal hierarchical spatial index.
//!
//! Uber's H3 system maps the globe into hexagonal cells at 16 resolutions
//! (0 = ~4.3M km² to 15 = ~0.9 m²). Advantages over geohash:
//! - Uniform cell area (no pole distortion)
//! - Hexagonal tessellation (6 equidistant neighbors vs 8 unequal for geohash)
//! - Better for analytics: equal-area binning for heatmaps
//!
//! Uses the `h3o` crate (pure Rust H3 implementation).

use h3o::{CellIndex, LatLng, Resolution};
use nodedb_types::geometry::Geometry;

/// Encode a (longitude, latitude) coordinate to an H3 cell index.
///
/// Resolution 0-15. Default 7 (~5.1 km² cells).
/// Returns the H3 index as a u64.
pub fn h3_encode(lng: f64, lat: f64, resolution: u8) -> Option<u64> {
    let res = Resolution::try_from(resolution).ok()?;
    let ll = LatLng::new(lat, lng).ok()?;
    let cell = ll.to_cell(res);
    Some(u64::from(cell))
}

/// Encode to H3 hex string (standard representation).
pub fn h3_encode_string(lng: f64, lat: f64, resolution: u8) -> Option<String> {
    let res = Resolution::try_from(resolution).ok()?;
    let ll = LatLng::new(lat, lng).ok()?;
    let cell = ll.to_cell(res);
    Some(cell.to_string())
}

/// Decode an H3 cell index to its center point (lng, lat).
pub fn h3_to_center(h3_index: u64) -> Option<(f64, f64)> {
    let cell = CellIndex::try_from(h3_index).ok()?;
    let ll = LatLng::from(cell);
    Some((ll.lng(), ll.lat()))
}

/// Decode an H3 cell index to its boundary polygon.
///
/// Returns a closed ring of [lng, lat] coordinates.
pub fn h3_to_boundary(h3_index: u64) -> Option<Geometry> {
    let cell = CellIndex::try_from(h3_index).ok()?;
    let boundary = cell.boundary();
    let mut ring: Vec<[f64; 2]> = boundary.iter().map(|ll| [ll.lng(), ll.lat()]).collect();
    // Close the ring.
    if let Some(&first) = ring.first() {
        ring.push(first);
    }
    Some(Geometry::Polygon {
        coordinates: vec![ring],
    })
}

/// Get the resolution of an H3 cell index.
pub fn h3_resolution(h3_index: u64) -> Option<u8> {
    let cell = CellIndex::try_from(h3_index).ok()?;
    Some(cell.resolution() as u8)
}

/// Get the parent cell at a coarser resolution.
pub fn h3_parent(h3_index: u64, parent_resolution: u8) -> Option<u64> {
    let cell = CellIndex::try_from(h3_index).ok()?;
    let res = Resolution::try_from(parent_resolution).ok()?;
    cell.parent(res).map(u64::from)
}

/// Get all neighbor cells (k-ring of distance 1).
pub fn h3_neighbors(h3_index: u64) -> Vec<u64> {
    let Ok(cell) = CellIndex::try_from(h3_index) else {
        return Vec::new();
    };
    cell.grid_disk::<Vec<_>>(1)
        .into_iter()
        .filter(|&c| c != cell)
        .map(u64::from)
        .collect()
}

/// Check if an H3 index is valid.
pub fn h3_is_valid(h3_index: u64) -> bool {
    CellIndex::try_from(h3_index).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_nyc() {
        let idx = h3_encode(-73.9857, 40.7484, 7).unwrap();
        assert!(h3_is_valid(idx));
        assert_eq!(h3_resolution(idx).unwrap(), 7);
    }

    #[test]
    fn encode_string_roundtrip() {
        let hex = h3_encode_string(0.0, 0.0, 5).unwrap();
        assert!(!hex.is_empty());
    }

    #[test]
    fn center_roundtrip() {
        let idx = h3_encode(10.0, 50.0, 9).unwrap();
        let (lng, lat) = h3_to_center(idx).unwrap();
        assert!((lng - 10.0).abs() < 0.01, "lng={lng}");
        assert!((lat - 50.0).abs() < 0.01, "lat={lat}");
    }

    #[test]
    fn boundary_is_polygon() {
        let idx = h3_encode(0.0, 0.0, 5).unwrap();
        let poly = h3_to_boundary(idx).unwrap();
        assert_eq!(poly.geometry_type(), "Polygon");
        if let Geometry::Polygon { coordinates } = &poly {
            // Hexagon has 6 vertices + close = 7 points.
            assert!(coordinates[0].len() >= 7, "len={}", coordinates[0].len());
        }
    }

    #[test]
    fn resolution_accessor() {
        for res in 0..=15 {
            let idx = h3_encode(0.0, 0.0, res).unwrap();
            assert_eq!(h3_resolution(idx).unwrap(), res);
        }
    }

    #[test]
    fn parent_is_coarser() {
        let child = h3_encode(0.0, 0.0, 9).unwrap();
        let parent = h3_parent(child, 7).unwrap();
        assert_eq!(h3_resolution(parent).unwrap(), 7);
    }

    #[test]
    fn neighbors_count() {
        let idx = h3_encode(0.0, 0.0, 7).unwrap();
        let nbrs = h3_neighbors(idx);
        // Hexagon has 6 neighbors.
        assert_eq!(nbrs.len(), 6, "got {} neighbors", nbrs.len());
    }

    #[test]
    fn invalid_index() {
        assert!(!h3_is_valid(0));
        assert!(h3_to_center(0).is_none());
    }

    #[test]
    fn nearby_points_same_cell() {
        let a = h3_encode(-73.985, 40.758, 9).unwrap();
        let b = h3_encode(-73.9851, 40.7581, 9).unwrap();
        // Very close points should be in the same cell.
        assert_eq!(a, b);
    }

    #[test]
    fn different_resolutions_different_cells() {
        let coarse = h3_encode(0.0, 0.0, 3).unwrap();
        let fine = h3_encode(0.0, 0.0, 9).unwrap();
        assert_ne!(coarse, fine);
    }
}
