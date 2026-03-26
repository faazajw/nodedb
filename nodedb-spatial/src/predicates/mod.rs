pub mod contains;
pub mod distance;
pub mod edge;
pub mod intersection;
pub mod intersects;

pub use contains::st_contains;
pub use distance::{st_distance, st_dwithin};
pub use intersection::st_intersection;
pub use intersects::st_intersects;

use nodedb_types::geometry::Geometry;

/// ST_Within(a, b) — A is fully within B. Equivalent to ST_Contains(b, a).
pub fn st_within(a: &Geometry, b: &Geometry) -> bool {
    st_contains(b, a)
}

/// ST_Disjoint(a, b) — no shared space. Inverse of ST_Intersects.
pub fn st_disjoint(a: &Geometry, b: &Geometry) -> bool {
    !st_intersects(a, b)
}
