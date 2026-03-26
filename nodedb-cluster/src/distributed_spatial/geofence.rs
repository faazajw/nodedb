//! Distributed geofence evaluation.
//!
//! Geofence polygons are replicated to all shards (small — typically <10K
//! polygons). Each shard evaluates point updates against the local replica.
//! No cross-shard coordination needed for evaluation. Registration changes
//! propagate via Raft.
//!
//! Use case: when a vehicle position update arrives, check if it enters/exits
//! any registered geofence polygon.

use nodedb_types::geometry::point_in_polygon;
use serde::{Deserialize, Serialize};

/// A registered geofence polygon.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Geofence {
    /// Unique geofence identifier.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// The geofence polygon (exterior ring only for v1).
    pub polygon: Vec<[f64; 2]>,
}

/// Event emitted when a point enters or exits a geofence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeofenceEvent {
    /// The geofence that was entered/exited.
    pub geofence_id: String,
    /// The entity (vehicle, device, etc.) that triggered the event.
    pub entity_id: String,
    /// Whether the entity entered or exited the geofence.
    pub event_type: GeofenceEventType,
    /// The point coordinates that triggered the event.
    pub point: [f64; 2],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GeofenceEventType {
    Enter,
    Exit,
}

/// Geofence registry replicated across all shards.
///
/// Each shard maintains a local copy of all registered geofences. When a
/// point update arrives, the shard evaluates it against all geofences
/// locally — no cross-shard coordination needed.
pub struct GeofenceRegistry {
    geofences: Vec<Geofence>,
    /// entity_id → set of geofence_ids the entity is currently inside.
    entity_state: std::collections::HashMap<String, std::collections::HashSet<String>>,
}

impl GeofenceRegistry {
    pub fn new() -> Self {
        Self {
            geofences: Vec::new(),
            entity_state: std::collections::HashMap::new(),
        }
    }

    /// Register a new geofence polygon.
    pub fn register(&mut self, geofence: Geofence) {
        // Remove old geofence with same ID if exists.
        self.geofences.retain(|g| g.id != geofence.id);
        self.geofences.push(geofence);
    }

    /// Unregister a geofence by ID.
    pub fn unregister(&mut self, geofence_id: &str) {
        self.geofences.retain(|g| g.id != geofence_id);
        // Remove from all entity states.
        for state in self.entity_state.values_mut() {
            state.remove(geofence_id);
        }
    }

    /// Evaluate a point update for an entity. Returns enter/exit events.
    ///
    /// Checks the point against all registered geofences. Compares with
    /// the entity's previous state to detect enter/exit transitions.
    pub fn evaluate_point(&mut self, entity_id: &str, lng: f64, lat: f64) -> Vec<GeofenceEvent> {
        let point = [lng, lat];
        let mut events = Vec::new();

        let current_inside: std::collections::HashSet<String> = self
            .geofences
            .iter()
            .filter(|g| point_in_polygon(lng, lat, &g.polygon))
            .map(|g| g.id.clone())
            .collect();

        let previous = self.entity_state.entry(entity_id.to_string()).or_default();

        // Detect enters: in current but not in previous.
        for gid in &current_inside {
            if !previous.contains(gid) {
                events.push(GeofenceEvent {
                    geofence_id: gid.clone(),
                    entity_id: entity_id.to_string(),
                    event_type: GeofenceEventType::Enter,
                    point,
                });
            }
        }

        // Detect exits: in previous but not in current.
        for gid in previous.iter() {
            if !current_inside.contains(gid) {
                events.push(GeofenceEvent {
                    geofence_id: gid.clone(),
                    entity_id: entity_id.to_string(),
                    event_type: GeofenceEventType::Exit,
                    point,
                });
            }
        }

        // Update state.
        *previous = current_inside;
        events
    }

    /// Number of registered geofences.
    pub fn len(&self) -> usize {
        self.geofences.len()
    }

    pub fn is_empty(&self) -> bool {
        self.geofences.is_empty()
    }

    /// Serialize the registry for replication to other shards.
    pub fn export(&self) -> Vec<u8> {
        // Safety: Vec<Geofence> with Serialize always serializes successfully.
        serde_json::to_vec(&self.geofences).expect("Geofence vec is always serializable")
    }

    /// Import geofences from a serialized registry (from another shard).
    pub fn import(&mut self, data: &[u8]) {
        if let Ok(geofences) = serde_json::from_slice::<Vec<Geofence>>(data) {
            self.geofences = geofences;
        }
    }
}

impl Default for GeofenceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn square_geofence(id: &str, min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Geofence {
        Geofence {
            id: id.to_string(),
            name: format!("zone_{id}"),
            polygon: vec![
                [min_x, min_y],
                [max_x, min_y],
                [max_x, max_y],
                [min_x, max_y],
                [min_x, min_y],
            ],
        }
    }

    #[test]
    fn enter_event() {
        let mut reg = GeofenceRegistry::new();
        reg.register(square_geofence("zone1", 0.0, 0.0, 10.0, 10.0));

        // First position outside.
        let events = reg.evaluate_point("vehicle1", -5.0, -5.0);
        assert!(events.is_empty());

        // Move inside.
        let events = reg.evaluate_point("vehicle1", 5.0, 5.0);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, GeofenceEventType::Enter);
        assert_eq!(events[0].geofence_id, "zone1");
    }

    #[test]
    fn exit_event() {
        let mut reg = GeofenceRegistry::new();
        reg.register(square_geofence("zone1", 0.0, 0.0, 10.0, 10.0));

        // Start inside.
        reg.evaluate_point("v1", 5.0, 5.0);
        // Move outside.
        let events = reg.evaluate_point("v1", 20.0, 20.0);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, GeofenceEventType::Exit);
    }

    #[test]
    fn no_event_when_staying() {
        let mut reg = GeofenceRegistry::new();
        reg.register(square_geofence("zone1", 0.0, 0.0, 10.0, 10.0));

        reg.evaluate_point("v1", 5.0, 5.0); // enter
        let events = reg.evaluate_point("v1", 6.0, 6.0); // still inside
        assert!(events.is_empty());
    }

    #[test]
    fn multiple_geofences() {
        let mut reg = GeofenceRegistry::new();
        reg.register(square_geofence("a", 0.0, 0.0, 10.0, 10.0));
        reg.register(square_geofence("b", 5.0, 5.0, 15.0, 15.0));

        // Point at (7, 7) is inside both.
        let events = reg.evaluate_point("v1", 7.0, 7.0);
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn unregister_geofence() {
        let mut reg = GeofenceRegistry::new();
        reg.register(square_geofence("zone1", 0.0, 0.0, 10.0, 10.0));
        reg.unregister("zone1");
        assert!(reg.is_empty());
    }

    #[test]
    fn export_import_roundtrip() {
        let mut reg = GeofenceRegistry::new();
        reg.register(square_geofence("a", 0.0, 0.0, 10.0, 10.0));
        reg.register(square_geofence("b", 20.0, 20.0, 30.0, 30.0));

        let data = reg.export();
        let mut reg2 = GeofenceRegistry::new();
        reg2.import(&data);
        assert_eq!(reg2.len(), 2);
    }
}
