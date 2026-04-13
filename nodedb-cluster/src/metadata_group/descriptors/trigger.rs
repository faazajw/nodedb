//! Trigger descriptor.

use serde::{Deserialize, Serialize};

use crate::metadata_group::descriptors::common::DescriptorHeader;

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TriggerDescriptor {
    pub header: DescriptorHeader,
    pub collection: String,
    pub timing: TriggerTiming,
    pub events: Vec<TriggerEvent>,
    pub for_each_row: bool,
    pub when_clause: Option<String>,
    pub action_body: String,
    pub enabled: bool,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum TriggerTiming {
    Before,
    After,
    InsteadOf,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum TriggerEvent {
    Insert,
    Update,
    Delete,
    Truncate,
}
