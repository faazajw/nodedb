//! Scheduled job descriptor.

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
pub struct ScheduleDescriptor {
    pub header: DescriptorHeader,
    pub cron: String,
    pub timezone: String,
    pub sql_body: String,
    pub enabled: bool,
    pub max_concurrent: u32,
}
