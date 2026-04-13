//! Stored procedure descriptor.

use serde::{Deserialize, Serialize};

use crate::metadata_group::descriptors::common::DescriptorHeader;
use crate::metadata_group::descriptors::function::FunctionParam;

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
pub struct ProcedureDescriptor {
    pub header: DescriptorHeader,
    pub parameters: Vec<FunctionParam>,
    pub body: String,
    pub language: String,
}
