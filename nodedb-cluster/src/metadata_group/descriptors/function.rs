//! SQL/WASM function descriptor.

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
pub struct FunctionDescriptor {
    pub header: DescriptorHeader,
    pub language: FunctionLanguage,
    pub parameters: Vec<FunctionParam>,
    pub return_type: String,
    pub body: String,
    pub deterministic: bool,
}

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
pub enum FunctionLanguage {
    Sql,
    Wasm { module_digest: [u8; 32] },
    PlPgSql,
}

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
pub struct FunctionParam {
    pub name: String,
    pub data_type: String,
    pub default: Option<String>,
}
