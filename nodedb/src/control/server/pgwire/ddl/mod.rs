pub mod apikey;
pub mod backup;
pub mod bulk;
pub mod cluster;
pub mod collection;
pub mod crdt_ops;
pub mod dsl;
pub mod grant;
pub mod inspect;
pub mod ownership;
pub mod role;
pub mod router;
pub mod service_account;
mod sync_dispatch;
pub mod tenant;
pub mod user;

pub use router::dispatch;
