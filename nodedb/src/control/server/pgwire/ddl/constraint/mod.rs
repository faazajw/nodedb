//! DDL handlers for all constraint kinds: state transitions, transition checks,
//! general CHECK constraints, and SHOW CONSTRAINTS.

mod handlers;
mod parse;
mod show;
mod validate;

pub use handlers::{
    add_check_constraint, add_state_constraint, add_transition_check, drop_constraint,
};
pub use show::show_constraints;

use pgwire::error::{ErrorInfo, PgWireError};

pub(crate) fn err(code: &str, msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        code.to_owned(),
        msg.to_owned(),
    )))
}
