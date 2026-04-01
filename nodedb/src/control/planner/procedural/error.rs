//! Typed error enum for procedural SQL parsing, compilation, and validation.
//!
//! Replaces `Result<T, String>` throughout the procedural module.

/// Errors from procedural SQL parsing, compilation, and validation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ProceduralError {
    /// Tokenizer failed to lex the input.
    #[error("tokenize error: {detail}")]
    Tokenize { detail: String },

    /// Parser failed to produce a valid AST.
    #[error("parse error: {detail}")]
    Parse { detail: String },

    /// Compiler failed to compile procedural AST to SQL expression.
    #[error("compile error: {detail}")]
    Compile { detail: String },

    /// Validation rejected the procedural block (e.g., DML in function body).
    #[error("validation error: {detail}")]
    Validate { detail: String },
}

impl ProceduralError {
    pub fn tokenize(detail: impl Into<String>) -> Self {
        Self::Tokenize {
            detail: detail.into(),
        }
    }

    pub fn parse(detail: impl Into<String>) -> Self {
        Self::Parse {
            detail: detail.into(),
        }
    }

    pub fn compile(detail: impl Into<String>) -> Self {
        Self::Compile {
            detail: detail.into(),
        }
    }

    pub fn validate(detail: impl Into<String>) -> Self {
        Self::Validate {
            detail: detail.into(),
        }
    }
}

impl From<ProceduralError> for crate::Error {
    fn from(e: ProceduralError) -> Self {
        crate::Error::BadRequest {
            detail: e.to_string(),
        }
    }
}
