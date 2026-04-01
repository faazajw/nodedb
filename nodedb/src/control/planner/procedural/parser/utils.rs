//! Token utility functions for the procedural SQL parser.

use super::super::error::ProceduralError;
use super::super::tokenizer::Token;

/// Check if a token matches a pattern token (ignoring content for parameterized variants).
pub(super) fn token_matches(token: &Token, pattern: &Token) -> bool {
    std::mem::discriminant(token) == std::mem::discriminant(pattern)
}

pub(super) fn skip_token(
    tokens: &[Token],
    pos: &mut usize,
    expected: &Token,
) -> Result<(), ProceduralError> {
    expect_token(tokens, pos, expected)
}

pub(super) fn expect_token(
    tokens: &[Token],
    pos: &mut usize,
    expected: &Token,
) -> Result<(), ProceduralError> {
    if *pos < tokens.len() && token_matches(&tokens[*pos], expected) {
        *pos += 1;
        Ok(())
    } else {
        Err(ProceduralError::parse(format!(
            "expected {expected:?} at position {pos}, got {:?}",
            tokens.get(*pos)
        )))
    }
}

pub(super) fn expect_ident(tokens: &[Token], pos: &mut usize) -> Result<String, ProceduralError> {
    match tokens.get(*pos) {
        Some(Token::Ident(s)) => {
            let name = s.clone();
            *pos += 1;
            Ok(name)
        }
        other => Err(ProceduralError::parse(format!(
            "expected identifier at position {pos}, got {other:?}"
        ))),
    }
}

pub(super) fn skip_if(tokens: &[Token], pos: &mut usize, token: &Token) {
    if *pos < tokens.len() && token_matches(&tokens[*pos], token) {
        *pos += 1;
    }
}

/// Convert a token back to its SQL text representation.
pub(super) fn token_to_sql(token: &Token) -> String {
    match token {
        Token::Ident(s) => s.clone(),
        Token::StringLit(s) => format!("'{}'", s.replace('\'', "''")),
        Token::NumberLit(s) => s.clone(),
        Token::SqlFragment(s) => s.clone(),
        Token::Semicolon => ";".into(),
        Token::Assign => ":=".into(),
        Token::DotDot => "..".into(),
        Token::In => "IN".into(),
        Token::Reverse => "REVERSE".into(),
        Token::If => "IF".into(),
        Token::Then => "THEN".into(),
        Token::Else => "ELSE".into(),
        Token::End => "END".into(),
        Token::Begin => "BEGIN".into(),
        Token::Loop => "LOOP".into(),
        Token::Return => "RETURN".into(),
        Token::Insert => "INSERT".into(),
        Token::Update => "UPDATE".into(),
        Token::Delete => "DELETE".into(),
        _ => format!("{token:?}"),
    }
}

/// Collect tokens as a SQL expression until one of the terminator tokens is found.
pub(super) fn collect_sql_until(
    tokens: &[Token],
    pos: &mut usize,
    terminators: &[Token],
) -> Result<super::super::ast::SqlExpr, ProceduralError> {
    let sql = collect_raw_sql_until(tokens, pos, terminators);
    if sql.is_empty() {
        return Err(ProceduralError::parse(format!(
            "expected SQL expression before {:?} at position {pos}",
            terminators
        )));
    }
    Ok(super::super::ast::SqlExpr::new(sql))
}

/// Collect tokens as raw SQL text until a terminator is found.
pub(super) fn collect_raw_sql_until(
    tokens: &[Token],
    pos: &mut usize,
    terminators: &[Token],
) -> String {
    let mut parts = Vec::new();
    while *pos < tokens.len() {
        if terminators.iter().any(|t| token_matches(&tokens[*pos], t)) {
            break;
        }
        parts.push(token_to_sql(&tokens[*pos]));
        *pos += 1;
    }
    parts.join(" ").trim().to_string()
}
