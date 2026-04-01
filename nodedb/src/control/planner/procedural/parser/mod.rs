//! Procedural SQL parser.
//!
//! Converts a token stream from the tokenizer into a `ProceduralBlock` AST.
//! Split into sub-modules by concern: statement parsers, exception handlers, utilities.

mod exception;
pub(crate) mod statements;
mod utils;

use super::ast::*;
use super::error::ProceduralError;
use super::tokenizer::Token;
use utils::*;

/// Parse a procedural SQL body into a `ProceduralBlock`.
///
/// Input: raw SQL text starting with `BEGIN` and ending with `END`.
pub fn parse_block(input: &str) -> Result<ProceduralBlock, ProceduralError> {
    let tokens = super::tokenizer::tokenize(input)?;
    let mut pos = 0;

    skip_token(&tokens, &mut pos, &Token::Begin)?;

    let statements = parse_statements(&tokens, &mut pos)?;

    let exception_handlers = if pos < tokens.len() && tokens[pos] == Token::Exception {
        exception::parse_exception_handlers(&tokens, &mut pos)?
    } else {
        Vec::new()
    };

    expect_token(&tokens, &mut pos, &Token::End)?;
    skip_if(&tokens, &mut pos, &Token::Semicolon);

    Ok(ProceduralBlock {
        statements,
        exception_handlers,
    })
}

/// Parse a sequence of statements until we hit END, ELSE, ELSIF, EXCEPTION, or end of tokens.
pub(crate) fn parse_statements(
    tokens: &[Token],
    pos: &mut usize,
) -> Result<Vec<Statement>, ProceduralError> {
    let mut stmts = Vec::new();

    while *pos < tokens.len() {
        match tokens.get(*pos) {
            Some(
                Token::End
                | Token::EndIf
                | Token::EndLoop
                | Token::Else
                | Token::Elsif
                | Token::Exception,
            ) => {
                break;
            }
            None => break,
            _ => {}
        }

        stmts.push(statements::parse_statement(tokens, pos)?);
    }

    Ok(stmts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_return() {
        let block = parse_block("BEGIN RETURN 42; END").unwrap();
        assert_eq!(block.statements.len(), 1);
        assert!(matches!(&block.statements[0], Statement::Return { expr } if expr.sql == "42"));
    }

    #[test]
    fn parse_if_else() {
        let block =
            parse_block("BEGIN IF x > 0 THEN RETURN 1; ELSE RETURN 0; END IF; END").unwrap();
        assert_eq!(block.statements.len(), 1);
        let Statement::If {
            condition,
            then_block,
            else_block,
            ..
        } = &block.statements[0]
        else {
            panic!("expected If");
        };
        assert_eq!(condition.sql, "x > 0");
        assert_eq!(then_block.len(), 1);
        assert!(else_block.is_some());
    }

    #[test]
    fn parse_if_elsif_else() {
        let block = parse_block(
            "BEGIN \
             IF x > 10 THEN RETURN 'high'; \
             ELSIF x > 5 THEN RETURN 'mid'; \
             ELSE RETURN 'low'; \
             END IF; \
             END",
        )
        .unwrap();
        let Statement::If {
            elsif_branches,
            else_block,
            ..
        } = &block.statements[0]
        else {
            panic!("expected If");
        };
        assert_eq!(elsif_branches.len(), 1);
        assert!(else_block.is_some());
    }

    #[test]
    fn parse_declare_and_assign() {
        let block = parse_block("BEGIN DECLARE x INT := 0; x := x + 1; RETURN x; END").unwrap();
        assert_eq!(block.statements.len(), 3);
        assert!(matches!(&block.statements[0], Statement::Declare { name, .. } if name == "x"));
        assert!(matches!(&block.statements[1], Statement::Assign { target, .. } if target == "x"));
    }

    #[test]
    fn parse_while_loop() {
        let block = parse_block("BEGIN WHILE i < 10 LOOP i := i + 1; END LOOP; END").unwrap();
        assert_eq!(block.statements.len(), 1);
        assert!(matches!(&block.statements[0], Statement::While { .. }));
    }

    #[test]
    fn parse_for_loop() {
        let block = parse_block("BEGIN FOR i IN 1..10 LOOP BREAK; END LOOP; END").unwrap();
        let Statement::For {
            var, reverse, body, ..
        } = &block.statements[0]
        else {
            panic!("expected For");
        };
        assert_eq!(var, "i");
        assert!(!reverse);
        assert_eq!(body.len(), 1);
    }

    #[test]
    fn parse_dml_detected() {
        let block = parse_block("BEGIN INSERT INTO users VALUES (1); END").unwrap();
        assert!(matches!(&block.statements[0], Statement::Dml { .. }));
    }

    #[test]
    fn parse_raise() {
        let block = parse_block("BEGIN RAISE EXCEPTION 'bad input'; END").unwrap();
        let Statement::Raise { level, message } = &block.statements[0] else {
            panic!("expected Raise");
        };
        assert_eq!(*level, RaiseLevel::Exception);
        assert!(message.sql.contains("bad input"));
    }

    #[test]
    fn parse_nested_if() {
        let block = parse_block(
            "BEGIN \
             IF x > 0 THEN \
               IF x > 10 THEN RETURN 'big'; \
               ELSE RETURN 'small'; \
               END IF; \
             END IF; \
             END",
        )
        .unwrap();
        let Statement::If { then_block, .. } = &block.statements[0] else {
            panic!("expected If");
        };
        assert!(matches!(&then_block[0], Statement::If { .. }));
    }
}
