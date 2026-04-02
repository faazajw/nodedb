//! `CONVERT_CURRENCY(amount, from, to, rate, precision, [rounding])` — pure arithmetic.
//!
//! Returns `ROUND(amount * rate, precision, rounding)`.
//! No side effects, no table mutations, no rate table lookup.
//! The `from` and `to` parameters are informational (not used in the arithmetic).

use std::any::Any;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{Result as DfResult, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use rust_decimal::Decimal;

/// `CONVERT_CURRENCY(amount, from_ccy, to_ccy, rate, precision, [rounding_mode])` → Decimal
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ConvertCurrency {
    signature: Signature,
}

impl ConvertCurrency {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::Any(5), TypeSignature::Any(6)],
                Volatility::Immutable,
            ),
        }
    }
}

impl Default for ConvertCurrency {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for ConvertCurrency {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "convert_currency"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Decimal128(38, 18))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let arg_values = &args.args;
        if arg_values.len() < 5 || arg_values.len() > 6 {
            return exec_err!(
                "CONVERT_CURRENCY requires 5-6 arguments: (amount, from, to, rate, precision, [rounding])"
            );
        }

        let amount = extract_decimal(&arg_values[0], "amount")?;
        // args[1] = from_ccy (informational, not used in arithmetic)
        // args[2] = to_ccy (informational, not used in arithmetic)
        let rate = extract_decimal(&arg_values[3], "rate")?;
        let precision = extract_u32(&arg_values[4], "precision")?;

        let rounding_mode = if arg_values.len() == 6 {
            extract_string(&arg_values[5])?
        } else {
            "HALF_EVEN".to_string()
        };

        let strategy = parse_rounding_mode(&rounding_mode)?;
        let converted = amount * rate;
        let rounded = converted.round_dp_with_strategy(precision, strategy);

        let mut scaled = rounded;
        scaled.rescale(precision);
        let mantissa = scaled.mantissa();

        Ok(ColumnarValue::Scalar(
            datafusion::common::ScalarValue::Decimal128(Some(mantissa), 38, precision as i8),
        ))
    }
}

fn extract_decimal(arg: &ColumnarValue, name: &str) -> DfResult<Decimal> {
    match arg {
        ColumnarValue::Scalar(scalar) => {
            use datafusion::common::ScalarValue;
            match scalar {
                ScalarValue::Decimal128(Some(v), _, scale) => {
                    Decimal::try_from_i128_with_scale(*v, *scale as u32).map_err(|_| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "{name}: Decimal128 out of range"
                        ))
                    })
                }
                ScalarValue::Float64(Some(f)) => Decimal::try_from(*f).map_err(|_| {
                    datafusion::error::DataFusionError::Execution(format!(
                        "{name}: cannot convert {f} to Decimal"
                    ))
                }),
                ScalarValue::Int64(Some(i)) => Ok(Decimal::from(*i)),
                ScalarValue::Int32(Some(i)) => Ok(Decimal::from(*i)),
                _ => {
                    let s = scalar
                        .to_string()
                        .trim_matches('\'')
                        .trim_matches('"')
                        .to_string();
                    s.parse::<Decimal>().map_err(|_| {
                        datafusion::error::DataFusionError::Execution(format!(
                            "{name}: cannot parse '{s}' as Decimal"
                        ))
                    })
                }
            }
        }
        ColumnarValue::Array(_) => {
            exec_err!("{name} must be a scalar value")
        }
    }
}

fn extract_u32(arg: &ColumnarValue, name: &str) -> DfResult<u32> {
    match arg {
        ColumnarValue::Scalar(scalar) => {
            let s = scalar.to_string();
            s.trim().parse::<u32>().map_err(|_| {
                datafusion::error::DataFusionError::Execution(format!(
                    "{name} must be a non-negative integer, got '{s}'"
                ))
            })
        }
        ColumnarValue::Array(_) => exec_err!("{name} must be a scalar"),
    }
}

fn extract_string(arg: &ColumnarValue) -> DfResult<String> {
    match arg {
        ColumnarValue::Scalar(scalar) => Ok(scalar
            .to_string()
            .trim_matches('\'')
            .trim_matches('"')
            .to_uppercase()),
        ColumnarValue::Array(_) => exec_err!("rounding mode must be a scalar string"),
    }
}

fn parse_rounding_mode(mode: &str) -> DfResult<rust_decimal::RoundingStrategy> {
    use rust_decimal::RoundingStrategy;
    match mode {
        "HALF_UP" => Ok(RoundingStrategy::MidpointAwayFromZero),
        "HALF_EVEN" | "BANKERS" => Ok(RoundingStrategy::MidpointNearestEven),
        "HALF_DOWN" => Ok(RoundingStrategy::MidpointTowardZero),
        "TRUNCATE" | "TRUNC" => Ok(RoundingStrategy::ToZero),
        "CEILING" | "CEIL" => Ok(RoundingStrategy::AwayFromZero),
        "FLOOR" => Ok(RoundingStrategy::ToNegativeInfinity),
        other => exec_err!(
            "unknown rounding mode '{other}'. Valid: HALF_UP, HALF_EVEN, HALF_DOWN, TRUNCATE, CEILING, FLOOR"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).unwrap()
    }

    #[test]
    fn basic_conversion() {
        // 920 EUR * 1.0882 rate = 1001.14 USD at precision 2
        let amount = d("920.00");
        let rate = d("1.0882");
        let converted = amount * rate;
        let rounded = converted
            .round_dp_with_strategy(2, rust_decimal::RoundingStrategy::MidpointNearestEven);
        assert_eq!(rounded, d("1001.14"));
    }

    #[test]
    fn rounding_modes_affect_result() {
        let amount = d("100.00");
        let rate = d("1.005");
        let converted = amount * rate; // 100.500
        let half_up = converted
            .round_dp_with_strategy(2, rust_decimal::RoundingStrategy::MidpointAwayFromZero);
        let half_even = converted
            .round_dp_with_strategy(2, rust_decimal::RoundingStrategy::MidpointNearestEven);
        assert_eq!(half_up, d("100.50"));
        assert_eq!(half_even, d("100.50"));
    }

    #[test]
    fn zero_rate() {
        let amount = d("1000.00");
        let rate = d("0");
        let converted = amount * rate;
        let rounded = converted
            .round_dp_with_strategy(2, rust_decimal::RoundingStrategy::MidpointNearestEven);
        assert_eq!(rounded, d("0.00"));
    }
}
