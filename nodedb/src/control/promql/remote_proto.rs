//! Prometheus remote write/read protobuf message types.
//!
//! Hand-coded prost types matching the Prometheus remote storage protocol:
//! <https://github.com/prometheus/prometheus/blob/main/prompb/remote.proto>
//! <https://github.com/prometheus/prometheus/blob/main/prompb/types.proto>

/// Remote write request: a batch of time series with samples.
#[derive(Clone, PartialEq, prost::Message)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

/// Remote read request: one or more queries.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ReadRequest {
    #[prost(message, repeated, tag = "1")]
    pub queries: Vec<Query>,
}

/// Remote read response: one result per query.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ReadResponse {
    #[prost(message, repeated, tag = "1")]
    pub results: Vec<QueryResult>,
}

/// A single remote read query.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Query {
    #[prost(int64, tag = "1")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag = "2")]
    pub end_timestamp_ms: i64,
    #[prost(message, repeated, tag = "3")]
    pub matchers: Vec<LabelMatcher>,
}

/// Result of a single query in a read response.
#[derive(Clone, PartialEq, prost::Message)]
pub struct QueryResult {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

/// A time series: labels + samples + optional exemplars.
#[derive(Clone, PartialEq, prost::Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
    #[prost(message, repeated, tag = "3")]
    pub exemplars: Vec<Exemplar>,
}

/// A label name-value pair.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

/// A single sample: timestamp + value.
#[derive(Clone, PartialEq, prost::Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64, // milliseconds since epoch
}

/// An exemplar: labels + value + timestamp (links metrics to traces).
#[derive(Clone, PartialEq, prost::Message)]
pub struct Exemplar {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(double, tag = "2")]
    pub value: f64,
    #[prost(int64, tag = "3")]
    pub timestamp: i64,
}

/// Label matcher for remote read queries.
#[derive(Clone, PartialEq, prost::Message)]
pub struct LabelMatcher {
    #[prost(enumeration = "MatchType", tag = "1")]
    pub match_type: i32,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub value: String,
}

/// Label match type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, prost::Enumeration)]
#[repr(i32)]
pub enum MatchType {
    Eq = 0,
    Neq = 1,
    Re = 2,
    Nre = 3,
}

impl TimeSeries {
    /// Get the `__name__` label value.
    pub fn metric_name(&self) -> &str {
        self.labels
            .iter()
            .find(|l| l.name == "__name__")
            .map_or("", |l| l.value.as_str())
    }

    /// Convert to ILP line format for ingestion into the timeseries engine.
    ///
    /// Format: `measurement,tag1=val1,tag2=val2 value=X timestampNs`
    pub fn to_ilp_lines(&self) -> Vec<String> {
        let metric = self.metric_name();
        if metric.is_empty() {
            return vec![];
        }

        // Build tag set from labels (excluding __name__).
        let tags: Vec<String> = self
            .labels
            .iter()
            .filter(|l| l.name != "__name__" && !l.name.is_empty())
            .map(|l| format!("{}={}", l.name, l.value))
            .collect();

        let tag_str = if tags.is_empty() {
            String::new()
        } else {
            format!(",{}", tags.join(","))
        };

        self.samples
            .iter()
            .map(|s| {
                format!(
                    "{metric}{tag_str} value={} {}",
                    s.value,
                    s.timestamp * 1_000_000 // ms → ns for ILP
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;

    #[test]
    fn write_request_roundtrip() {
        let req = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    Label {
                        name: "__name__".into(),
                        value: "up".into(),
                    },
                    Label {
                        name: "job".into(),
                        value: "api".into(),
                    },
                ],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1000,
                }],
                exemplars: vec![],
            }],
        };

        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        let decoded = WriteRequest::decode(&buf[..]).unwrap();
        assert_eq!(decoded.timeseries.len(), 1);
        assert_eq!(decoded.timeseries[0].metric_name(), "up");
    }

    #[test]
    fn to_ilp_lines() {
        let ts = TimeSeries {
            labels: vec![
                Label {
                    name: "__name__".into(),
                    value: "http_requests".into(),
                },
                Label {
                    name: "method".into(),
                    value: "GET".into(),
                },
            ],
            samples: vec![
                Sample {
                    value: 42.0,
                    timestamp: 1000,
                },
                Sample {
                    value: 43.0,
                    timestamp: 2000,
                },
            ],
            exemplars: vec![],
        };
        let lines = ts.to_ilp_lines();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].starts_with("http_requests,method=GET value=42"));
        assert!(lines[0].ends_with("1000000000"));
    }

    #[test]
    fn read_request_roundtrip() {
        let req = ReadRequest {
            queries: vec![Query {
                start_timestamp_ms: 0,
                end_timestamp_ms: 10000,
                matchers: vec![LabelMatcher {
                    match_type: MatchType::Eq as i32,
                    name: "__name__".into(),
                    value: "up".into(),
                }],
            }],
        };

        let mut buf = Vec::new();
        req.encode(&mut buf).unwrap();
        let decoded = ReadRequest::decode(&buf[..]).unwrap();
        assert_eq!(decoded.queries.len(), 1);
        assert_eq!(decoded.queries[0].matchers[0].value, "up");
    }
}
