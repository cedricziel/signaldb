//! # Query IR pagination and tail cursors
//!
//! A cursor names a position in the total order a `page` or `tail` walks
//! (`openspec/changes/query-result-pagination-and-tail`, design D4):
//!
//! ```text
//! sdbc1.<base64url(payload)>.<base64url(sha256("sdbc1." || payload)[0..16])>
//! ```
//!
//! The payload is compact JSON. The checksum catches corruption and casual
//! editing; it is not a signature, since every value in a cursor is one the
//! caller could put in its own document. The fingerprint binds a cursor to
//! the tenant, dataset and document that produced it.

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The token prefix of the current cursor format.
const PREFIX: &str = "sdbc1";
/// The payload format version inside a `sdbc1` token.
const VERSION: u32 = 1;
/// Bytes of the SHA-256 digest kept as the checksum.
const CHECKSUM_LEN: usize = 16;

/// Why a presented cursor cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CursorError {
    /// Not a cursor, or its content does not match its checksum.
    #[error("the cursor is malformed or corrupted")]
    Corrupt,
    /// Past its lifetime, or from an incompatible cursor format.
    #[error("the cursor has expired: {0}")]
    Expired(&'static str),
    /// Issued for another tenant, dataset or document.
    #[error("the cursor belongs to another tenant, dataset or document")]
    Mismatch,
}

/// Whether a cursor continues a `page` walk or a `tail`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CursorKind {
    Page,
    Tail,
}

/// One value of a row's sort key, typed so it compares the way the column
/// does when the next page rebuilds the keyset predicate.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", content = "v", rename_all = "snake_case")]
pub enum KeyValue {
    Null,
    I64(i64),
    /// Encoded as text so NaN and the infinities survive JSON.
    F64(#[serde(with = "f64_text")] f64),
    Str(String),
    Bytes(#[serde(with = "base64_text")] Vec<u8>),
    Bool(bool),
}

/// One column of a sort key and the value the last emitted row held there.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyPart {
    #[serde(rename = "c")]
    pub field: String,
    #[serde(flatten)]
    pub value: KeyValue,
}

/// The decoded content of a cursor.
#[derive(Debug, Clone, PartialEq)]
pub struct Cursor {
    pub kind: CursorKind,
    /// [`fingerprint`] of the request that issued it.
    pub fingerprint: String,
    /// The frozen absolute window `[start_ns, end_ns]` of a page walk, or a
    /// tail's lower bound and the end it read up to.
    pub window: [i64; 2],
    /// The last emitted row's full sort key.
    pub key: Vec<KeyPart>,
    /// Per-key direction, `a`/`d`, cross-checked against the re-derived order.
    pub dir: String,
    /// Rows (or traces) emitted so far in the walk.
    pub emitted: u64,
    /// When it was issued, unix epoch nanoseconds.
    pub issued_at_ns: i64,
    /// A tail's `settled_through_ns` of the previous call.
    pub settled_through_ns: Option<i64>,
}

/// The `page` object of a `query_ir` Flight ticket: how the querier sorts,
/// resumes and cuts one page of a document's result.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PageRequest {
    /// Rows (or traces) to return.
    pub size: u32,
    pub unit: query_ir::PageUnit,
    /// The total order, from [`query_ir::pagination_order`].
    pub order: Vec<query_ir::SortKey>,
    /// Resume strictly after this sort key; absent on the first page.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<Vec<KeyPart>>,
    /// Cut at exactly `size` rows, even inside a tie group: a trailing
    /// `limit` caps the walk here.
    #[serde(default)]
    pub exact: bool,
}

/// What the querier emitted for a [`PageRequest`], reported in the Flight
/// trailer (`common::flight::CorrelateReport::page`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PageReport {
    /// The full sort key of the last emitted row; absent on an empty page.
    #[serde(default)]
    pub last_key: Option<Vec<KeyPart>>,
    /// Whether rows (or traces) follow the page.
    pub has_more: bool,
    /// Rows (or traces) on the page.
    pub emitted: u64,
}

#[derive(Serialize, Deserialize)]
struct Payload {
    v: u32,
    k: CursorKind,
    fp: String,
    w: [i64; 2],
    key: Vec<KeyPart>,
    dir: String,
    n: u64,
    iat: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    st: Option<i64>,
}

impl Cursor {
    /// The opaque token for this cursor.
    pub fn encode(&self) -> String {
        let payload = Payload {
            v: VERSION,
            k: self.kind,
            fp: self.fingerprint.clone(),
            w: self.window,
            key: self.key.clone(),
            dir: self.dir.clone(),
            n: self.emitted,
            iat: self.issued_at_ns,
            st: self.settled_through_ns,
        };
        // A struct of plain values and strings always serializes.
        let json = serde_json::to_vec(&payload).unwrap_or_default();
        let body = URL_SAFE_NO_PAD.encode(json);
        format!("{PREFIX}.{body}.{}", checksum(&body))
    }

    /// Decode `token` and check it against the request presenting it: its
    /// kind, its lifetime (`ttl_ns` from issue, at `now_ns`) and the
    /// request's [`fingerprint`]. The checks run in that order, so a corrupt
    /// token is never reported as expired.
    pub fn decode(
        token: &str,
        kind: CursorKind,
        fingerprint: &str,
        now_ns: i64,
        ttl_ns: i64,
    ) -> Result<Self, CursorError> {
        let mut parts = token.split('.');
        let (Some(prefix), Some(body), Some(sum), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(CursorError::Corrupt);
        };
        if prefix != PREFIX {
            return Err(if prefix.starts_with("sdbc") {
                CursorError::Expired("issued by an incompatible server version")
            } else {
                CursorError::Corrupt
            });
        }
        if checksum(body) != sum {
            return Err(CursorError::Corrupt);
        }
        let json = URL_SAFE_NO_PAD
            .decode(body)
            .map_err(|_| CursorError::Corrupt)?;
        let raw: serde_json::Value =
            serde_json::from_slice(&json).map_err(|_| CursorError::Corrupt)?;
        if raw.get("v").and_then(serde_json::Value::as_u64) != Some(u64::from(VERSION)) {
            return Err(CursorError::Expired(
                "issued by an incompatible server version",
            ));
        }
        let p: Payload = serde_json::from_value(raw).map_err(|_| CursorError::Corrupt)?;
        if p.k != kind {
            return Err(CursorError::Mismatch);
        }
        if now_ns.saturating_sub(p.iat) > ttl_ns {
            return Err(CursorError::Expired("past its lifetime"));
        }
        if p.fp != fingerprint {
            return Err(CursorError::Mismatch);
        }
        Ok(Cursor {
            kind: p.k,
            fingerprint: p.fp,
            window: p.w,
            key: p.key,
            dir: p.dir,
            emitted: p.n,
            issued_at_ns: p.iat,
            settled_through_ns: p.st,
        })
    }
}

fn checksum(body: &str) -> String {
    let digest = Sha256::digest(format!("{PREFIX}.{body}").as_bytes());
    URL_SAFE_NO_PAD.encode(&digest[..CHECKSUM_LEN])
}

/// SHA-256 (hex) over the tenant, dataset and request document a cursor is
/// bound to. The document is taken without `page.cursor`/`tail.cursor` and
/// with object keys sorted, so the fingerprint does not depend on the
/// cursor itself or on the client's key order. `irVersion` is part of the
/// document.
pub fn fingerprint(tenant_id: &str, dataset_id: &str, document: &serde_json::Value) -> String {
    let mut document = document.clone();
    for member in ["page", "tail"] {
        if let Some(obj) = document.get_mut(member).and_then(|v| v.as_object_mut()) {
            obj.remove("cursor");
        }
    }
    let mut canonical = String::new();
    write_canonical(&document, &mut canonical);
    let mut hasher = Sha256::new();
    for part in [tenant_id, dataset_id, &canonical] {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    hex::encode(hasher.finalize())
}

fn write_canonical(value: &serde_json::Value, out: &mut String) {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|a, b| a.0.cmp(b.0));
            out.push('{');
            for (i, (k, v)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::Value::String(k.clone()).to_string());
                out.push(':');
                write_canonical(v, out);
            }
            out.push('}');
        }
        serde_json::Value::Array(items) => {
            out.push('[');
            for (i, v) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(v, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

mod f64_text {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &f64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
        String::deserialize(d)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

mod base64_text {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&URL_SAFE_NO_PAD.encode(v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        URL_SAFE_NO_PAD
            .decode(String::deserialize(d)?)
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NOW: i64 = 1_700_000_000_000_000_000;
    const TTL: i64 = 15 * 60 * 1_000_000_000;

    fn cursor(kind: CursorKind) -> Cursor {
        Cursor {
            kind,
            fingerprint: "fp".into(),
            window: [1, 2],
            key: vec![
                KeyPart {
                    field: "timestamp".into(),
                    value: KeyValue::I64(42),
                },
                KeyPart {
                    field: "trace_id".into(),
                    value: KeyValue::Str("abc".into()),
                },
                KeyPart {
                    field: "raw".into(),
                    value: KeyValue::Bytes(vec![0, 255, 7]),
                },
                KeyPart {
                    field: "span_id".into(),
                    value: KeyValue::Null,
                },
                KeyPart {
                    field: "score".into(),
                    value: KeyValue::F64(f64::INFINITY),
                },
                KeyPart {
                    field: "is_root".into(),
                    value: KeyValue::Bool(true),
                },
            ],
            dir: "daaaaa".into(),
            emitted: 1000,
            issued_at_ns: NOW,
            settled_through_ns: (kind == CursorKind::Tail).then_some(NOW - 10),
        }
    }

    fn decode(token: &str, kind: CursorKind) -> Result<Cursor, CursorError> {
        Cursor::decode(token, kind, "fp", NOW, TTL)
    }

    #[test]
    fn page_and_tail_cursors_round_trip_with_typed_keys() {
        for kind in [CursorKind::Page, CursorKind::Tail] {
            let c = cursor(kind);
            let token = c.encode();
            assert!(token.starts_with("sdbc1."));
            assert_eq!(decode(&token, kind), Ok(c));
        }
    }

    #[test]
    fn a_key_part_is_a_flat_typed_object() {
        let part = KeyPart {
            field: "timestamp".into(),
            value: KeyValue::I64(42),
        };
        assert_eq!(
            serde_json::to_value(&part).expect("serializes"),
            json!({ "c": "timestamp", "t": "i64", "v": 42 })
        );
    }

    #[test]
    fn a_checksum_mismatch_is_corrupt() {
        let token = cursor(CursorKind::Page).encode();
        let mut parts: Vec<&str> = token.split('.').collect();
        let edited = URL_SAFE_NO_PAD.encode(
            String::from_utf8(URL_SAFE_NO_PAD.decode(parts[1]).expect("base64"))
                .expect("utf8")
                .replace("\"n\":1000", "\"n\":0"),
        );
        parts[1] = &edited;
        assert_eq!(
            decode(&parts.join("."), CursorKind::Page),
            Err(CursorError::Corrupt)
        );
        assert_eq!(
            decode("garbage", CursorKind::Page),
            Err(CursorError::Corrupt)
        );
        assert_eq!(
            decode("nope.a.b", CursorKind::Page),
            Err(CursorError::Corrupt)
        );
    }

    #[test]
    fn an_unknown_version_is_expired() {
        let token = cursor(CursorKind::Page)
            .encode()
            .replacen("sdbc1", "sdbc2", 1);
        assert!(matches!(
            decode(&token, CursorKind::Page),
            Err(CursorError::Expired(_))
        ));

        let body = URL_SAFE_NO_PAD.encode(br#"{"v":2}"#);
        let token = format!("sdbc1.{body}.{}", checksum(&body));
        assert!(matches!(
            decode(&token, CursorKind::Page),
            Err(CursorError::Expired(_))
        ));
    }

    #[test]
    fn a_cursor_past_its_ttl_is_expired() {
        let token = cursor(CursorKind::Page).encode();
        assert!(Cursor::decode(&token, CursorKind::Page, "fp", NOW + TTL, TTL).is_ok());
        assert!(matches!(
            Cursor::decode(&token, CursorKind::Page, "fp", NOW + TTL + 1, TTL),
            Err(CursorError::Expired(_))
        ));
    }

    #[test]
    fn a_cursor_of_another_kind_or_fingerprint_mismatches() {
        let token = cursor(CursorKind::Page).encode();
        assert_eq!(decode(&token, CursorKind::Tail), Err(CursorError::Mismatch));
        assert_eq!(
            Cursor::decode(&token, CursorKind::Page, "other", NOW, TTL),
            Err(CursorError::Mismatch)
        );
    }

    fn document() -> serde_json::Value {
        json!({
            "irVersion": 13, "from": "logs", "result": "rows",
            "range": { "from": "now-1h", "to": "now" }, "pipeline": [],
            "page": { "size": 100 }
        })
    }

    #[test]
    fn the_fingerprint_binds_tenant_dataset_document_and_version() {
        let base = fingerprint("t1", "d1", &document());
        assert_ne!(fingerprint("t2", "d1", &document()), base);
        assert_ne!(fingerprint("t1", "d2", &document()), base);
        let mut edited = document();
        edited["from"] = json!("traces");
        assert_ne!(fingerprint("t1", "d1", &edited), base);
        let mut version = document();
        version["irVersion"] = json!(14);
        assert_ne!(fingerprint("t1", "d1", &version), base);
        // Field boundaries are unambiguous.
        assert_ne!(fingerprint("t1d", "1", &document()), base);
    }

    #[test]
    fn the_fingerprint_ignores_the_cursor_and_key_order() {
        let base = fingerprint("t", "d", &document());
        let mut with_cursor = document();
        with_cursor["page"]["cursor"] = json!("sdbc1.x.y");
        assert_eq!(fingerprint("t", "d", &with_cursor), base);

        let reordered: serde_json::Value = serde_json::from_str(
            r#"{"page":{"size":100},"pipeline":[],"range":{"to":"now","from":"now-1h"},
                "result":"rows","from":"logs","irVersion":13}"#,
        )
        .expect("json");
        assert_eq!(fingerprint("t", "d", &reordered), base);

        let tail = json!({ "irVersion": 14, "tail": { "settle": "10s" } });
        let mut tail_cursor = tail.clone();
        tail_cursor["tail"]["cursor"] = json!("sdbc1.x.y");
        assert_eq!(
            fingerprint("t", "d", &tail),
            fingerprint("t", "d", &tail_cursor)
        );
    }
}
