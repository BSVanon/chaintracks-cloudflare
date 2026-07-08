//! D1 database helpers — parameterized queries for Cloudflare D1.
//!
//! Adapted from ~/bsv/rust-overlay/crates/overlay-cloudflare/src/d1/mod.rs.

use serde::de::DeserializeOwned;
use worker::wasm_bindgen::JsValue;
use worker::{D1Database, D1PreparedStatement};

/// A value that can be bound to a D1 prepared statement.
pub enum QVal {
    Null,
    Int(i64),
    Text(String),
    Bool(bool),
    Float(f64),
}

impl QVal {
    pub fn to_js(&self) -> JsValue {
        match self {
            Self::Null => JsValue::null(),
            Self::Int(i) => JsValue::from_f64(*i as f64),
            Self::Text(s) => JsValue::from_str(s),
            Self::Bool(b) => JsValue::from_f64(if *b { 1.0 } else { 0.0 }),
            Self::Float(f) => JsValue::from_f64(*f),
        }
    }
}

impl From<i64> for QVal {
    fn from(v: i64) -> Self {
        Self::Int(v)
    }
}
impl From<i32> for QVal {
    fn from(v: i32) -> Self {
        Self::Int(v as i64)
    }
}
impl From<u32> for QVal {
    fn from(v: u32) -> Self {
        Self::Int(v as i64)
    }
}
impl From<u64> for QVal {
    fn from(v: u64) -> Self {
        Self::Int(v as i64)
    }
}
impl From<String> for QVal {
    fn from(v: String) -> Self {
        Self::Text(v)
    }
}
impl From<&str> for QVal {
    fn from(v: &str) -> Self {
        Self::Text(v.to_string())
    }
}
impl From<bool> for QVal {
    fn from(v: bool) -> Self {
        Self::Bool(v)
    }
}
impl From<f64> for QVal {
    fn from(v: f64) -> Self {
        Self::Float(v)
    }
}

impl<T: Into<QVal>> From<Option<T>> for QVal {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(inner) => inner.into(),
            None => Self::Null,
        }
    }
}

/// Builds a parameterized D1 query with bind values.
pub struct Query {
    sql: String,
    params: Vec<QVal>,
}

impl Query {
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            params: Vec::new(),
        }
    }

    pub fn bind(mut self, val: impl Into<QVal>) -> Self {
        self.params.push(val.into());
        self
    }

    pub fn prepare(self, db: &D1Database) -> worker::Result<D1PreparedStatement> {
        let stmt = db.prepare(&self.sql);
        if self.params.is_empty() {
            return Ok(stmt);
        }
        let js_values: Vec<JsValue> = self.params.iter().map(|v| v.to_js()).collect();
        stmt.bind(&js_values)
    }

    #[allow(dead_code)]
    pub async fn run(self, db: &D1Database) -> worker::Result<()> {
        self.prepare(db)?.run().await?;
        Ok(())
    }

    pub async fn first<T: DeserializeOwned>(self, db: &D1Database) -> worker::Result<Option<T>> {
        let result = self.prepare(db)?.first::<T>(None).await?;
        Ok(result)
    }

    pub async fn all<T: DeserializeOwned>(self, db: &D1Database) -> worker::Result<Vec<T>> {
        let result = self.prepare(db)?.all().await?;
        result.results::<T>()
    }
}

// ─── Batch Collector ────────────────────────────────────────────────────────

/// Collects prepared statements for atomic batch execution via `db.batch()`.
///
/// D1 has no BEGIN/COMMIT. Instead, `db.batch(stmts)` executes all statements
/// atomically (up to 100 per batch). If the batch exceeds 100 statements,
/// it is split into sequential sub-batches.
#[allow(dead_code)]
pub struct BatchCollector<'a> {
    db: &'a D1Database,
    statements: Vec<D1PreparedStatement>,
}

#[allow(dead_code)]
impl<'a> BatchCollector<'a> {
    pub fn new(db: &'a D1Database) -> Self {
        Self {
            db,
            statements: Vec::new(),
        }
    }

    /// Add a parameterized statement to the batch.
    pub fn add(&mut self, sql: &str, params: Vec<QVal>) -> worker::Result<()> {
        let stmt = self.db.prepare(sql);
        let bound = if params.is_empty() {
            stmt
        } else {
            let js_values: Vec<JsValue> = params.iter().map(|v| v.to_js()).collect();
            stmt.bind(&js_values)?
        };
        self.statements.push(bound);
        Ok(())
    }

    /// Number of statements in the batch.
    pub fn len(&self) -> usize {
        self.statements.len()
    }

    pub fn is_empty(&self) -> bool {
        self.statements.is_empty()
    }

    /// Execute all statements atomically.
    ///
    /// Splits into chunks of 100 (D1 limit). Each sub-batch is atomic
    /// internally, but failures in later batches won't roll back earlier ones.
    pub async fn execute(self) -> worker::Result<Vec<worker::D1Result>> {
        if self.statements.is_empty() {
            return Ok(Vec::new());
        }

        let mut all_results = Vec::new();
        for chunk in self.statements.chunks(100) {
            let batch: Vec<D1PreparedStatement> = chunk.to_vec();
            let results = self.db.batch(batch).await?;
            all_results.extend(results);
        }
        Ok(all_results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: to_js() tests require WASM target and can't run natively.
    // We test the From impls and Query builder which are pure Rust.

    #[test]
    fn test_from_i64() {
        let val: QVal = 42i64.into();
        assert!(matches!(val, QVal::Int(42)));
    }

    #[test]
    fn test_from_i32() {
        let val: QVal = 42i32.into();
        assert!(matches!(val, QVal::Int(42)));
    }

    #[test]
    fn test_from_u32() {
        let val: QVal = 100u32.into();
        assert!(matches!(val, QVal::Int(100)));
    }

    #[test]
    fn test_from_u64() {
        let val: QVal = 999u64.into();
        assert!(matches!(val, QVal::Int(999)));
    }

    #[test]
    fn test_from_string() {
        let val: QVal = String::from("hello").into();
        assert!(matches!(val, QVal::Text(s) if s == "hello"));
    }

    #[test]
    fn test_from_str() {
        let val: QVal = "test".into();
        assert!(matches!(val, QVal::Text(s) if s == "test"));
    }

    #[test]
    fn test_from_bool() {
        let val: QVal = true.into();
        assert!(matches!(val, QVal::Bool(true)));

        let val: QVal = false.into();
        assert!(matches!(val, QVal::Bool(false)));
    }

    #[test]
    fn test_from_f64() {
        let val: QVal = 3.14f64.into();
        assert!(matches!(val, QVal::Float(f) if (f - 3.14).abs() < f64::EPSILON));
    }

    #[test]
    fn test_from_option_some() {
        let val: QVal = Some(42i64).into();
        assert!(matches!(val, QVal::Int(42)));
    }

    #[test]
    fn test_from_option_none() {
        let val: QVal = Option::<i64>::None.into();
        assert!(matches!(val, QVal::Null));
    }

    #[test]
    fn test_query_builder_bind() {
        let q = Query::new("SELECT * FROM headers WHERE height = ? AND hash = ?")
            .bind(100u32)
            .bind("abc123");
        assert_eq!(q.sql, "SELECT * FROM headers WHERE height = ? AND hash = ?");
        assert_eq!(q.params.len(), 2);
    }

    #[test]
    fn test_query_builder_no_params() {
        let q = Query::new("SELECT COUNT(*) FROM headers");
        assert_eq!(q.params.len(), 0);
    }

    #[test]
    fn test_query_builder_many_params() {
        let q = Query::new("INSERT INTO headers VALUES (?, ?, ?, ?, ?)")
            .bind(1u32)
            .bind("hash")
            .bind(true)
            .bind(3.14f64)
            .bind(Option::<i64>::None);
        assert_eq!(q.params.len(), 5);
    }
}
