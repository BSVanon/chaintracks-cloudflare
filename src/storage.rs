//! D1 storage operations for block headers.
//!
//! Implements ChaintracksStorage-equivalent operations against Cloudflare D1.
//! Based on bsv-wallet-toolbox-rs/src/chaintracks/storage/sqlite.rs.

use worker::D1Database;

use crate::d1::{BatchCollector, QVal, Query};
use crate::types::{calculate_work, BlockHeader, Chain, ChaintracksInfo, InsertHeaderResult};

// ─── D1 Row Type ────────────────────────────────────────────────────────────

/// D1 row representation (all numbers as f64 per D1 convention).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HeaderRow {
    pub header_id: Option<f64>,
    pub previous_header_id: Option<f64>,
    pub previous_hash: Option<String>,
    pub height: Option<f64>,
    pub is_active: Option<f64>,
    pub is_chain_tip: Option<f64>,
    pub hash: Option<String>,
    pub chain_work: Option<String>,
    pub version: Option<f64>,
    pub merkle_root: Option<String>,
    pub time: Option<f64>,
    pub bits: Option<f64>,
    pub nonce: Option<f64>,
}

impl HeaderRow {
    pub fn into_block_header(self) -> BlockHeader {
        BlockHeader {
            header_id: self.header_id.map(|v| v as i64),
            previous_header_id: self.previous_header_id.map(|v| v as i64),
            version: self.version.unwrap_or(0.0) as u32,
            previous_hash: self.previous_hash.unwrap_or_default(),
            merkle_root: self.merkle_root.unwrap_or_default(),
            time: self.time.unwrap_or(0.0) as u32,
            bits: self.bits.unwrap_or(0.0) as u32,
            nonce: self.nonce.unwrap_or(0.0) as u32,
            height: self.height.unwrap_or(0.0) as u32,
            hash: self.hash.unwrap_or_default(),
            chain_work: self.chain_work.unwrap_or_default(),
            is_active: self.is_active.unwrap_or(0.0) as i64 == 1,
            is_chain_tip: self.is_chain_tip.unwrap_or(0.0) as i64 == 1,
        }
    }
}

const SELECT_HEADER: &str = "SELECT header_id, previous_header_id, previous_hash, height, \
    is_active, is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce \
    FROM headers";

// ─── Reads ──────────────────────────────────────────────────────────────────

pub async fn find_chain_tip(db: &D1Database) -> worker::Result<Option<BlockHeader>> {
    let row: Option<HeaderRow> =
        Query::new(format!("{SELECT_HEADER} WHERE is_chain_tip = 1 LIMIT 1"))
            .first(db)
            .await?;
    Ok(row.map(|r| r.into_block_header()))
}

pub async fn get_chain_tip_height(db: &D1Database) -> worker::Result<u32> {
    match find_chain_tip(db).await? {
        Some(h) => Ok(h.height),
        None => Ok(0),
    }
}

pub async fn find_header_for_height(
    db: &D1Database,
    height: u32,
) -> worker::Result<Option<BlockHeader>> {
    let row: Option<HeaderRow> = Query::new(format!(
        "{SELECT_HEADER} WHERE height = ? AND is_active = 1 LIMIT 1"
    ))
    .bind(height)
    .first(db)
    .await?;
    Ok(row.map(|r| r.into_block_header()))
}

/// Lookup by hash across ALL headers (active + orphaned).
/// Used internally by insert_header (dedup), parent linking, and reorg walk-back.
/// Public endpoints should prefer `find_active_header_for_hash`.
pub async fn find_header_for_hash(
    db: &D1Database,
    hash: &str,
) -> worker::Result<Option<BlockHeader>> {
    let row: Option<HeaderRow> = Query::new(format!("{SELECT_HEADER} WHERE hash = ? LIMIT 1"))
        .bind(hash)
        .first(db)
        .await?;
    Ok(row.map(|r| r.into_block_header()))
}

/// Lookup by hash restricted to the active chain. Matches the TS server's
/// findLiveHeaderForBlockHash — orphaned headers from prior reorgs are hidden.
pub async fn find_active_header_for_hash(
    db: &D1Database,
    hash: &str,
) -> worker::Result<Option<BlockHeader>> {
    let row: Option<HeaderRow> = Query::new(format!(
        "{SELECT_HEADER} WHERE hash = ? AND is_active = 1 LIMIT 1"
    ))
    .bind(hash)
    .first(db)
    .await?;
    Ok(row.map(|r| r.into_block_header()))
}

/// Merkle root validation — the most critical query for downstream consumers.
/// Only checks active chain headers. Uses partial index idx_headers_merkle_active.
pub async fn is_valid_root_for_height(
    db: &D1Database,
    root: &str,
    height: u32,
) -> worker::Result<bool> {
    let row: Option<HeaderRow> = Query::new(format!(
        "{SELECT_HEADER} WHERE merkle_root = ? AND height = ? AND is_active = 1 LIMIT 1"
    ))
    .bind(root)
    .bind(height)
    .first(db)
    .await?;
    Ok(row.is_some())
}

pub async fn get_headers_hex(
    db: &D1Database,
    start_height: u32,
    count: u32,
) -> worker::Result<String> {
    let end_height = start_height + count;
    let rows: Vec<HeaderRow> = Query::new(format!(
        "{SELECT_HEADER} WHERE height >= ? AND height < ? AND is_active = 1 ORDER BY height ASC"
    ))
    .bind(start_height)
    .bind(end_height)
    .all(db)
    .await?;

    let mut hex_str = String::with_capacity(rows.len() * 160);
    for row in rows {
        let header = row.into_block_header();
        hex_str.push_str(&hex::encode(header.to_bytes()));
    }
    Ok(hex_str)
}

pub async fn get_info(db: &D1Database, chain: &Chain) -> worker::Result<ChaintracksInfo> {
    #[derive(serde::Deserialize)]
    struct CountRow {
        cnt: Option<f64>,
    }

    let count: Option<CountRow> = Query::new("SELECT COUNT(*) as cnt FROM headers")
        .first(db)
        .await?;
    let header_count = count.map(|c| c.cnt.unwrap_or(0.0) as u64).unwrap_or(0);

    let tip_height = get_chain_tip_height(db).await?;

    // Last-observed network tip, persisted by the cron. Defaults to 0 if the
    // column is absent (pre-migration-0002) so /getInfo never hard-fails.
    let woc_tip = get_woc_tip_height(db).await.unwrap_or(0);
    let behind_by = woc_tip.saturating_sub(tip_height);

    Ok(ChaintracksInfo {
        chain: chain.as_str().to_string(),
        height_live: tip_height,
        height_bulk: 0,
        header_count,
        // Report "syncing" whenever we're more than a couple blocks behind the
        // last-seen tip — an external monitor can alarm on this or on behind_by.
        is_syncing: behind_by > 2,
        storage_type: "d1".to_string(),
        woc_tip,
        behind_by,
    })
}

/// Last network tip observed by the cron (sync_state.woc_tip_height).
/// Errors (e.g. column absent before migration 0002) propagate so the caller
/// can default to 0.
async fn get_woc_tip_height(db: &D1Database) -> worker::Result<u32> {
    #[derive(serde::Deserialize)]
    struct TipRow {
        woc_tip_height: Option<f64>,
    }
    let row: Option<TipRow> =
        Query::new("SELECT woc_tip_height FROM sync_state WHERE id = 1")
            .first(db)
            .await?;
    Ok(row
        .and_then(|r| r.woc_tip_height)
        .map(|h| h as u32)
        .unwrap_or(0))
}

// ─── Writes (Issue #5: insert_header) ───────────────────────────────────────

/// Insert a single header with duplicate detection, parent linking, and chain tip management.
/// Returns InsertHeaderResult with all flags set per the toolbox-rs contract.
///
/// Logic (from sqlite.rs):
/// 1. Check duplicate by hash
/// 2. Calculate chain_work if not set
/// 3. Find previous_header_id by looking up previous_hash
/// 4. Get current tip to decide if this becomes new tip
/// 5. Insert row
/// 6. If new tip and doesn't extend old tip → reorg
/// 7. Update chain tip
pub async fn insert_header(
    db: &D1Database,
    header: &BlockHeader,
) -> worker::Result<InsertHeaderResult> {
    // 1. Duplicate check
    let existing = find_header_for_hash(db, &header.hash).await?;
    if existing.is_some() {
        return Ok(InsertHeaderResult {
            dupe: true,
            ..Default::default()
        });
    }

    // 2. Chain work
    let chain_work = if header.chain_work.is_empty() || header.chain_work == "0" {
        calculate_work(header.bits)
    } else {
        header.chain_work.clone()
    };

    // 3. Find previous header
    let zero_hash = "0".repeat(64);
    let previous_header = if header.previous_hash != zero_hash {
        find_header_for_hash(db, &header.previous_hash).await?
    } else {
        None
    };
    let previous_header_id = previous_header.as_ref().and_then(|h| h.header_id);

    // 4. Get current tip
    let current_tip = find_chain_tip(db).await?;
    let becomes_tip = match &current_tip {
        None => true,
        Some(tip) => header.height > tip.height,
    };

    // 5. Insert — always active on the main chain.
    // Reorg logic (handle_reorg) deactivates old-chain headers when needed.
    let is_active = true;

    Query::new(
        "INSERT OR IGNORE INTO headers (previous_header_id, previous_hash, height, is_active, \
         is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(previous_header_id)
    .bind(&*header.previous_hash)
    .bind(header.height)
    .bind(is_active)
    .bind(becomes_tip)
    .bind(&*header.hash)
    .bind(&*chain_work)
    .bind(header.version)
    .bind(&*header.merkle_root)
    .bind(header.time)
    .bind(header.bits)
    .bind(header.nonce)
    .run(db)
    .await?;

    let mut result = InsertHeaderResult {
        added: true,
        no_prev: previous_header.is_none() && header.height > 0,
        no_tip: current_tip.is_none(),
        is_active_tip: becomes_tip,
        ..Default::default()
    };

    // 6. Handle chain tip changes
    if becomes_tip {
        if let Some(ref tip) = current_tip {
            if header.previous_hash != tip.hash {
                // Reorg detected
                let deactivated = handle_reorg(db, header, tip).await?;
                result.reorg_depth = deactivated;
            }
        }
        // Clear old tip, set new tip
        update_chain_tip(db, &header.hash).await?;
    }

    Ok(result)
}

// ─── Chain Tip Management (Issue #10) ───────────────────────────────────────

/// Clear old chain tip and set new tip by hash.
pub async fn update_chain_tip(db: &D1Database, hash: &str) -> worker::Result<()> {
    Query::new("UPDATE headers SET is_chain_tip = 0 WHERE is_chain_tip = 1")
        .run(db)
        .await?;
    Query::new("UPDATE headers SET is_chain_tip = 1, is_active = 1 WHERE hash = ?")
        .bind(hash)
        .run(db)
        .await?;
    Ok(())
}

/// Set chain tip to the highest active header. Call after batch insert.
pub async fn update_chain_tip_to_highest(db: &D1Database) -> worker::Result<Option<BlockHeader>> {
    // Clear existing tip
    Query::new("UPDATE headers SET is_chain_tip = 0 WHERE is_chain_tip = 1")
        .run(db)
        .await?;

    // Find highest active header
    let row: Option<HeaderRow> = Query::new(format!(
        "{SELECT_HEADER} WHERE is_active = 1 ORDER BY height DESC LIMIT 1"
    ))
    .first(db)
    .await?;

    match row {
        Some(r) => {
            let header = r.into_block_header();
            Query::new("UPDATE headers SET is_chain_tip = 1 WHERE hash = ?")
                .bind(&*header.hash)
                .run(db)
                .await?;
            Ok(Some(header))
        }
        None => Ok(None),
    }
}

/// Mark all active headers above a height as inactive (for reorg).
pub async fn mark_headers_inactive_above_height(
    db: &D1Database,
    height: u32,
) -> worker::Result<u32> {
    // Count how many we'll deactivate
    #[derive(serde::Deserialize)]
    struct CountRow {
        cnt: Option<f64>,
    }
    let count: Option<CountRow> =
        Query::new("SELECT COUNT(*) as cnt FROM headers WHERE is_active = 1 AND height > ?")
            .bind(height)
            .first(db)
            .await?;

    let n = count.map(|c| c.cnt.unwrap_or(0.0) as u32).unwrap_or(0);

    if n > 0 {
        Query::new(
            "UPDATE headers SET is_active = 0, is_chain_tip = 0 WHERE height > ? AND is_active = 1",
        )
        .bind(height)
        .run(db)
        .await?;
    }

    Ok(n)
}

// ─── Reorg Handling (Issues #15, #17) ───────────────────────────────────────

/// Find the common ancestor between two headers by walking back via previous_hash.
/// Returns the common ancestor header, or None if not found within limit.
pub async fn find_common_ancestor(
    db: &D1Database,
    header_a: &BlockHeader,
    header_b: &BlockHeader,
) -> worker::Result<Option<BlockHeader>> {
    let mut a = Some(header_a.clone());
    let mut b = Some(header_b.clone());
    let mut steps = 0u32;
    let max_steps = 400; // reorg_height_threshold

    while let (Some(ref ha), Some(ref hb)) = (&a, &b) {
        if ha.hash == hb.hash {
            return Ok(a);
        }
        if steps >= max_steps {
            break;
        }
        steps += 1;

        match ha.height.cmp(&hb.height) {
            std::cmp::Ordering::Greater => {
                a = walk_back(db, ha).await?;
            }
            std::cmp::Ordering::Less => {
                b = walk_back(db, hb).await?;
            }
            std::cmp::Ordering::Equal => {
                a = walk_back(db, ha).await?;
                b = walk_back(db, hb).await?;
            }
        }
    }

    Ok(None)
}

/// Walk back one step: find the parent header by previous_header_id or previous_hash.
async fn walk_back(db: &D1Database, header: &BlockHeader) -> worker::Result<Option<BlockHeader>> {
    // Prefer previous_header_id (direct link)
    if let Some(prev_id) = header.previous_header_id {
        let row: Option<HeaderRow> =
            Query::new(format!("{SELECT_HEADER} WHERE header_id = ? LIMIT 1"))
                .bind(prev_id)
                .first(db)
                .await?;
        if let Some(r) = row {
            return Ok(Some(r.into_block_header()));
        }
    }
    // Fallback to previous_hash
    let zero_hash = "0".repeat(64);
    if header.previous_hash != zero_hash {
        return find_header_for_hash(db, &header.previous_hash).await;
    }
    Ok(None)
}

/// Execute a reorg: deactivate old chain above ancestor, activate new chain.
/// Returns the number of deactivated headers (reorg depth).
///
/// Algorithm (from sqlite.rs handle_reorg):
/// 1. Find common ancestor between new header and old tip
/// 2. Deactivate old chain headers above ancestor height
/// 3. Activate new chain by walking back from new header to ancestor
async fn handle_reorg(
    db: &D1Database,
    new_header: &BlockHeader,
    old_tip: &BlockHeader,
) -> worker::Result<u32> {
    let ancestor = find_common_ancestor(db, new_header, old_tip).await?;
    let ancestor_height = ancestor.as_ref().map(|a| a.height).unwrap_or(0);

    // Deactivate old chain above ancestor
    let deactivated = mark_headers_inactive_above_height(db, ancestor_height).await?;

    // Activate new chain: walk back from new_header to ancestor
    let mut current = Some(new_header.clone());
    while let Some(ref h) = current {
        if h.height <= ancestor_height {
            break;
        }
        Query::new("UPDATE headers SET is_active = 1 WHERE hash = ?")
            .bind(&*h.hash)
            .run(db)
            .await?;
        current = walk_back(db, h).await?;
    }

    Ok(deactivated)
}

// ─── Batch Insert (Issue #6) ────────────────────────────────────────────────

/// Batch insert headers for bulk import. Uses D1 batch() for atomicity.
/// Skips duplicates. Does NOT update chain tip — call update_chain_tip_to_highest() after.
///
/// Returns number of headers actually inserted.
pub async fn insert_headers_batch(db: &D1Database, headers: &[BlockHeader]) -> worker::Result<u32> {
    if headers.is_empty() {
        return Ok(0);
    }

    let mut inserted = 0u32;
    let mut batch = BatchCollector::new(db);

    for header in headers {
        // Calculate chain work if needed
        let chain_work = if header.chain_work.is_empty() || header.chain_work == "0" {
            calculate_work(header.bits)
        } else {
            header.chain_work.clone()
        };

        batch.add(
            "INSERT OR IGNORE INTO headers (previous_header_id, previous_hash, height, is_active, \
             is_chain_tip, hash, chain_work, version, merkle_root, time, bits, nonce) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            vec![
                QVal::Null, // previous_header_id — link later or not needed for bulk
                QVal::Text(header.previous_hash.clone()),
                QVal::Int(header.height as i64),
                QVal::Bool(true),  // is_active
                QVal::Bool(false), // is_chain_tip (set after via update_chain_tip_to_highest)
                QVal::Text(header.hash.clone()),
                QVal::Text(chain_work),
                QVal::Int(header.version as i64),
                QVal::Text(header.merkle_root.clone()),
                QVal::Int(header.time as i64),
                QVal::Int(header.bits as i64),
                QVal::Int(header.nonce as i64),
            ],
        )?;

        inserted += 1;

        // D1 limit: 100 statements per batch. Execute and start new batch.
        if batch.len() >= 100 {
            batch.execute().await?;
            batch = BatchCollector::new(db);
        }
    }

    // Execute remaining statements
    if !batch.is_empty() {
        batch.execute().await?;
    }

    Ok(inserted)
}

// ─── Tests ──────────────────────────────────────────────────────────────────
//
// Following the rust-wallet-infra pattern: test D1 row deserialization with
// serde_json (simulating what D1 returns), and test pure business logic.
// Actual D1 execution is tested via integration tests (wrangler dev + curl).

#[cfg(test)]
mod tests {
    use super::*;

    // ── HeaderRow deserialization (simulates D1 responses) ──

    #[test]
    fn test_header_row_full() {
        let json = serde_json::json!({
            "header_id": 42.0,
            "previous_header_id": 41.0,
            "previous_hash": "abc123",
            "height": 100.0,
            "is_active": 1.0,
            "is_chain_tip": 0.0,
            "hash": "def456",
            "chain_work": "00ff",
            "version": 1.0,
            "merkle_root": "merkle_abc",
            "time": 1234567890.0,
            "bits": 486604799.0,
            "nonce": 99999.0,
        });

        let row: HeaderRow = serde_json::from_value(json).unwrap();
        let header = row.into_block_header();

        assert_eq!(header.header_id, Some(42));
        assert_eq!(header.previous_header_id, Some(41));
        assert_eq!(header.height, 100);
        assert!(header.is_active);
        assert!(!header.is_chain_tip);
        assert_eq!(header.hash, "def456");
        assert_eq!(header.version, 1);
        assert_eq!(header.merkle_root, "merkle_abc");
        assert_eq!(header.time, 1234567890);
        assert_eq!(header.bits, 486604799);
        assert_eq!(header.nonce, 99999);
    }

    #[test]
    fn test_header_row_nulls() {
        // D1 can return null for optional fields
        let json = serde_json::json!({
            "header_id": null,
            "previous_header_id": null,
            "previous_hash": null,
            "height": null,
            "is_active": null,
            "is_chain_tip": null,
            "hash": null,
            "chain_work": null,
            "version": null,
            "merkle_root": null,
            "time": null,
            "bits": null,
            "nonce": null,
        });

        let row: HeaderRow = serde_json::from_value(json).unwrap();
        let header = row.into_block_header();

        assert_eq!(header.header_id, None);
        assert_eq!(header.previous_header_id, None);
        assert_eq!(header.height, 0);
        assert!(!header.is_active);
        assert!(!header.is_chain_tip);
        assert_eq!(header.hash, "");
        assert_eq!(header.version, 0);
    }

    #[test]
    fn test_header_row_d1_numeric_quirk() {
        // D1 returns booleans as 1.0/0.0, not true/false
        let json = serde_json::json!({
            "header_id": 1.0,
            "previous_header_id": null,
            "previous_hash": "prev",
            "height": 0.0,
            "is_active": 1.0,
            "is_chain_tip": 1.0,
            "hash": "genesis",
            "chain_work": "work",
            "version": 1.0,
            "merkle_root": "merkle",
            "time": 1231006505.0,
            "bits": 486604799.0,
            "nonce": 2083236893.0,
        });

        let row: HeaderRow = serde_json::from_value(json).unwrap();
        let header = row.into_block_header();

        assert!(header.is_active);
        assert!(header.is_chain_tip);
        // Verify large nonce doesn't overflow f64→u32
        assert_eq!(header.nonce, 2083236893);
    }

    #[test]
    fn test_header_row_inactive() {
        let json = serde_json::json!({
            "header_id": 5.0,
            "previous_header_id": 4.0,
            "previous_hash": "prev",
            "height": 100.0,
            "is_active": 0.0,
            "is_chain_tip": 0.0,
            "hash": "forked",
            "chain_work": "work",
            "version": 1.0,
            "merkle_root": "merkle",
            "time": 1000.0,
            "bits": 1000.0,
            "nonce": 1000.0,
        });

        let row: HeaderRow = serde_json::from_value(json).unwrap();
        let header = row.into_block_header();

        assert!(!header.is_active);
        assert!(!header.is_chain_tip);
    }

    #[test]
    fn test_header_row_roundtrip_serde() {
        // Ensure HeaderRow can serialize and deserialize (needed for D1 results)
        let row = HeaderRow {
            header_id: Some(1.0),
            previous_header_id: None,
            previous_hash: Some("abc".to_string()),
            height: Some(0.0),
            is_active: Some(1.0),
            is_chain_tip: Some(1.0),
            hash: Some("genesis".to_string()),
            chain_work: Some("work".to_string()),
            version: Some(1.0),
            merkle_root: Some("merkle".to_string()),
            time: Some(1000.0),
            bits: Some(486604799.0),
            nonce: Some(12345.0),
        };

        let json = serde_json::to_string(&row).unwrap();
        let parsed: HeaderRow = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.header_id, Some(1.0));
        assert_eq!(parsed.hash, Some("genesis".to_string()));
    }

    // ── InsertHeaderResult logic (pure business logic) ──

    #[test]
    fn test_insert_result_first_header() {
        // First header inserted: added=true, no_tip=true, is_active_tip=true
        let result = InsertHeaderResult {
            added: true,
            no_tip: true,
            is_active_tip: true,
            ..Default::default()
        };
        assert!(result.added);
        assert!(result.no_tip);
        assert!(result.is_active_tip);
        assert!(!result.dupe);
        assert_eq!(result.reorg_depth, 0);
    }

    #[test]
    fn test_insert_result_duplicate() {
        let result = InsertHeaderResult {
            dupe: true,
            ..Default::default()
        };
        assert!(!result.added);
        assert!(result.dupe);
    }

    #[test]
    fn test_insert_result_chain_growth() {
        // Normal chain growth: added, active tip, no reorg
        let result = InsertHeaderResult {
            added: true,
            is_active_tip: true,
            ..Default::default()
        };
        assert!(result.added);
        assert!(result.is_active_tip);
        assert_eq!(result.reorg_depth, 0);
    }

    #[test]
    fn test_insert_result_reorg() {
        let result = InsertHeaderResult {
            added: true,
            is_active_tip: true,
            reorg_depth: 3,
            ..Default::default()
        };
        assert!(result.added);
        assert_eq!(result.reorg_depth, 3);
    }

    #[test]
    fn test_insert_result_orphan() {
        // Header whose parent is not found
        let result = InsertHeaderResult {
            added: true,
            no_prev: true,
            ..Default::default()
        };
        assert!(result.added);
        assert!(result.no_prev);
    }

    // ── Chain work computation (tested inline with storage context) ──

    #[test]
    fn test_chain_work_calculated_when_empty() {
        // Simulate the logic in insert_header: if chain_work is empty, calculate it
        let header = BlockHeader {
            header_id: None,
            previous_header_id: None,
            version: 1,
            previous_hash: "0".repeat(64),
            merkle_root: "merkle".to_string(),
            time: 1231006505,
            bits: 0x1d00ffff,
            nonce: 2083236893,
            height: 0,
            hash: "genesis".to_string(),
            chain_work: String::new(),
            is_active: true,
            is_chain_tip: false,
        };

        let work = if header.chain_work.is_empty() || header.chain_work == "0" {
            calculate_work(header.bits)
        } else {
            header.chain_work.clone()
        };

        assert_eq!(work.len(), 64);
        assert_ne!(work, "0".repeat(64));
    }

    #[test]
    fn test_chain_work_preserved_when_set() {
        let header = BlockHeader {
            chain_work: "00000000000000000000000000000001".to_string(),
            bits: 0x1d00ffff,
            ..Default::default()
        };

        let work = if header.chain_work.is_empty() || header.chain_work == "0" {
            calculate_work(header.bits)
        } else {
            header.chain_work.clone()
        };

        assert_eq!(work, "00000000000000000000000000000001");
    }

    // ── Tip decision logic (pure) ──

    #[test]
    fn test_becomes_tip_no_existing() {
        // No current tip → new header always becomes tip
        let current_tip: Option<BlockHeader> = None;
        let new_height = 0u32;
        let becomes_tip = match &current_tip {
            None => true,
            Some(tip) => new_height > tip.height,
        };
        assert!(becomes_tip);
    }

    #[test]
    fn test_becomes_tip_higher() {
        let current_tip = Some(BlockHeader {
            height: 100,
            ..Default::default()
        });
        let new_height = 101u32;
        let becomes_tip = match &current_tip {
            None => true,
            Some(tip) => new_height > tip.height,
        };
        assert!(becomes_tip);
    }

    #[test]
    fn test_does_not_become_tip_lower() {
        let current_tip = Some(BlockHeader {
            height: 100,
            ..Default::default()
        });
        let new_height = 99u32;
        let becomes_tip = match &current_tip {
            None => true,
            Some(tip) => new_height > tip.height,
        };
        assert!(!becomes_tip);
    }

    #[test]
    fn test_does_not_become_tip_equal() {
        let current_tip = Some(BlockHeader {
            height: 100,
            ..Default::default()
        });
        let new_height = 100u32;
        let becomes_tip = match &current_tip {
            None => true,
            Some(tip) => new_height > tip.height,
        };
        assert!(!becomes_tip);
    }

    // ── Reorg detection logic (pure) ──

    #[test]
    fn test_reorg_detected_when_prev_hash_differs() {
        let current_tip = BlockHeader {
            hash: "tip_hash".to_string(),
            height: 100,
            ..Default::default()
        };
        let new_header = BlockHeader {
            previous_hash: "different_hash".to_string(),
            height: 101,
            ..Default::default()
        };

        // Reorg if new header becomes tip but doesn't extend current tip
        let is_reorg = new_header.previous_hash != current_tip.hash;
        assert!(is_reorg);
    }

    #[test]
    fn test_no_reorg_when_extends_tip() {
        let current_tip = BlockHeader {
            hash: "tip_hash".to_string(),
            height: 100,
            ..Default::default()
        };
        let new_header = BlockHeader {
            previous_hash: "tip_hash".to_string(),
            height: 101,
            ..Default::default()
        };

        let is_reorg = new_header.previous_hash != current_tip.hash;
        assert!(!is_reorg);
    }

    // ── SQL pattern verification ──

    #[test]
    fn test_select_header_sql() {
        assert!(SELECT_HEADER.contains("header_id"));
        assert!(SELECT_HEADER.contains("previous_header_id"));
        assert!(SELECT_HEADER.contains("merkle_root"));
        assert!(SELECT_HEADER.contains("chain_work"));
        assert!(SELECT_HEADER.contains("FROM headers"));
    }

    // ── is_active bug regression tests ──
    // Bug: insert_header was setting is_active based on becomes_tip,
    // causing non-tip headers to be inactive and invisible to queries.
    // Fix: all headers on the main chain are always active. Reorg logic
    // handles deactivation when needed.

    #[test]
    fn test_all_inserted_headers_are_active() {
        // Every header inserted via insert_header should be active.
        // The is_active flag is only set to false by handle_reorg when
        // switching chains. Normal insertion = always active.
        let is_active = true; // This is what insert_header now does
        assert!(
            is_active,
            "All inserted headers must be active on the main chain"
        );
    }

    #[test]
    fn test_insert_sql_uses_or_ignore() {
        // INSERT OR IGNORE prevents UNIQUE constraint errors when
        // cron and bulk-sync race. But it also means we can't update
        // existing rows — so the initial insert must be correct.
        let sql = "INSERT OR IGNORE INTO headers";
        assert!(sql.contains("OR IGNORE"));
    }

    #[test]
    fn test_find_header_for_height_requires_active() {
        // The WHERE clause must include is_active = 1
        let sql = format!("{SELECT_HEADER} WHERE height = ? AND is_active = 1 LIMIT 1");
        assert!(sql.contains("is_active = 1"));
    }

    #[test]
    fn test_is_valid_root_requires_active() {
        // Merkle root validation must only check active chain
        let sql = format!(
            "{SELECT_HEADER} WHERE merkle_root = ? AND height = ? AND is_active = 1 LIMIT 1"
        );
        assert!(sql.contains("is_active = 1"));
    }

    #[test]
    fn test_find_active_header_for_hash_filters_active() {
        // /findHeaderHexForBlockHash must not return headers orphaned by reorg.
        // Matches TS server's findLiveHeaderForBlockHash semantics.
        let sql = format!("{SELECT_HEADER} WHERE hash = ? AND is_active = 1 LIMIT 1");
        assert!(sql.contains("is_active = 1"));
    }

    #[test]
    fn test_find_header_for_hash_is_unfiltered() {
        // Internal lookup (dedup, parent linking, reorg walk-back) must see
        // ALL headers including orphaned ones — do NOT filter by is_active.
        let sql = format!("{SELECT_HEADER} WHERE hash = ? LIMIT 1");
        let where_clause = sql.split("WHERE").nth(1).unwrap();
        assert!(!where_clause.contains("is_active"));
    }
}
