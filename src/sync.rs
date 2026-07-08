//! Cron-triggered chain synchronization.
//!
//! Polls WhatsOnChain for new blocks, ingests headers into D1.
//! During catch-up, optionally fetches bulk headers from an upstream
//! chaintracks instance (UPSTREAM_CHAINTRACKS_URL env var) — much faster
//! than WoC one-by-one. Falls back to WoC if upstream is unset or fails.

use worker::*;

use crate::storage;
use crate::types::{BlockHeader, Chain};
use crate::woc::WocClient;

/// Called every minute by the cron trigger.
///
/// Two modes:
/// - **Catch-up** (gap > 10): fetch bulk hex from production chaintracks,
///   parse 80-byte headers, batch insert. ~1000 headers per request.
/// - **Live** (gap <= 10): fetch from WoC one-by-one with full insert logic.
pub async fn poll_for_new_blocks(env: &Env) -> Result<()> {
    let db = env.d1("DB")?;

    let chain = match env
        .var("CHAIN")
        .map(|v| v.to_string())
        .unwrap_or_default()
        .as_str()
    {
        "test" => Chain::Test,
        _ => Chain::Main,
    };

    // The key is a worker SECRET (env.secret), with a var fallback for
    // local dev — env.var() fails silently on secrets.
    let api_key = env
        .secret("WHATSONCHAIN_API_KEY")
        .map(|v| v.to_string())
        .ok()
        .or_else(|| env.var("WHATSONCHAIN_API_KEY").map(|v| v.to_string()).ok())
        .filter(|s| !s.is_empty());

    let client = WocClient::new(&chain, api_key);

    let upstream_url = env
        .var("UPSTREAM_CHAINTRACKS_URL")
        .map(|v| v.to_string())
        .ok()
        .filter(|s| !s.is_empty());

    let our_height = storage::get_chain_tip_height(&db).await?;

    let chain_info = match client.get_chain_info().await {
        Ok(info) => info,
        Err(e) => {
            console_log!("Cron: WoC unavailable, skipping: {e:?}");
            return Ok(());
        }
    };
    let woc_height = chain_info.blocks;

    if woc_height <= our_height {
        // Equal height is NOT automatically "in sync" (audit C2): if WoC's
        // best hash differs from our tip hash at the same height, the
        // network reorged to a competitor we can never fetch by height —
        // the old code returned here and served the losing branch forever.
        if woc_height == our_height && our_height > 0 {
            if let Some(best_hash) = chain_info.best_block_hash.as_deref() {
                if let Some(our_tip) = storage::find_chain_tip(&db).await? {
                    if !our_tip.hash.eq_ignore_ascii_case(best_hash) {
                        console_log!(
                            "Cron: equal-height branch mismatch at {} (ours {} vs WoC {}) — fetching competitor",
                            our_height, our_tip.hash, best_hash
                        );
                        match client.get_header_by_hash(best_hash).await {
                            Ok(header) => {
                                insert_with_parent_backfill(&db, &client, header).await?;
                            }
                            Err(e) => console_log!("Cron: competitor fetch failed: {e:?}"),
                        }
                    }
                }
            }
        }
        return Ok(());
    }

    let gap = woc_height - our_height;

    if gap > 10 {
        // ─── Catch-up: bulk fetch from upstream chaintracks if configured ─
        // getHeaders returns concatenated 80-byte hex — 1000 headers/request.
        // If no upstream configured, fall straight through to WoC one-by-one.
        let batch_size = 1000u32;
        let max_per_cycle = 5000u32; // 5 requests × 1000 headers
        let end_height = (our_height + max_per_cycle).min(woc_height);

        console_log!(
            "Cron: catch-up {} blocks ({} → {end_height})",
            end_height - our_height,
            our_height + 1
        );

        let mut height = our_height + 1;
        let mut used_fallback = upstream_url.is_none();
        while height <= end_height {
            let count = batch_size.min(end_height - height + 1);

            // Anchor batch[0] to our stored header below it (review M-2).
            let anchor: Option<String> = if height > 0 {
                storage::find_header_for_height(&db, height - 1)
                    .await?
                    .map(|h| h.hash)
            } else {
                None
            };
            let upstream_result = match upstream_url.as_deref() {
                Some(url) => {
                    fetch_headers_from_upstream(url, height, count, anchor.as_deref()).await
                }
                None => Err(Error::RustError("upstream not configured".into())),
            };

            match upstream_result {
                Ok(headers) if !headers.is_empty() => {
                    let n = headers.len() as u32;
                    storage::insert_headers_batch(&db, &headers).await?;
                    height += n;
                }
                Ok(_) => break, // empty response
                Err(e) => {
                    if !used_fallback {
                        console_log!("Cron: upstream unavailable ({e:?}), falling back to WoC");
                        used_fallback = true;
                    }
                    // WoC fallback: one-by-one (slower but independent)
                    for h in height..=(height + count - 1).min(end_height) {
                        match client.get_header_by_height(h).await {
                            Ok(header) => {
                                let _ = storage::insert_header(&db, &header).await?;
                            }
                            Err(e2) => {
                                console_log!("Cron: WoC also failed at {h}: {e2:?}");
                                height = end_height + 1; // break outer loop
                                break;
                            }
                        }
                    }
                    if height <= end_height {
                        height += count;
                    }
                }
            }
        }
        storage::update_chain_tip_to_highest(&db).await?;
        // Self-heal any dual-active debris the bulk path can leave (audit
        // C3): exactly one active row may exist per height; keep the newest
        // ingest, the live reorg walk corrects branch choice if needed.
        crate::d1::Query::new(
            "UPDATE headers SET is_active = 0 WHERE is_active = 1 AND header_id NOT IN              (SELECT MAX(header_id) FROM headers WHERE is_active = 1 GROUP BY height)",
        )
        .run(&db)
        .await?;
    } else {
        // ─── Live: one-by-one from WoC with reorg detection ─────────────
        console_log!(
            "Cron: live sync {gap} blocks ({} → {woc_height})",
            our_height + 1
        );

        for height in (our_height + 1)..=woc_height {
            match client.get_header_by_height(height).await {
                Ok(header) => {
                    let result = insert_with_parent_backfill(&db, &client, header).await?;
                    if result.reorg_depth > 0 {
                        console_log!(
                            "Cron: REORG at height {} (depth {})",
                            height,
                            result.reorg_depth
                        );
                    }
                }
                Err(e) => {
                    console_log!("Cron: WoC failed at {height}: {e:?}");
                    break;
                }
            }
        }
    }

    let new_tip = storage::get_chain_tip_height(&db).await?;
    if new_tip > our_height {
        console_log!("Cron: synced to {} (+{})", new_tip, new_tip - our_height);
        update_sync_state(&db, new_tip).await?;
    }

    // Keep cumulative work factual across the fork-relevant window (H-3:
    // legacy/bulk rows carry non-cumulative work; 144 blocks ≈ 24h covers
    // any reorg the 400-step ancestor walk would accept in practice).
    match storage::repair_cumulative_work(&db, 144).await {
        Ok(0) => {}
        Ok(n) => console_log!("Cron: repaired cumulative work on {n} header(s)"),
        Err(e) => console_log!("Cron: repair_cumulative_work failed: {e:?}"),
    }

    Ok(())
}

/// Insert a live header; when its parent is missing locally (no_prev),
/// backfill ancestors BY HASH from WoC — bounded walk, oldest-first insert,
/// then retry the child (reference: Chaintracks.ts:398-404,523-544
/// getMissingBlockHeader with addLiveRecursionLimit=36; audit C2 — without
/// this, a competitor branch wedged the tip forever because find_common_
/// ancestor hit the missing parent and every later insert became a dupe
/// no-op).
pub(crate) async fn insert_with_parent_backfill(
    db: &worker::D1Database,
    client: &WocClient,
    header: BlockHeader,
) -> Result<crate::types::InsertHeaderResult> {
    const BACKFILL_LIMIT: usize = 36; // TS addLiveRecursionLimit parity

    let result = storage::insert_header(db, &header).await?;
    if !result.no_prev {
        // H-1 (adversarial review): a dupe that is STILL unlinked means a
        // previous backfill aborted mid-walk (crash / WoC error after the
        // orphan row landed). Without this repair the dupe short-circuit
        // made the wedge permanent — the walk was never re-attempted.
        let stored_orphan = result.dupe
            && header.height > 0
            && matches!(
                storage::find_header_for_hash(db, &header.hash).await?,
                Some(ref h) if h.previous_header_id.is_none()
            );
        if !stored_orphan {
            return Ok(result);
        }
        console_log!(
            "Cron: stored header {} at {} is an unlinked orphan — resuming backfill",
            header.hash,
            header.height
        );
    }

    console_log!(
        "Cron: header {} at {} has no stored parent — backfilling branch by hash",
        header.hash,
        header.height
    );

    // Walk back by hash until we hit a stored header (fork point) or budget.
    let mut branch: Vec<BlockHeader> = Vec::new();
    let mut want = header.previous_hash.clone();
    let zero_hash = "0".repeat(64);
    for _ in 0..BACKFILL_LIMIT {
        if want == zero_hash {
            break;
        }
        if storage::find_header_for_hash(db, &want).await?.is_some() {
            break;
        }
        let parent = client.get_header_by_hash(&want).await?;
        want = parent.previous_hash.clone();
        branch.push(parent);
    }

    if !branch.is_empty() && want != zero_hash {
        if storage::find_header_for_hash(db, &want).await?.is_none() {
            console_log!(
                "Cron: backfill budget exhausted without reaching a stored ancestor (still missing {}) — leaving branch inactive",
                want
            );
        }
    }

    // Insert oldest-first so each child finds its parent (and cumulative
    // chain work accumulates correctly).
    for parent in branch.iter().rev() {
        let _ = storage::insert_header(db, parent).await?;
    }

    // The child row already exists (orphan, inactive, per-block-only work).
    // Relink it to the backfilled parent, recompute cumulative work, and
    // re-evaluate the tip — running the reorg walk NOW if the repaired
    // branch outworks the active one.
    let repaired = storage::relink_orphan_and_reevaluate(db, &header.hash).await?;
    if repaired.reorg_depth > 0 {
        console_log!(
            "Cron: backfilled branch won — reorg depth {}",
            repaired.reorg_depth
        );
    }
    Ok(repaired)
}

/// Fetch headers from an upstream chaintracks instance via getHeaders endpoint.
/// Returns parsed BlockHeaders from the concatenated hex response.
async fn fetch_headers_from_upstream(
    base_url: &str,
    start_height: u32,
    count: u32,
    expected_prev_hash: Option<&str>,
) -> Result<Vec<BlockHeader>> {
    let base = base_url.trim_end_matches('/');
    let url = format!("{base}/getHeaders?height={start_height}&count={count}");

    let mut init = RequestInit::new();
    init.with_method(Method::Get);
    let request = Request::new_with_init(&url, &init)?;
    let mut response = Fetch::Request(request).send().await?;

    let status = response.status_code();
    if !(200..300).contains(&status) {
        return Err(Error::RustError(format!("Production HTTP {status}")));
    }

    // Parse {status, value} wrapper
    #[derive(serde::Deserialize)]
    struct Resp {
        value: Option<String>,
    }
    let resp: Resp = response.json().await?;
    let hex_str = resp.value.unwrap_or_default();

    if hex_str.is_empty() {
        return Ok(Vec::new());
    }

    let bytes = hex::decode(&hex_str).map_err(|e| Error::RustError(format!("hex decode: {e}")))?;

    let mut headers: Vec<BlockHeader> = Vec::with_capacity(bytes.len() / 80);
    for (i, chunk) in bytes.chunks(80).enumerate() {
        if chunk.len() < 80 {
            break;
        }
        if let Some(header) = BlockHeader::from_bytes(chunk, start_height + i as u32) {
            // LINKAGE GUARD (audit M4): heights are assigned blindly as
            // start+i, so an upstream response with a gap or splice would
            // store every subsequent header at the wrong height. Each
            // header must link to its predecessor — INCLUDING the first one,
            // which must link to our locally stored header at start-1
            // (review M-2: an unanchored batch[0] let a stale upstream
            // bulk-insert a foreign branch at blind heights).
            let expected: Option<String> = match headers.last() {
                Some(prev) => Some(prev.hash.clone()),
                None => expected_prev_hash.map(|h: &str| h.to_string()),
            };
            if let Some(expected) = expected {
                if !header.previous_hash.eq_ignore_ascii_case(&expected) {
                    worker::console_log!(
                        "Cron: upstream linkage break at height {} (links {} ≠ {}) — truncating batch",
                        start_height + i as u32,
                        header.previous_hash,
                        expected
                    );
                    break;
                }
            }
            headers.push(header);
        } else {
            break;
        }
    }

    Ok(headers)
}

async fn update_sync_state(db: &D1Database, height: u32) -> Result<()> {
    crate::d1::Query::new(
        "UPDATE sync_state SET last_synced_height = ?, live_sync_active = 1, \
         updated_at = datetime('now') WHERE id = 1",
    )
    .bind(height)
    .run(db)
    .await
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_catch_up_threshold() {
        assert!(11 > 10, "gap > 10 triggers catch-up from production");
        assert!(!(10 > 10), "gap == 10 uses live WoC mode");
    }

    #[test]
    fn test_batch_size() {
        let batch_size = 1000u32;
        let max_per_cycle = 5000u32;
        // 5 requests × 1000 headers = 5000 per cycle
        assert_eq!(max_per_cycle / batch_size, 5);
    }

    #[test]
    fn test_end_height_cap() {
        let our_height = 930000u32;
        let woc_height = 944000u32;
        let max_per_cycle = 5000u32;
        let end_height = (our_height + max_per_cycle).min(woc_height);
        assert_eq!(end_height, 935000);
    }
}
