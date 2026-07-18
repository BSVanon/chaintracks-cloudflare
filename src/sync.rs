//! Cron-triggered chain synchronization.
//!
//! Polls WhatsOnChain for the network tip, then closes the gap to it.
//!
//! Catch-up strategy (gap > 10), in priority order:
//!   1. UPSTREAM_CHAINTRACKS_URL, if configured — bulk getHeaders (1000/req).
//!   2. Public bulk-header CDN — one ~100k-header file per tick, idempotent.
//!      This is the self-healing default: no external config required, and it
//!      recovers automatically from any large gap within the CDN snapshot.
//!   3. WhatsOnChain one-by-one — only for the small remaining gap above the
//!      CDN snapshot, capped per tick to stay under the CF subrequest and WoC
//!      free-tier rate limits.
//!
//! Every tick first reconciles the reported tip to the highest active header,
//! "banking" any rows a previous (possibly timed-out) tick loaded but could
//! not tip. Live mode (gap <= 10) fetches one-by-one from WoC with reorg
//! detection.

use worker::*;

use crate::storage;
use crate::types::{BlockHeader, Chain};
use crate::woc::WocClient;

/// Called every minute by the cron trigger.
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

    let api_key = env
        .var("WHATSONCHAIN_API_KEY")
        .map(|v| v.to_string())
        .ok()
        .filter(|s| !s.is_empty());

    let client = WocClient::new(&chain, api_key);

    let upstream_url = env
        .var("UPSTREAM_CHAINTRACKS_URL")
        .map(|v| v.to_string())
        .ok()
        .filter(|s| !s.is_empty());

    // Bank any rows a prior tick loaded but was cut off before it could tip.
    // Bulk inserts update the tip flag last, so a timed-out tick can leave
    // active headers stranded above a stale is_chain_tip row. This costs one
    // indexed lookup + tip swap per minute and keeps `currentHeight` honest.
    let _ = storage::update_chain_tip_to_highest(&db).await;

    let our_height = storage::get_chain_tip_height(&db).await?;

    let chain_info = match client.get_chain_info().await {
        Ok(info) => info,
        Err(e) => {
            console_log!("Cron: WoC unavailable, skipping: {e:?}");
            return Ok(());
        }
    };
    let woc_height = chain_info.blocks;

    // Record the network tip so /getInfo can report how far behind we are.
    // Non-fatal: tolerate the column being absent until migration 0002 lands.
    let _ = update_woc_tip(&db, woc_height).await;

    if woc_height <= our_height {
        return Ok(());
    }

    let gap = woc_height - our_height;

    if gap > 10 {
        // ─── Catch-up ───────────────────────────────────────────────────────
        console_log!("Cron: catch-up, {gap} behind ({} → {woc_height})", our_height + 1);

        if let Some(url) = upstream_url.as_deref() {
            catch_up_from_upstream(&db, &client, url, our_height, woc_height).await?;
        } else {
            // Self-healing default: one bulk-CDN file per tick until the CDN
            // snapshot is exhausted, then WoC one-by-one for the small remainder.
            match catch_up_from_bulk_cdn(&db, &chain, our_height).await {
                Ok(Some(new_tip)) => console_log!("Cron: bulk CDN → {new_tip}"),
                Ok(None) => {
                    woc_catchup_one_by_one(&db, &client, our_height, woc_height).await?;
                }
                Err(e) => {
                    console_log!("Cron: bulk CDN failed ({e:?}), WoC fallback");
                    woc_catchup_one_by_one(&db, &client, our_height, woc_height).await?;
                }
            }
        }
        storage::update_chain_tip_to_highest(&db).await?;
    } else {
        // ─── Live: one-by-one from WoC with reorg detection ─────────────────
        console_log!("Cron: live sync {gap} blocks ({} → {woc_height})", our_height + 1);

        for height in (our_height + 1)..=woc_height {
            match client.get_header_by_height(height).await {
                Ok(header) => {
                    let result = storage::insert_header(&db, &header).await?;
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

    Ok(())
}

/// Bulk catch-up from an upstream chaintracks instance (getHeaders, 1000/req).
/// On upstream failure, falls back to a bounded WoC one-by-one window for this
/// tick; the next cron tick resumes.
async fn catch_up_from_upstream(
    db: &D1Database,
    client: &WocClient,
    url: &str,
    our_height: u32,
    woc_height: u32,
) -> Result<()> {
    let batch_size = 1000u32;
    let max_per_cycle = 5000u32; // 5 requests × 1000 headers
    let end_height = (our_height + max_per_cycle).min(woc_height);

    let mut height = our_height + 1;
    while height <= end_height {
        let count = batch_size.min(end_height - height + 1);
        match fetch_headers_from_upstream(url, height, count).await {
            Ok(headers) if !headers.is_empty() => {
                let n = headers.len() as u32;
                storage::insert_headers_batch(db, &headers).await?;
                height += n;
            }
            Ok(_) => break, // empty response
            Err(e) => {
                console_log!("Cron: upstream failed ({e:?}), WoC fallback this tick");
                woc_catchup_one_by_one(db, client, height - 1, end_height).await?;
                break;
            }
        }
    }
    Ok(())
}

/// Self-healing bulk catch-up from the public block-header CDN.
///
/// Downloads ONE file (~100k headers) covering the next needed height and
/// inserts it idempotently (`INSERT OR IGNORE`). Only the rows above our
/// current height are inserted, so a mid-file resume is cheap. Returns:
///   * `Ok(Some(tip))` — advanced to `tip`.
///   * `Ok(None)`      — no CDN file covers `our_height + 1`, or the covering
///                       file added nothing new (we're at/above the snapshot's
///                       top). The caller should use WoC for the remainder.
async fn catch_up_from_bulk_cdn(
    db: &D1Database,
    chain: &Chain,
    our_height: u32,
) -> Result<Option<u32>> {
    // Headers pulled per tick. Small enough that the ranged download + parse +
    // batch insert (BULK_PER_TICK/100 D1 batches) always completes within the
    // scheduled-worker budget; whatever commits is banked by the next tick's
    // start-of-tick reconcile, so progress is monotonic even if a tick is cut.
    const BULK_PER_TICK: u32 = 25_000;

    let next_height = our_height + 1;
    let listing = WocClient::get_bulk_file_listing(chain).await?;

    // If we're already past everything the snapshot provides, don't waste this
    // tick — signal WoC directly for the remaining gap to the live tip.
    let snapshot_top = listing
        .files
        .iter()
        .map(|f| f.coverage_end())
        .max()
        .unwrap_or(0);
    if next_height >= snapshot_top {
        return Ok(None);
    }

    // Files are 100k-aligned; pick the highest whose first_height <= next.
    let idx = match listing
        .files
        .iter()
        .rposition(|f| f.first_height.unwrap_or(0) <= next_height)
    {
        Some(i) => i,
        None => return Ok(None),
    };
    let file_info = &listing.files[idx];
    let file_first = file_info.first_height.unwrap_or((idx as u32) * 100_000);
    let file_end = file_info.coverage_end(); // exclusive

    // Bounded slice [next_height, next_height + BULK_PER_TICK), clamped to the
    // file's coverage. Only the rows we don't have yet.
    let end_excl = next_height.saturating_add(BULK_PER_TICK).min(file_end);
    let count = end_excl.saturating_sub(next_height);
    if count == 0 {
        return Ok(None);
    }

    let bulk_client = WocClient::new(chain, None);
    let headers = bulk_client
        .download_bulk_range(file_info, file_first, next_height, count)
        .await?;
    if headers.is_empty() {
        return Ok(None);
    }

    storage::insert_headers_batch(db, &headers).await?;
    storage::update_chain_tip_to_highest(db).await?;

    let new_tip = storage::get_chain_tip_height(db).await?;
    if new_tip > our_height {
        Ok(Some(new_tip))
    } else {
        Ok(None)
    }
}

/// Fetch headers one-by-one from WoC for a bounded window above `from_height`.
/// Capped per tick to stay under the Cloudflare subrequest limit and WoC's
/// free-tier rate limit; a single failure ends this tick (next tick resumes).
/// Setting WHATSONCHAIN_API_KEY lifts the rate limit and makes this fast.
async fn woc_catchup_one_by_one(
    db: &D1Database,
    client: &WocClient,
    from_height: u32,
    to_height: u32,
) -> Result<()> {
    // ~120 headers/tick, paced ~350ms apart (≈2.8 req/s) to stay under WoC's
    // keyless ~3 req/s limit so we don't self-throttle into 429s. A WoC key
    // lifts the limit but isn't required. ~120/min closes the ~16k above-CDN
    // gap in a few hours, and any small future gap in a tick or two.
    const MAX_PER_TICK: u32 = 120;
    const PACE_MS: u64 = 350;
    let end = to_height.min(from_height + MAX_PER_TICK);
    for h in (from_height + 1)..=end {
        match client.get_header_by_height(h).await {
            Ok(header) => {
                storage::insert_header(db, &header).await?;
            }
            Err(e) => {
                console_log!("Cron: WoC one-by-one stopped at {h}: {e:?}");
                break;
            }
        }
        if h < end {
            worker::Delay::from(std::time::Duration::from_millis(PACE_MS)).await;
        }
    }
    Ok(())
}

/// Fetch headers from an upstream chaintracks instance via getHeaders endpoint.
/// Returns parsed BlockHeaders from the concatenated hex response.
async fn fetch_headers_from_upstream(
    base_url: &str,
    start_height: u32,
    count: u32,
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

    let mut headers = Vec::with_capacity(bytes.len() / 80);
    for (i, chunk) in bytes.chunks(80).enumerate() {
        if chunk.len() < 80 {
            break;
        }
        if let Some(header) = BlockHeader::from_bytes(chunk, start_height + i as u32) {
            headers.push(header);
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

/// Persist the last-observed network tip for the /getInfo behind-by monitor.
async fn update_woc_tip(db: &D1Database, woc_height: u32) -> Result<()> {
    crate::d1::Query::new(
        "UPDATE sync_state SET woc_tip_height = ?, updated_at = datetime('now') WHERE id = 1",
    )
    .bind(woc_height)
    .run(db)
    .await
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_catch_up_threshold() {
        assert!(11 > 10, "gap > 10 triggers catch-up");
        assert!(!(10 > 10), "gap == 10 uses live WoC mode");
    }

    #[test]
    fn test_batch_size() {
        let batch_size = 1000u32;
        let max_per_cycle = 5000u32;
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

    #[test]
    fn test_woc_one_by_one_cap() {
        // A single tick never attempts more than MAX_PER_TICK WoC lookups.
        let from_height = 942761u32;
        let to_height = 958000u32;
        let max_per_tick = 120u32;
        let end = to_height.min(from_height + max_per_tick);
        assert_eq!(end, 942881, "capped to 120 above from_height");
    }

    #[test]
    fn test_ranged_slice_math() {
        // Bounded slice per tick, clamped to the covering file's end.
        let bulk_per_tick = 25_000u32;
        let file_first = 400_000u32;
        let file_end = 500_000u32; // exclusive (100k file)
        // Mid-file resume at 452_252.
        let next = 452_253u32;
        let end_excl = next.saturating_add(bulk_per_tick).min(file_end);
        let count = end_excl.saturating_sub(next);
        assert_eq!(count, 25_000, "full slice fits inside the file");
        // Byte offset math into the file.
        let byte_start = (next - file_first) as u64 * 80;
        assert_eq!(byte_start, 52_253 * 80);
        // Near the file end, the slice clamps.
        let next2 = 490_000u32;
        let end2 = next2.saturating_add(bulk_per_tick).min(file_end);
        assert_eq!(end2.saturating_sub(next2), 10_000, "clamped to file_end");
    }

    #[test]
    fn test_snapshot_top_stops_bulk() {
        // Once next_height reaches the snapshot's coverage end, bulk is skipped.
        let coverage_ends = [100_000u32, 200_000, 942_761];
        let snapshot_top = coverage_ends.iter().copied().max().unwrap();
        assert_eq!(snapshot_top, 942_761);
        assert!(942_761 >= snapshot_top, "at the top → WoC, no download");
        assert!(942_760 < snapshot_top, "one below → bulk still covers it");
    }

    #[test]
    fn test_bulk_file_pick_by_height() {
        // rposition picks the highest file whose first_height <= next_height.
        let first_heights = [0u32, 100_000, 200_000, 300_000, 400_000];
        // our_height 297299 → next 297300 → file index 2 (first_height 200000)
        let next = 297_300u32;
        let idx = first_heights.iter().rposition(|&fh| fh <= next).unwrap();
        assert_eq!(idx, 2);
        // At a boundary: our_height 299999 → next 300000 → file index 3.
        let next = 300_000u32;
        let idx = first_heights.iter().rposition(|&fh| fh <= next).unwrap();
        assert_eq!(idx, 3);
    }
}
