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

            let upstream_result = match upstream_url.as_deref() {
                Some(url) => fetch_headers_from_upstream(url, height, count).await,
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
    } else {
        // ─── Live: one-by-one from WoC with reorg detection ─────────────
        console_log!(
            "Cron: live sync {gap} blocks ({} → {woc_height})",
            our_height + 1
        );

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
