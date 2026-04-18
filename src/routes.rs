//! HTTP routing for Chaintracks API.
//!
//! Mirrors the chaintracks-server ChaintracksService endpoints.
//! All responses wrapped in `{status: "success", value: T}` to match
//! the format expected by rust-wallet-infra and rust-overlay consumers.

use worker::*;

use crate::storage;
use crate::types::{BlockHeader, Chain};

/// Public block header (8 fields, matching production /findHeaderHexForHeight).
/// Omits internal tracking fields (headerId, chainWork, isActive, etc.)
#[derive(serde::Serialize)]
#[serde(rename_all = "camelCase")]
struct PublicBlockHeader {
    version: u32,
    previous_hash: String,
    merkle_root: String,
    time: u32,
    bits: u32,
    nonce: u32,
    height: u32,
    hash: String,
}

impl From<BlockHeader> for PublicBlockHeader {
    fn from(h: BlockHeader) -> Self {
        Self {
            version: h.version,
            previous_hash: h.previous_hash,
            merkle_root: h.merkle_root,
            time: h.time,
            bits: h.bits,
            nonce: h.nonce,
            height: h.height,
            hash: h.hash,
        }
    }
}

/// Standard response wrapper matching ChaintracksService format.
fn wrap_success(value: impl serde::Serialize) -> Result<Response> {
    Response::from_json(&serde_json::json!({
        "status": "success",
        "value": value
    }))
}

fn wrap_error(message: &str, status_code: u16) -> Result<Response> {
    let body = serde_json::json!({
        "status": "error",
        "code": "ERR_NOT_FOUND",
        "description": message
    });
    let response = Response::from_json(&body)?;
    Ok(response.with_status(status_code))
}

pub async fn handle_request(req: Request, env: &Env) -> Result<Response> {
    let path = req.path();
    let method = req.method();

    // CORS preflight
    if method == Method::Options {
        return cors_preflight();
    }

    let chain = match env
        .var("CHAIN")
        .map(|v| v.to_string())
        .unwrap_or_default()
        .as_str()
    {
        "test" => Chain::Test,
        _ => Chain::Main,
    };

    let db = env.d1("DB")?;
    let response = match (method, path.as_str()) {
        // Health (plain text, no wrapper — matches production root endpoint)
        (Method::Get, "/") => health(&chain),

        // Info & chain
        (Method::Get, "/getChain") => wrap_success(chain.as_str()),
        (Method::Get, "/getInfo") => get_info(&db, &chain).await,
        (Method::Get, "/currentHeight") => current_height(&db).await,

        // Chain tip
        (Method::Get, "/findChainTipHashHex") => find_chain_tip_hash(&db).await,
        (Method::Get, "/findChainTipHeaderHex") => find_chain_tip_header_hex(&db).await,

        // Header queries
        (Method::Get, "/findHeaderHexForHeight") => {
            let url = req.url()?;
            find_header_hex_for_height(&db, &url).await
        }
        (Method::Get, "/findHeaderHexForBlockHash") => {
            let url = req.url()?;
            find_header_hex_for_block_hash(&db, &url).await
        }
        (Method::Get, "/getHeaders") => {
            let url = req.url()?;
            get_headers(&db, &url).await
        }

        // Validation
        (Method::Get, "/isValidRootForHeight") => {
            let url = req.url()?;
            is_valid_root_for_height(&db, &url).await
        }

        // Admin: trigger bulk CDN sync for a single file
        (Method::Get, "/admin/bulk-sync") => {
            let url = req.url()?;
            admin_bulk_sync(&db, &chain, &url).await
        }

        // Admin: export headers from D1 to R2
        (Method::Get, "/admin/export-r2") => {
            let url = req.url()?;
            admin_export_r2(&db, env, &chain, &url).await
        }

        // Serve bulk header files from R2
        (Method::Get, path) if path.starts_with("/headers/") => serve_r2_file(env, path).await,

        _ => Response::error("Not Found", 404),
    };

    response.map(add_cors)
}

fn health(chain: &Chain) -> Result<Response> {
    // Root health endpoint returns plain text (matches production exactly)
    Response::ok(format!("Chaintracks {chain}Net Block Header Service"))
}

async fn get_info(db: &worker::D1Database, chain: &Chain) -> Result<Response> {
    let info = storage::get_info(db, chain).await?;
    wrap_success(&info)
}

async fn current_height(db: &worker::D1Database) -> Result<Response> {
    let height = storage::get_chain_tip_height(db).await?;
    wrap_success(height)
}

async fn find_chain_tip_hash(db: &worker::D1Database) -> Result<Response> {
    match storage::find_chain_tip(db).await? {
        Some(h) => wrap_success(&h.hash),
        None => wrap_error("No chain tip", 404),
    }
}

async fn find_chain_tip_header_hex(db: &worker::D1Database) -> Result<Response> {
    match storage::find_chain_tip(db).await? {
        // Production returns full header JSON, not just hex
        Some(h) => wrap_success(&h),
        None => wrap_error("No chain tip", 404),
    }
}

async fn find_header_hex_for_height(db: &worker::D1Database, url: &url::Url) -> Result<Response> {
    let height: u32 = url
        .query_pairs()
        .find(|(k, _)| k == "height")
        .and_then(|(_, v)| v.parse().ok())
        .ok_or_else(|| Error::RustError("Missing ?height= parameter".into()))?;

    match storage::find_header_for_height(db, height).await? {
        Some(h) => wrap_success(PublicBlockHeader::from(h)),
        None => wrap_error("Header not found", 404),
    }
}

async fn find_header_hex_for_block_hash(
    db: &worker::D1Database,
    url: &url::Url,
) -> Result<Response> {
    let hash = url
        .query_pairs()
        .find(|(k, _)| k == "hash")
        .map(|(_, v)| v.to_string())
        .ok_or_else(|| Error::RustError("Missing ?hash= parameter".into()))?;

    match storage::find_active_header_for_hash(db, &hash).await? {
        Some(h) => wrap_success(PublicBlockHeader::from(h)),
        None => wrap_error("Header not found", 404),
    }
}

async fn get_headers(db: &worker::D1Database, url: &url::Url) -> Result<Response> {
    let height: u32 = url
        .query_pairs()
        .find(|(k, _)| k == "height")
        .and_then(|(_, v)| v.parse().ok())
        .ok_or_else(|| Error::RustError("Missing ?height= parameter".into()))?;
    let count: u32 = url
        .query_pairs()
        .find(|(k, _)| k == "count")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(1);

    let hex_str = storage::get_headers_hex(db, height, count).await?;
    wrap_success(&hex_str)
}

async fn is_valid_root_for_height(db: &worker::D1Database, url: &url::Url) -> Result<Response> {
    let root = url
        .query_pairs()
        .find(|(k, _)| k == "root")
        .map(|(_, v)| v.to_string())
        .ok_or_else(|| Error::RustError("Missing ?root= parameter".into()))?;
    let height: u32 = url
        .query_pairs()
        .find(|(k, _)| k == "height")
        .and_then(|(_, v)| v.parse().ok())
        .ok_or_else(|| Error::RustError("Missing ?height= parameter".into()))?;

    let valid = storage::is_valid_root_for_height(db, &root, height).await?;
    wrap_success(valid)
}

/// Admin endpoint: download one bulk CDN file and insert into D1.
/// Usage: /admin/bulk-sync?file=0 (file index 0-8)
/// Each file contains ~100k headers. Run one at a time.
async fn admin_bulk_sync(
    db: &worker::D1Database,
    chain: &Chain,
    url: &url::Url,
) -> Result<Response> {
    let file_idx: usize = url
        .query_pairs()
        .find(|(k, _)| k == "file")
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);

    // Get file listing from CDN
    let listing = crate::woc::WocClient::get_bulk_file_listing(chain).await?;

    if file_idx >= listing.files.len() {
        return wrap_error(
            &format!(
                "File index {} out of range (0-{})",
                file_idx,
                listing.files.len() - 1
            ),
            400,
        );
    }

    let file_info = &listing.files[file_idx];
    let start_height = file_info.first_height.unwrap_or(file_idx as u32 * 100_000);

    // Download and parse
    let client = crate::woc::WocClient::new(chain, None);
    let headers = client.download_bulk_file(file_info, start_height).await?;
    let count = headers.len();

    // Batch insert
    let inserted = storage::insert_headers_batch(db, &headers).await?;

    // Update chain tip
    storage::update_chain_tip_to_highest(db).await?;

    wrap_success(serde_json::json!({
        "file": file_info.file_name,
        "startHeight": start_height,
        "headersInFile": count,
        "inserted": inserted,
    }))
}

/// Admin endpoint: export headers from D1 to R2 as bulk binary files.
/// Usage: /admin/export-r2 (exports all) or /admin/export-r2?file=0 (single file)
async fn admin_export_r2(
    db: &worker::D1Database,
    env: &worker::Env,
    chain: &Chain,
    url: &url::Url,
) -> Result<Response> {
    let bucket = env.bucket("BULK_HEADERS")?;

    // Use the worker's own URL as CDN base (served via /headers/ route)
    let cdn_base_url = format!("https://{}/headers", url.host_str().unwrap_or("localhost"));

    let file_param: Option<u32> = url
        .query_pairs()
        .find(|(k, _)| k == "file")
        .and_then(|(_, v)| v.parse().ok());

    match file_param {
        Some(idx) => {
            let count = crate::r2::export_bulk_file(db, &bucket, chain, idx, &cdn_base_url).await?;
            wrap_success(serde_json::json!({
                "file": format!("{chain}Net_{idx}.headers"),
                "exported": count,
            }))
        }
        None => {
            let result = crate::r2::export_all(db, &bucket, chain, &cdn_base_url).await?;
            wrap_success(serde_json::json!({
                "totalExported": result.total_exported,
                "fileCount": result.file_count,
            }))
        }
    }
}

/// Serve bulk header files from R2 bucket.
/// /headers/mainNetBlockHeaders.json — index
/// /headers/mainNet_0.headers — binary file
async fn serve_r2_file(env: &worker::Env, path: &str) -> Result<Response> {
    let bucket = env.bucket("BULK_HEADERS")?;

    // Strip /headers/ prefix to get the R2 key
    let key = path.trim_start_matches("/headers/");
    if key.is_empty() {
        return Response::error("Not Found", 404);
    }

    match crate::r2::serve_file(&bucket, key).await? {
        Some(bytes) => {
            let headers = Headers::new();
            headers.set("Cache-Control", "public, max-age=3600")?;

            if key.ends_with(".json") {
                headers.set("Content-Type", "application/json")?;
            } else if key.ends_with(".headers") {
                headers.set("Content-Type", "application/octet-stream")?;
            }

            Ok(Response::from_bytes(bytes)?.with_headers(headers))
        }
        None => Response::error("Not Found", 404),
    }
}

fn cors_preflight() -> Result<Response> {
    let headers = Headers::new();
    headers.set("Access-Control-Allow-Origin", "*")?;
    headers.set("Access-Control-Allow-Methods", "GET, OPTIONS")?;
    headers.set("Access-Control-Allow-Headers", "*")?;
    headers.set("Access-Control-Max-Age", "86400")?;
    Ok(Response::empty()?.with_status(204).with_headers(headers))
}

fn add_cors(mut response: Response) -> Response {
    let _ = response
        .headers_mut()
        .set("Access-Control-Allow-Origin", "*");
    response
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_wrap_success_format() {
        // Verify the wrapper format matches ChaintracksService
        let json = serde_json::json!({
            "status": "success",
            "value": "main"
        });
        assert_eq!(json["status"], "success");
        assert_eq!(json["value"], "main");
    }

    #[test]
    fn test_wrap_success_number() {
        let json = serde_json::json!({
            "status": "success",
            "value": 870000
        });
        assert_eq!(json["value"], 870000);
    }

    #[test]
    fn test_wrap_success_boolean() {
        let json = serde_json::json!({
            "status": "success",
            "value": true
        });
        assert_eq!(json["value"], true);
    }

    #[test]
    fn test_wrap_error_format() {
        let json = serde_json::json!({
            "status": "error",
            "code": "ERR_NOT_FOUND",
            "description": "Header not found"
        });
        assert_eq!(json["status"], "error");
        assert_eq!(json["code"], "ERR_NOT_FOUND");
    }

    #[test]
    fn test_health_text_format() {
        // Production root returns plain text, not JSON wrapper
        let expected = "Chaintracks mainNet Block Header Service";
        assert!(expected.contains("Chaintracks"));
        assert!(expected.contains("mainNet"));
    }
}
