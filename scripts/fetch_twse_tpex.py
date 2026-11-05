#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TWSE / TPEx 全端點動態擷取器 (fetch_twse_tpex.py)
自動自官方 Swagger 規格解析並採集全市場盤後數據，存為 Parquet 格式。
並自動產出/更新 endpoint_catalog.json 資料字典供 AI / MCP 查詢。
"""

import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime

# 時區設定 (優先使用標準庫 zoneinfo)
try:
    from zoneinfo import ZoneInfo
    TAIPEI_TZ = ZoneInfo("Asia/Taipei")
except ImportError:
    try:
        import pytz
        TAIPEI_TZ = pytz.timezone("Asia/Taipei")
    except ImportError:
        from datetime import timezone, timedelta
        TAIPEI_TZ = timezone(timedelta(hours=8))

import requests
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.twse.com.tw/"
}

SOURCES = {
    "twse": {
        "name": "臺灣證券交易所",
        "spec_url": "https://openapi.twse.com.tw/v1/swagger.json",
        "base_url": "https://openapi.twse.com.tw/v1"
    },
    "tpex": {
        "name": "證券櫃檯買賣中心",
        "spec_url": "https://www.tpex.org.tw/openapi/swagger.json",
        "base_url": "https://www.tpex.org.tw/openapi/v1"
    }
}

def get_endpoints(spec_url: str):
    """
    動態下載並解析 Swagger Specification，提取所有 GET Paths 及其詳細元數據 (名稱、描述)。
    過濾掉帶有路徑變數（例如包含 '{'）的端點，確保能全自動無參採集。
    """
    try:
        resp = requests.get(spec_url, headers=HEADERS, timeout=30)
        if resp.status_code == 200:
            spec = resp.json()
            paths = spec.get("paths", {})
            endpoints_meta = []
            for p, methods in paths.items():
                if "get" in methods and "{" not in p:
                    get_info = methods["get"]
                    summary = get_info.get("summary", "") or get_info.get("description", "") or p
                    summary = summary.strip().replace("\n", " ").replace("\r", "")
                    description = get_info.get("description", "") or ""
                    endpoints_meta.append({
                        "path": p,
                        "slug": p.strip("/").replace("/", "_").replace("-", "_"),
                        "summary": summary,
                        "description": description.strip()
                    })
            logging.info(f"Loaded {len(endpoints_meta)} valid GET endpoints from {spec_url}")
            return endpoints_meta
        else:
            logging.error(f"Failed to fetch OpenAPI spec ({resp.status_code}) from {spec_url}")
    except Exception as e:
        logging.error(f"Exception parsing OpenAPI spec from {spec_url}: {e}")
    return []

def safe_fetch(url: str, retries: int = 3, retry_delay: float = 2.0):
    """
    附帶指數退避 (Exponential Backoff) 的安全 HTTP GET 請求。
    針對 429 (Rate Limit) 與短暫 5xx 錯誤自動重試。
    """
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=40)
            if resp.status_code == 200:
                if not resp.text or not resp.text.strip():
                    return None
                return resp.json()
            elif resp.status_code in [404, 204]:
                return None
            elif resp.status_code == 429:
                sleep_sec = 10 * attempt
                logging.warning(f"Rate limited (429) on {url}. Sleeping {sleep_sec}s before retry {attempt}/{retries}")
                time.sleep(sleep_sec)
            elif resp.status_code in [500, 502, 503, 504]:
                time.sleep(retry_delay * attempt)
            else:
                logging.debug(f"HTTP {resp.status_code} for {url}")
                return None
        except requests.exceptions.RequestException as e:
            if attempt == retries:
                logging.warning(f"Fetch failed after {retries} retries for {url}: {e}")
            time.sleep(retry_delay * attempt)
    return None

def normalize_payload(data):
    """
    將多樣化回傳格式正規化為二維 DataFrame。
    支援 List of Dicts, 嵌套 dict (data, tables, results 等)。
    """
    if data is None:
        return None

    df = None
    if isinstance(data, list) and len(data) > 0:
        if isinstance(data[0], dict):
            df = pd.DataFrame(data)
        elif isinstance(data[0], list):
            df = pd.DataFrame(data)
    elif isinstance(data, dict):
        for candidate_key in ["data", "tables", "results", "items", "aaData", "dataList"]:
            if candidate_key in data and isinstance(data[candidate_key], list) and len(data[candidate_key]) > 0:
                candidate_data = data[candidate_key]
                if isinstance(candidate_data[0], dict):
                    df = pd.DataFrame(candidate_data)
                elif isinstance(candidate_data[0], list):
                    df = pd.DataFrame(candidate_data)
                break
        if df is None:
            df = pd.DataFrame([data])

    if df is not None and not df.empty:
        df.columns = [str(c).strip().replace(" ", "_").replace("\n", "").replace("\r", "") for c in df.columns]
        return df

    return None

def save_parquet(df: pd.DataFrame, out_file: str, today_str: str):
    """
    儲存為標準 Parquet (Snappy 壓縮)。
    加入 collected_date 與 collected_at 元數據欄位。
    """
    df["collected_date"] = today_str
    df["collected_at"] = datetime.now(TAIPEI_TZ).isoformat()

    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("").astype(str).str.strip()

    df.to_parquet(out_file, index=False, engine="pyarrow", compression="snappy")

def run(target_date: str = None, sources_to_run: list = None, limit_endpoints: int = None, sleep_interval: float = 1.2):
    today_dt = datetime.now(TAIPEI_TZ) if not target_date else datetime.strptime(target_date, "%Y%m%d")
    today_str = today_dt.strftime("%Y%m%d")
    year_str = today_str[:4]

    logging.info(f"=== Starting TWSE/TPEx Ingestion for {today_str} ===")
    stats = {
        "date": today_str,
        "executed_at": datetime.now(TAIPEI_TZ).isoformat(),
        "results": {}
    }

    if not sources_to_run:
        sources_to_run = list(SOURCES.keys())

    catalog = {}
    catalog_path = os.path.join("manifests", "endpoint_catalog.json")
    if os.path.exists(catalog_path):
        try:
            with open(catalog_path, "r", encoding="utf-8") as f:
                catalog = json.load(f)
        except Exception:
            catalog = {}

    for src in sources_to_run:
        if src not in SOURCES:
            logging.warning(f"Unknown source: {src}. Skipping.")
            continue

        info = SOURCES[src]
        logging.info(f"Processing source: {info['name']} ({src})")
        endpoints_meta = get_endpoints(info["spec_url"])

        # 同步更新資料字典目錄
        if src not in catalog:
            catalog[src] = {}
        for item in endpoints_meta:
            catalog[src][item["slug"]] = {
                "name": item["summary"],
                "path": item["path"],
                "source": src,
                "source_name": info["name"],
                "description": item["description"]
            }

        if limit_endpoints and limit_endpoints > 0:
            logging.info(f"Limiting to first {limit_endpoints} endpoints for testing.")
            endpoints_meta = endpoints_meta[:limit_endpoints]

        stats["results"][src] = {
            "total_endpoints": len(endpoints_meta),
            "saved": 0,
            "empty_or_skipped": 0,
            "failed": 0
        }

        for idx, item in enumerate(endpoints_meta, 1):
            path = item["path"]
            slug = item["slug"]
            summary = item["summary"]
            out_dir = os.path.join("data", src, slug, year_str)
            out_file = os.path.join(out_dir, f"{today_str}.parquet")

            full_url = f"{info['base_url']}/{path.lstrip('/')}"
            logging.info(f"[{idx}/{len(endpoints_meta)}] Fetching {src}: {summary} ({slug})...")

            data = safe_fetch(full_url)
            if not data:
                data = safe_fetch(f"{full_url}?date={today_str}&response=json")

            df = normalize_payload(data)
            if df is not None and not df.empty:
                os.makedirs(out_dir, exist_ok=True)
                save_parquet(df, out_file, today_str)
                stats["results"][src]["saved"] += 1
                logging.info(f"  [OK] Saved {summary} ({len(df)} rows) -> {out_file}")
            else:
                stats["results"][src]["empty_or_skipped"] += 1
                logging.info(f"  [SKIP] No data returned for {summary}")

            time.sleep(sleep_interval)

    # 儲存全市場資料端點字典 catalog
    os.makedirs("manifests", exist_ok=True)
    with open(catalog_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)
    logging.info(f"Updated global endpoint catalog at {catalog_path}")

    # 儲存當日執行日誌 manifest
    log_dir = os.path.join("manifests", year_str)
    os.makedirs(log_dir, exist_ok=True)
    manifest_path = os.path.join(log_dir, f"{today_str}.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

    logging.info(f"=== Completed Ingestion for {today_str}. Manifest saved to {manifest_path} ===")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TWSE/TPEx OpenAPI Data Ingest")
    parser.add_argument("--date", type=str, default=None, help="Target date in YYYYMMDD format (default: today)")
    parser.add_argument("--sources", type=str, default="twse,tpex", help="Comma-separated sources: twse,tpex")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of endpoints per source (for testing)")
    parser.add_argument("--sleep", type=float, default=1.2, help="Sleep interval between requests in seconds")
    args = parser.parse_args()

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    run(target_date=args.date, sources_to_run=sources, limit_endpoints=args.limit, sleep_interval=args.sleep)
