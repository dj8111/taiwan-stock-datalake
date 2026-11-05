#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TDCC 集保戶股權分散表擷取器 (fetch_tdcc.py)
每週採集集保千張大戶與散戶分級籌碼資料，具備內容去重與冪等性機制。
"""

import os
import sys
import hashlib
import logging
import argparse
from io import StringIO
from datetime import datetime
import requests
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

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
DEFAULT_TDCC_URL = "https://smart.tdcc.com.tw/opendata/getOD.ashx?id=1-5"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/csv,application/csv,text/plain,*/*"
}

def run(tdcc_url: str = DEFAULT_TDCC_URL, force: bool = False):
    today_dt = datetime.now(TAIPEI_TZ)
    today_str = today_dt.strftime("%Y%m%d")
    logging.info(f"=== Checking TDCC Shareholder Distribution (Execution Date: {today_str}) ===")

    try:
        resp = requests.get(tdcc_url, headers=HEADERS, timeout=90)
        if resp.status_code != 200:
            logging.error(f"TDCC responded with HTTP {resp.status_code}")
            return False

        content_bytes = resp.content
        if not content_bytes:
            logging.warning("Empty response received from TDCC.")
            return False

        current_hash = hashlib.md5(content_bytes).hexdigest()

        # 自動偵測編碼 (UTF-8 優先，失敗則回退至 CP950/Big5)
        try:
            csv_text = content_bytes.decode("utf-8")
        except UnicodeDecodeError:
            try:
                csv_text = content_bytes.decode("cp950")
            except UnicodeDecodeError:
                csv_text = content_bytes.decode("big5", errors="ignore")

        df = pd.read_csv(StringIO(csv_text))
        if df.empty:
            logging.warning("Parsed empty DataFrame from TDCC CSV.")
            return False

        # 清理欄位標題空白
        df.columns = [str(c).strip().replace(" ", "_").replace("\n", "").replace("\r", "") for c in df.columns]

        # 動態解析統計日期
        data_date = today_str
        date_col_found = False
        for col in ["資料日期", "統計日期", "Date", "date", "DATE"]:
            if col in df.columns:
                raw_val = str(df[col].iloc[0]).replace("-", "").replace("/", "").strip()
                if len(raw_val) == 8 and raw_val.isdigit():
                    data_date = raw_val
                    date_col_found = True
                    break

        if not date_col_found:
            logging.warning(f"Could not locate statistical date column in TDCC data; defaulting to today: {data_date}")
        else:
            logging.info(f"Detected TDCC statistical date: {data_date}")

        year_str = data_date[:4]
        out_dir = os.path.join("data", "tdcc", "share_distribution", year_str)
        out_parquet = os.path.join(out_dir, f"{data_date}.parquet")
        out_hash = os.path.join(out_dir, f"{data_date}.md5")
        os.makedirs(out_dir, exist_ok=True)

        # 內容冪等性去重檢查
        if os.path.exists(out_parquet) and os.path.exists(out_hash) and not force:
            try:
                with open(out_hash, "r", encoding="utf-8") as f:
                    saved_hash = f.read().strip()
                if saved_hash == current_hash:
                    logging.info(f"[SKIP] No changes detected for date {data_date} (MD5: {current_hash[:8]}). Skipping commit.")
                    return True
            except Exception as e:
                logging.warning(f"Failed to read existing hash file: {e}")

        # 資料正規化與元數據
        df["collected_at"] = datetime.now(TAIPEI_TZ).isoformat()
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].fillna("").astype(str).str.strip()

        df.to_parquet(out_parquet, index=False, engine="pyarrow", compression="snappy")

        with open(out_hash, "w", encoding="utf-8") as f:
            f.write(current_hash)

        logging.info(f"[SUCCESS] Saved TDCC distribution {out_parquet} ({len(df)} rows, MD5: {current_hash[:8]}).")
        return True

    except Exception as e:
        logging.error(f"Error occurred during TDCC ingestion: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TDCC Share Distribution Ingest")
    parser.add_argument("--url", type=str, default=DEFAULT_TDCC_URL, help="TDCC OpenData URL")
    parser.add_argument("--force", action="store_true", help="Force overwrite even if hash matches")
    args = parser.parse_args()

    success = run(tdcc_url=args.url, force=args.force)
    sys.exit(0 if success else 1)
