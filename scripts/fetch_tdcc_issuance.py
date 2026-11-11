#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TDCC 有價證券帳簿劃撥交付日程（新股發放/配發日程一覽表）擷取器 (fetch_tdcc_issuance.py)
擷取無償配股(除權股票股利)、有償認股(現金增資)、初次上市櫃(IPO)、減資等股票發放交付日程。
具備歷史資料累加、去重與冪等性機制。
"""

import os
import sys
import re
import time
import glob
import logging
import argparse
from datetime import datetime, timedelta
import urllib.request
import urllib.parse
import http.cookiejar

try:
    from zoneinfo import ZoneInfo
    TAIPEI_TZ = ZoneInfo("Asia/Taipei")
except ImportError:
    try:
        import pytz
        TAIPEI_TZ = pytz.timezone("Asia/Taipei")
    except ImportError:
        from datetime import timezone
        TAIPEI_TZ = timezone(timedelta(hours=8))

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

TDCC_URL = "https://www.tdcc.com.tw/portal/zh/smWeb/qryPS"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

class TDCCIssuanceHarvester:
    def __init__(self):
        self.cj = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cj))
        self.token = self._init_session()

    def _init_session(self) -> str:
        req = urllib.request.Request(TDCC_URL, headers={"User-Agent": USER_AGENT})
        with self.opener.open(req) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
        match = re.search(r'name=["\']SYNCHRONIZER_TOKEN["\']\s+value=["\']([^"\']+)["\']', html)
        return match.group(1) if match else ""

    def query_date(self, date_str: str):
        """
        date_str: 'YYYY/MM/DD'
        Returns: list of dicts
        """
        data = {
            "SYNCHRONIZER_TOKEN": self.token,
            "SYNCHRONIZER_URI": "/portal/zh/smWeb/qryPS",
            "method": "submit",
            "sqlMethod": "stockDate",
            "stockNo": "",
            "stockName": "",
            "stockDate": date_str
        }
        post_bytes = urllib.parse.urlencode(data).encode("utf-8")
        post_req = urllib.request.Request(
            TDCC_URL,
            data=post_bytes,
            headers={
                "User-Agent": USER_AGENT,
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": TDCC_URL
            }
        )
        try:
            with self.opener.open(post_req) as post_resp:
                post_html = post_resp.read().decode("utf-8", errors="ignore")
        except Exception as e:
            logging.warning(f"Error querying TDCC for date {date_str}: {e}")
            return []

        # Update token for subsequent queries
        new_tok_m = re.search(r'name=["\']SYNCHRONIZER_TOKEN["\']\s+value=["\']([^"\']+)["\']', post_html)
        if new_tok_m:
            self.token = new_tok_m.group(1)

        rows = re.findall(
            r'<tr>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>\s*<td[^>]*>(.*?)</td>\s*</tr>',
            post_html,
            re.I | re.S
        )
        records = []
        for r in rows:
            cols = [re.sub(r'<[^>]+>', '', c).strip() for c in r]
            if len(cols) != 4:
                continue
            if '請輸入' in cols[1] or '請選擇' in cols[3]:
                continue
            if '交付原因' in cols[0]:
                cols[0] = cols[0].split()[-1]
            
            code, name, roc_date, reason = cols
            if not code or code == '證券代號':
                continue

            iso_date = self.normalize_roc_date(roc_date)
            records.append({
                "security_code": code,
                "security_name": name,
                "delivery_date": iso_date,
                "roc_delivery_date": roc_date,
                "distribution_reason": reason
            })
        return records

    @staticmethod
    def normalize_roc_date(roc_str: str) -> str:
        # e.g. "115年11月06日" -> "2026-11-06"
        m = re.search(r'(\d+)\s*年\s*(\d+)\s*月\s*(\d+)\s*日', roc_str)
        if m:
            year = int(m.group(1)) + 1911
            month = int(m.group(2))
            day = int(m.group(3))
            return f"{year:04d}-{month:02d}-{day:02d}"
        return roc_str

def load_existing_records(target_dir: str):
    """載入已有歷史檔案以進行累加去重"""
    existing_map = {}
    if not DUCKDB_AVAILABLE:
        return existing_map
    
    parquet_files = glob.glob(os.path.join(target_dir, "**", "*.parquet"), recursive=True)
    if not parquet_files:
        return existing_map
    
    try:
        con = duckdb.connect(database=":memory:")
        safe_pattern = os.path.join(target_dir, "**", "*.parquet").replace("\\", "/")
        rel = con.execute(f"SELECT security_code, security_name, delivery_date, roc_delivery_date, distribution_reason, collected_at FROM read_parquet('{safe_pattern}', union_by_name=True);")
        for r in rel.fetchall():
            key = (r[0], r[2], r[4])
            existing_map[key] = {
                "security_code": r[0],
                "security_name": r[1],
                "delivery_date": r[2],
                "roc_delivery_date": r[3],
                "distribution_reason": r[4],
                "collected_at": r[5]
            }
        con.close()
        logging.info(f"Loaded {len(existing_map)} previously existing records from {len(parquet_files)} parquet files.")
    except Exception as e:
        logging.warning(f"Could not load existing parquet files: {e}")
    return existing_map

def harvest_issuance(days_back: int = 120, days_forward: int = 30, out_dir: str = "data/tdcc/share_issuance_schedule"):
    now_dt = datetime.now(TAIPEI_TZ)
    today_str = now_dt.strftime("%Y%m%d")
    current_year = now_dt.strftime("%Y")

    logging.info(f"=== Harvesting TDCC Stock Issuance Schedule (Back: {days_back}d, Forward: {days_forward}d) ===")
    
    # 讀取現有庫存紀錄以保留歷史
    records_map = load_existing_records(out_dir)

    harvester = TDCCIssuanceHarvester()

    start_date = now_dt.date() - timedelta(days=days_back)
    end_date = now_dt.date() + timedelta(days=days_forward)

    cur = start_date
    query_dates = []
    while cur <= end_date:
        if cur.weekday() < 5:
            query_dates.append(cur)
        cur += timedelta(days=1)

    logging.info(f"Scanning {len(query_dates)} trading/business days from {start_date} to {end_date}...")

    new_discovered = 0
    for idx, d in enumerate(query_dates):
        d_str = d.strftime("%Y/%m/%d")
        records = harvester.query_date(d_str)
        if records:
            for r in records:
                k = (r["security_code"], r["delivery_date"], r["distribution_reason"])
                if k not in records_map:
                    r["collected_at"] = now_dt.isoformat()
                    records_map[k] = r
                    new_discovered += 1
            logging.info(f"[{idx+1}/{len(query_dates)}] {d_str}: found {len(records)} items (Cumulative: {len(records_map)}, New: {new_discovered})")
        time.sleep(0.12)

    logging.info(f"Harvest complete. Total cumulative records: {len(records_map)} (Newly discovered: {new_discovered})")

    if not records_map:
        logging.warning("No distribution records collected.")
        return False

    all_records = list(records_map.values())

    # 依年份分區寫入 Parquet
    target_year_dir = os.path.join(out_dir, current_year)
    os.makedirs(target_year_dir, exist_ok=True)
    out_file = os.path.join(target_year_dir, f"{today_str}.parquet")

    if DUCKDB_AVAILABLE:
        con = duckdb.connect(database=":memory:")
        con.execute("""
            CREATE TABLE issuance (
                security_code VARCHAR,
                security_name VARCHAR,
                delivery_date VARCHAR,
                roc_delivery_date VARCHAR,
                distribution_reason VARCHAR,
                collected_at VARCHAR
            )
        """)
        row_tuples = [
            (
                r["security_code"],
                r["security_name"],
                r["delivery_date"],
                r["roc_delivery_date"],
                r["distribution_reason"],
                r.get("collected_at", now_dt.isoformat())
            )
            for r in all_records
        ]
        con.executemany("INSERT INTO issuance VALUES (?, ?, ?, ?, ?, ?)", row_tuples)
        safe_out = out_file.replace("\\", "/")
        con.execute(f"COPY (SELECT * FROM issuance ORDER BY delivery_date DESC, security_code ASC) TO '{safe_out}' (FORMAT PARQUET, COMPRESSION SNAPPY);")
        con.close()
        logging.info(f"[SUCCESS] Saved {len(all_records)} cumulative records to {out_file} using DuckDB.")
    else:
        import pandas as pd
        df = pd.DataFrame(all_records)
        df = df.sort_values(by=["delivery_date", "security_code"], ascending=[False, True])
        df.to_parquet(out_file, index=False, engine="pyarrow", compression="snappy")
        logging.info(f"[SUCCESS] Saved {len(all_records)} cumulative records to {out_file} using Pandas/PyArrow.")

    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Harvest TDCC Share Issuance Schedule")
    parser.add_argument("--days-back", type=int, default=120, help="Days to look back from today")
    parser.add_argument("--days-forward", type=int, default=30, help="Days to look forward from today")
    parser.add_argument("--out-dir", type=str, default="data/tdcc/share_issuance_schedule", help="Output directory")
    args = parser.parse_args()

    ok = harvest_issuance(days_back=args.days_back, days_forward=args.days_forward, out_dir=args.out_dir)
    sys.exit(0 if ok else 1)
