#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Taiwan Stock DataLake 網頁查詢伺服器 (app.py)
內建輕量 HTTP 伺服器與 DuckDB 查詢引擎，提供端點瀏覽、數據表格檢視與 SQL 即時工作台。
"""

import os
import sys
import glob
import json
import time
import urllib.parse
from http.server import HTTPServer, SimpleHTTPRequestHandler
import argparse

# 檢查 DuckDB 可用性
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

PORT = 8000
WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")

class DataLakeRequestHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)

        if path == "/api/catalog":
            self.handle_api_catalog()
        elif path == "/api/data":
            self.handle_api_data(query)
        elif path == "/api/status":
            self.handle_api_status()
        elif path in ["", "/", "/index.html"]:
            self.send_file(os.path.join(WEB_DIR, "index.html"), "text/html; charset=utf-8")
        else:
            # 支援訪問 static 資源
            super().do_GET()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/api/sql":
            content_length = int(self.headers.get("Content-Length", 0))
            post_body = self.rfile.read(content_length).decode("utf-8")
            self.handle_api_sql(post_body)
        else:
            self.send_error(404, "Endpoint not found")

    def handle_api_status(self):
        status = {
            "duckdb_available": DUCKDB_AVAILABLE,
            "has_local_data": os.path.exists("data"),
            "server_time": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        self.send_json(status)

    def handle_api_catalog(self):
        catalog = {}
        catalog_path = os.path.join("manifests", "endpoint_catalog.json")
        if os.path.exists(catalog_path):
            try:
                with open(catalog_path, "r", encoding="utf-8") as f:
                    catalog = json.load(f)
            except Exception as e:
                print("Failed to load catalog file:", e)

        # 檢查本地已有資料的目錄
        local_slugs = set()
        if os.path.exists("data"):
            for root, dirs, files in os.walk("data"):
                if any(f.endswith(".parquet") for f in files):
                    parts = root.replace("\\", "/").split("/")
                    # data/{source}/{slug}/{year}
                    if len(parts) >= 3:
                        local_slugs.add(f"{parts[1]}/{parts[2]}")

        # 若 catalog 尚無 TDCC，補上 TDCC 資訊
        if "tdcc" not in catalog:
            catalog["tdcc"] = {
                "share_distribution": {
                    "name": "集保戶股權分散表 (每週千張大戶/散戶分級)",
                    "path": "/opendata/getOD.ashx?id=1-5",
                    "source": "tdcc",
                    "source_name": "臺灣集中保管結算所",
                    "description": "統計各股票 1~15 級持股人數、股數與持股比率"
                }
            }

        # 標註哪些端點本地已有 parquet
        for src, items in catalog.items():
            for slug, item in items.items():
                item["has_local"] = f"{src}/{slug}" in local_slugs

        self.send_json(catalog)

    def handle_api_data(self, query):
        source = query.get("source", [""])[0]
        endpoint = query.get("endpoint", [""])[0]
        limit = int(query.get("limit", [100])[0])

        if not source or not endpoint:
            self.send_json({"error": "Missing source or endpoint parameter"}, status=400)
            return

        pattern = os.path.join("data", source, endpoint, "*", "*.parquet").replace("\\", "/")
        matching_files = glob.glob(pattern)

        if matching_files and DUCKDB_AVAILABLE:
            try:
                con = duckdb.connect(database=":memory:")
                sql = f"SELECT * FROM read_parquet('{pattern}', union_by_name=True) LIMIT {limit};"
                df = con.execute(sql).df()
                # 處理 NaN
                df = df.fillna("")
                records = df.to_dict(orient="records")
                self.send_json({"source": source, "endpoint": endpoint, "rows": records, "count": len(records)})
                return
            except Exception as e:
                print(f"Error querying parquet for {source}/{endpoint}: {e}")

        # 若本地尚未有該 Parquet 檔案
        self.send_json({
            "source": source,
            "endpoint": endpoint,
            "rows": [],
            "message": f"本地目前尚未下載 {source}/{endpoint} 的 Parquet 檔案（可透過 GitHub Actions 或執行 python scripts/fetch_twse_tpex.py 採集）。"
        })

    def handle_api_sql(self, body_text):
        if not DUCKDB_AVAILABLE:
            self.send_json({"error": "DuckDB is not installed in current Python environment"}, status=500)
            return

        try:
            payload = json.loads(body_text) if body_text else {}
            sql = payload.get("sql", "").strip()
            if not sql:
                self.send_json({"error": "Empty SQL query"}, status=400)
                return

            # 安全性保護：只允許唯讀 SELECT 語句
            sql_clean = sql.strip().lower()
            if not (sql_clean.startswith("select") or sql_clean.startswith("with") or sql_clean.startswith("describe") or sql_clean.startswith("show")):
                self.send_json({"error": "Only read-only SELECT / WITH queries are permitted"}, status=403)
                return

            start_t = time.time()
            con = duckdb.connect(database=":memory:")
            # 限制單次查詢最大 1,000 筆避免記憶體超載
            if "limit" not in sql_clean:
                sql = f"{sql.rstrip(';')} LIMIT 500;"

            df = con.execute(sql).df()
            cost_ms = int((time.time() - start_t) * 1000)
            df = df.fillna("")
            records = df.to_dict(orient="records")

            self.send_json({
                "duration_ms": cost_ms,
                "count": len(records),
                "columns": list(df.columns),
                "rows": records
            })
        except Exception as e:
            self.send_json({"error": str(e)}, status=400)

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def send_file(self, filepath, content_type):
        if not os.path.exists(filepath):
            self.send_error(404, "File not found")
            return
        with open(filepath, "rb") as f:
            content = f.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

def start_server(port=PORT):
    server = HTTPServer(("0.0.0.0", port), DataLakeRequestHandler)
    print(f"============================================================")
    print(f"  Taiwan Stock DataLake Web Query Console")
    print(f"  本地查詢工作台已啟動: http://localhost:{port}")
    print(f"  DuckDB 引擎: {'已啟用 (高效零拷貝)' if DUCKDB_AVAILABLE else '未啟用'}")
    print(f"============================================================")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n伺服器已正常停止。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Taiwan Stock DataLake Web Server")
    parser.add_argument("--port", type=int, default=PORT, help="Port to listen on (default 8000)")
    args = parser.parse_args()
    start_server(port=args.port)
