#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Taiwan Stock DataLake 網頁查詢伺服器 (app.py)
核心資料源：直接讀取 GitHub 儲存庫 (dj8111/taiwan-stock-datalake) 最新發布的 Parquet 與 Manifests。
支援本地快取與 DuckDB 零拷貝查詢引擎。
"""

import os
import sys
import glob
import json
import time
import io
import urllib.request
import urllib.parse
from http.server import HTTPServer, SimpleHTTPRequestHandler
import argparse
import threading
import webbrowser

# 依賴套件檢查
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

PORT = 8888
WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
GITHUB_REPO = "dj8111/taiwan-stock-datalake"
GITHUB_BRANCH = "main"
RAW_BASE_URL = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}"
API_BASE_URL = f"https://api.github.com/repos/{GITHUB_REPO}"

# 記憶體與本地 Parquet 快取 (避免重複下載)
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache", "parquet")
os.makedirs(CACHE_DIR, exist_ok=True)

def get_git_token():
    """嘗試從本機 git credential 取得 GitHub Token (以避免 GitHub API 頻率限制)"""
    try:
        import subprocess
        proc = subprocess.Popen(['git', 'credential', 'fill'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        out, _ = proc.communicate(f'protocol=https\nhost=github.com\n')
        for line in out.splitlines():
            if line.startswith('password='):
                return line.split('=', 1)[1]
    except Exception:
        pass
    return os.getenv("GITHUB_TOKEN", "")

GITHUB_TOKEN = get_git_token()

def fetch_github_file(relative_path: str, timeout: int = 20) -> bytes:
    """下載 GitHub Raw 檔案內容"""
    url = f"{RAW_BASE_URL}/{relative_path.lstrip('/')}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) DataLakeWeb/1.0"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()

def get_github_tree():
    """取得 GitHub 倉庫全部檔案清單"""
    url = f"{API_BASE_URL}/git/trees/{GITHUB_BRANCH}?recursive=1"
    headers = {
        "User-Agent": "Mozilla/5.0 DataLakeWeb/1.0",
        "Accept": "application/vnd.github+json"
    }
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            return [item["path"] for item in data.get("tree", []) if item.get("type") == "blob"]
    except Exception as e:
        print(f"[WARN] Failed to fetch GitHub tree: {e}")
        return []

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
            "source": f"GitHub ({GITHUB_REPO})",
            "branch": GITHUB_BRANCH,
            "raw_base": RAW_BASE_URL,
            "duckdb_available": DUCKDB_AVAILABLE,
            "pandas_available": PANDAS_AVAILABLE,
            "server_time": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        self.send_json(status)

    def handle_api_catalog(self):
        catalog = {}
        # 1. 優先從 GitHub Raw 讀取 endpoint_catalog.json
        try:
            raw_catalog_bytes = fetch_github_file("manifests/endpoint_catalog.json", timeout=10)
            catalog = json.loads(raw_catalog_bytes.decode("utf-8"))
        except Exception:
            # 2. 次選本地 manifests/endpoint_catalog.json
            catalog_path = os.path.join("manifests", "endpoint_catalog.json")
            if os.path.exists(catalog_path):
                try:
                    with open(catalog_path, "r", encoding="utf-8") as f:
                        catalog = json.load(f)
                except Exception:
                    catalog = {}

        # 3. 取得 GitHub 倉庫上目前所有實際存在的 Parquet 檔案清單
        github_files = get_github_tree()
        github_parquet_endpoints = set()
        for fpath in github_files:
            if fpath.startswith("data/") and fpath.endswith(".parquet"):
                # data/{source}/{endpoint}/{year}/{date}.parquet
                parts = fpath.split("/")
                if len(parts) >= 4:
                    github_parquet_endpoints.add(f"{parts[1]}/{parts[2]}")

        # 本地 Parquet 檔案清單
        local_parquet_endpoints = set()
        if os.path.exists("data"):
            for root, dirs, files in os.walk("data"):
                if any(f.endswith(".parquet") for f in files):
                    parts = root.replace("\\", "/").split("/")
                    if len(parts) >= 3:
                        local_parquet_endpoints.add(f"{parts[1]}/{parts[2]}")

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

        # 標註哪些端點在 GitHub 與本地已有 parquet
        for src, items in catalog.items():
            for slug, item in items.items():
                key = f"{src}/{slug}"
                item["has_github"] = key in github_parquet_endpoints
                item["has_local"] = key in local_parquet_endpoints

        self.send_json(catalog)

    def handle_api_data(self, query):
        source = query.get("source", [""])[0]
        endpoint = query.get("endpoint", [""])[0]
        limit = int(query.get("limit", [200])[0])

        if not source or not endpoint:
            self.send_json({"error": "Missing source or endpoint parameter"}, status=400)
            return

        # 1. 檢查本地是否存在已有的 Parquet 檔案
        local_pattern = os.path.join("data", source, endpoint, "*", "*.parquet").replace("\\", "/")
        matching_local = glob.glob(local_pattern)

        df = None
        data_origin = "local"

        if matching_local and PANDAS_AVAILABLE:
            try:
                # 讀取最新的一個本地 parquet
                latest_local = sorted(matching_local)[-1]
                df = pd.read_parquet(latest_local)
                data_origin = f"local ({os.path.basename(latest_local)})"
            except Exception as e:
                print(f"Error reading local parquet: {e}")

        # 2. 若本地無資料，直接自 GitHub 查詢並下載最新檔案
        if df is None:
            github_files = get_github_tree()
            target_prefix = f"data/{source}/{endpoint}/"
            matching_github = [f for f in github_files if f.startswith(target_prefix) and f.endswith(".parquet")]

            if matching_github and PANDAS_AVAILABLE:
                latest_github_file = sorted(matching_github)[-1]
                try:
                    # 檢查快取
                    cache_file = os.path.join(CACHE_DIR, latest_github_file.replace("/", "_"))
                    if os.path.exists(cache_file):
                        df = pd.read_parquet(cache_file)
                    else:
                        parquet_bytes = fetch_github_file(latest_github_file)
                        with open(cache_file, "wb") as f:
                            f.write(parquet_bytes)
                        df = pd.read_parquet(io.BytesIO(parquet_bytes))
                    data_origin = f"GitHub ({latest_github_file})"
                except Exception as e:
                    print(f"Error fetching/reading parquet from GitHub: {e}")

        if df is not None:
            df = df.fillna("")
            if limit and len(df) > limit:
                df = df.head(limit)
            records = df.to_dict(orient="records")
            self.send_json({
                "source": source,
                "endpoint": endpoint,
                "origin": data_origin,
                "rows": records,
                "count": len(records),
                "columns": list(df.columns)
            })
            return

        self.send_json({
            "source": source,
            "endpoint": endpoint,
            "origin": "none",
            "rows": [],
            "message": f"GitHub 儲存庫目前尚未有 {source}/{endpoint} 的資料（排程採集中或今日尚未產生）。"
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

            sql_clean = sql.strip().lower()
            if not (sql_clean.startswith("select") or sql_clean.startswith("with") or sql_clean.startswith("describe") or sql_clean.startswith("show")):
                self.send_json({"error": "Only read-only SELECT / WITH queries are permitted"}, status=403)
                return

            # 如果 SQL 包含了本地路徑 data/tdcc/... 但本地無檔案，自動改寫為 GitHub Raw URL
            # 例如 read_parquet('data/tdcc/share_distribution/*/*.parquet') -> read_parquet('https://raw.githubusercontent.com/.../20261030.parquet')
            github_files = get_github_tree()
            for gfile in github_files:
                if gfile.endswith(".parquet"):
                    dir_prefix = "/".join(gfile.split("/")[:-1])
                    local_match = glob.glob(f"{dir_prefix}/*.parquet")
                    if not local_match:
                        # 替換 wildcard 為 GitHub 的具體檔案 URL
                        raw_file_url = f"{RAW_BASE_URL}/{gfile}"
                        sql = sql.replace(f"'{dir_prefix}/*/*.parquet'", f"'{raw_file_url}'")
                        sql = sql.replace(f"'{dir_prefix}/*.parquet'", f"'{raw_file_url}'")

            start_t = time.time()
            con = duckdb.connect(database=":memory:")
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

def start_server(port=PORT, auto_open=True):
    actual_port = port
    server = None
    for attempt_port in range(port, port + 20):
        try:
            server = HTTPServer(("0.0.0.0", attempt_port), DataLakeRequestHandler)
            actual_port = attempt_port
            break
        except OSError:
            continue

    if not server:
        print(f"[ERROR] 無法在端口 {port} ~ {port+20} 建立伺服器，請確認無其他程序佔用。")
        sys.exit(1)

    url = f"http://localhost:{actual_port}"
    print(f"============================================================")
    print(f"  Taiwan Stock DataLake 數據湖查詢平台")
    print(f"  資料來源: GitHub 雲端倉庫 ({GITHUB_REPO})")
    print(f"  本機服務網址: {url}")
    print(f"  DuckDB 引擎: {'已啟用 (高效零拷貝)' if DUCKDB_AVAILABLE else '未啟用'}")
    print(f"============================================================")

    if auto_open:
        def open_browser():
            time.sleep(0.8)
            try:
                webbrowser.open(url)
            except Exception:
                pass
        threading.Thread(target=open_browser, daemon=True).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n伺服器已正常停止。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Taiwan Stock DataLake Web Server")
    parser.add_argument("--port", type=int, default=PORT, help=f"Port to listen on (default {PORT})")
    parser.add_argument("--no-open", action="store_true", help="Do not automatically open browser")
    args = parser.parse_args()
    start_server(port=args.port, auto_open=not args.no_open)
