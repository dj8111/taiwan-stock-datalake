#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Taiwan Stock DataLake 網頁查詢伺服器 (app.py)
核心資料源：直接讀取本機數據湖或 GitHub 儲存庫 (dj8111/taiwan-stock-datalake)。
採用原生 DuckDB 零拷貝查詢引擎（無需依賴外部 Pandas/PyArrow，免疫系統 DLL 限制）。
"""

import os
import sys
import glob
import json
import time
import urllib.request
import urllib.parse
from http.server import HTTPServer, SimpleHTTPRequestHandler
import argparse
import threading
import webbrowser

# DuckDB 原生引擎檢查
try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False

PORT = 8888
WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
GITHUB_REPO = "dj8111/taiwan-stock-datalake"
GITHUB_BRANCH = "main"
RAW_BASE_URL = f"https://raw.githubusercontent.com/{GITHUB_REPO}/{GITHUB_BRANCH}"
API_BASE_URL = f"https://api.github.com/repos/{GITHUB_REPO}"

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache", "parquet")
os.makedirs(CACHE_DIR, exist_ok=True)

def get_git_token():
    try:
        import subprocess
        proc = subprocess.Popen(['git', 'credential', 'fill'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        out, _ = proc.communicate('protocol=https\nhost=github.com\n')
        for line in out.splitlines():
            if line.startswith('password='):
                return line.split('=', 1)[1]
    except Exception:
        pass
    return os.getenv("GITHUB_TOKEN", "")

GITHUB_TOKEN = get_git_token()

def fetch_github_file(relative_path: str, timeout: int = 20) -> bytes:
    url = f"{RAW_BASE_URL}/{relative_path.lstrip('/')}"
    headers = {"User-Agent": "Mozilla/5.0 DataLakeWeb/1.0"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()

def get_github_tree():
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
            "has_local_data": os.path.exists("data"),
            "server_time": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        self.send_json(status)

    def handle_api_catalog(self):
        catalog = {}
        # 1. 優先從本地 manifests/endpoint_catalog.json 讀取
        catalog_path = os.path.join("manifests", "endpoint_catalog.json")
        if os.path.exists(catalog_path):
            try:
                with open(catalog_path, "r", encoding="utf-8") as f:
                    catalog = json.load(f)
            except Exception:
                catalog = {}

        # 2. 若本地無 catalog 則嘗試從 GitHub Raw 讀取
        if not catalog:
            try:
                raw_catalog_bytes = fetch_github_file("manifests/endpoint_catalog.json", timeout=10)
                catalog = json.loads(raw_catalog_bytes.decode("utf-8"))
            except Exception:
                catalog = {}

        # 3. 掃描本地實際存在的 Parquet 檔案
        local_parquet_endpoints = set()
        if os.path.exists("data"):
            for root, dirs, files in os.walk("data"):
                if any(f.endswith(".parquet") for f in files):
                    parts = root.replace("\\", "/").split("/")
                    if len(parts) >= 3:
                        local_parquet_endpoints.add(f"{parts[1]}/{parts[2]}")

        # 4. 補齊 TDCC 資訊
        if "tdcc" not in catalog:
            catalog["tdcc"] = {}
        catalog["tdcc"]["share_distribution"] = {
            "name": "集保戶股權分散表 (每週千張大戶/散戶分級)",
            "path": "/opendata/getOD.ashx?id=1-5",
            "source": "tdcc",
            "source_name": "臺灣集中保管結算所",
            "description": "統計各股票 1~15 級持股人數、股數與持股比率"
        }
        catalog["tdcc"]["share_issuance_schedule"] = {
            "name": "有價證券帳簿劃撥交付日程 (新股發放/除權增資股票交付)",
            "path": "/portal/zh/smWeb/qryPS",
            "source": "tdcc",
            "source_name": "臺灣集中保管結算所",
            "description": "集保結算所公佈之無償配股(除權股票股利)、有償認股(現金增資)、初次上市櫃與減資等股票交付發放日程"
        }

        # 標註哪些端點本地或 GitHub 已有 parquet
        for src, items in catalog.items():
            for slug, item in items.items():
                key = f"{src}/{slug}"
                item["has_local"] = key in local_parquet_endpoints
                item["has_github"] = True  # GitHub Actions 已同步

        self.send_json(catalog)

    def handle_api_data(self, query):
        source = query.get("source", [""])[0]
        endpoint = query.get("endpoint", [""])[0]
        limit = int(query.get("limit", [200])[0])

        if not source or not endpoint:
            self.send_json({"error": "Missing source or endpoint parameter"}, status=400)
            return

        if not DUCKDB_AVAILABLE:
            self.send_json({"error": "DuckDB is not installed in current Python environment"}, status=500)
            return

        # 1. 優先檢查本地 Parquet 檔案
        local_files = glob.glob(os.path.join("data", source, endpoint, "**", "*.parquet"), recursive=True)
        local_pattern = os.path.join("data", source, endpoint, "**", "*.parquet").replace("\\", "/")

        records = []
        cols = []
        data_origin = "local"

        if local_files:
            try:
                con = duckdb.connect(database=":memory:")
                # 排序選取最新檔案或讀取全部分區
                sql = f"SELECT * FROM read_parquet('{local_pattern}', union_by_name=True) LIMIT {limit};"
                rel = con.execute(sql)
                cols = [c[0] for c in rel.description] if rel.description else []
                raw_rows = rel.fetchall()
                records = [dict(zip(cols, [str(v) if v is not None else '' for v in r])) for r in raw_rows]
                data_origin = f"本地數據庫 ({len(local_files)} 個檔案)"
            except Exception as e:
                print(f"[WARN] Local parquet read error: {e}")

        # 2. 若本地尚無檔案，直接自 GitHub 抓取最新檔案至快取並讀取
        if not records:
            github_files = get_github_tree()
            target_prefix = f"data/{source}/{endpoint}/"
            matching_github = [f for f in github_files if f.startswith(target_prefix) and f.endswith(".parquet")]

            if matching_github:
                latest_github_file = sorted(matching_github)[-1]
                cache_file = os.path.join(CACHE_DIR, latest_github_file.replace("/", "_"))
                try:
                    if not os.path.exists(cache_file):
                        parquet_bytes = fetch_github_file(latest_github_file)
                        with open(cache_file, "wb") as f:
                            f.write(parquet_bytes)

                    con = duckdb.connect(database=":memory:")
                    safe_cache_path = cache_file.replace("\\", "/")
                    sql = f"SELECT * FROM read_parquet('{safe_cache_path}') LIMIT {limit};"
                    rel = con.execute(sql)
                    cols = [c[0] for c in rel.description] if rel.description else []
                    raw_rows = rel.fetchall()
                    records = [dict(zip(cols, [str(v) if v is not None else '' for v in r])) for r in raw_rows]
                    data_origin = f"GitHub 雲端 ({os.path.basename(latest_github_file)})"
                except Exception as e:
                    print(f"[WARN] GitHub parquet read error: {e}")

        if records:
            self.send_json({
                "source": source,
                "endpoint": endpoint,
                "origin": data_origin,
                "rows": records,
                "count": len(records),
                "columns": cols
            })
            return

        self.send_json({
            "source": source,
            "endpoint": endpoint,
            "origin": "none",
            "rows": [],
            "message": f"尚未找到 {source}/{endpoint} 的數據，可能排程採集中或今日尚未產生。"
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

            start_t = time.time()
            con = duckdb.connect(database=":memory:")
            if "limit" not in sql_clean:
                sql = f"{sql.rstrip(';')} LIMIT 500;"

            rel = con.execute(sql)
            cost_ms = int((time.time() - start_t) * 1000)
            cols = [c[0] for c in rel.description] if rel.description else []
            raw_rows = rel.fetchall()
            records = [dict(zip(cols, [str(v) if v is not None else '' for v in r])) for r in raw_rows]

            self.send_json({
                "duration_ms": cost_ms,
                "count": len(records),
                "columns": cols,
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
    print(f"  資料來源: 本機 Parquet 數據湖 + GitHub 雲端備援")
    print(f"  本機服務網址: {url}")
    print(f"  DuckDB 原生引擎: {'已啟用 (高效零拷貝)' if DUCKDB_AVAILABLE else '未啟用'}")
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
