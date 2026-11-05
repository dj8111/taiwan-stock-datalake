# Taiwan Stock DataLake (TWSE / TPEx / TDCC)

基於 GitHub Actions 的 Serverless 台股開放數據湖管線。自動自官方 OpenAPI (Swagger) 採集全市場盤後數據、整合同步集保戶股權分散表，並以 Apache Parquet 分區儲存，可無縫透過 DuckDB 提供給 AI Agent (MCP) 查詢與長期量化回測。

---

## 系統架構

```
┌────────────────────────────────────────────────────────────────────────┐
│                        GitHub Actions Workflows                        │
│                                                                        │
│ [Daily: 16:30 & 21:30 CST]                [Weekly: Fri 21:30 & Sat 09:30]
│  TWSE / TPEx Dynamic Collector            TDCC Share Distribution      │
│         │                                           │                  │
│         ▼                                           ▼                  │
│  Swagger Spec Parser (v1)                 MD5 Hash Deduplication       │
│  (過濾路徑變數，採集全市場日頻數據)           (無變動直接跳過，具冪等性)  │
│         │                                           │                  │
│         └─────────────────────┬─────────────────────┘                  │
│                               ▼                                        │
│                 Parquet Serializer (Snappy)                            │
│                 data/{source}/{endpoint}/{YYYY}/{Date}.parquet         │
│                               │                                        │
│                               ▼                                        │
│                 Git Commit & Push (Hot Storage)                        │
│                                                                        │
│ [Annual: Jan 1 03:00 CST]                                              │
│  Annual Data Archival Workflow                                         │
│         │                                                              │
│         ├─── 1. Package: taiwan_stock_YYYY.tar.gz + SHA256             │
│         ├─── 2. Upload to GitHub Releases (Tag: archive-YYYY)          │
│         ├─── 3. Sync to Cloudflare R2 / AWS S3 (Cold Storage)          │
│         └─── 4. Git Prune: 建立發布保護屏障後安全修剪本機歷史目錄      │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 目錄分區結構 (Partitioning)

```text
taiwan-stock-datalake/
├── .github/
│   └── workflows/
│       ├── daily_market_ingest.yml    # TWSE/TPEx 每日排程 (16:30 & 21:30)
│       ├── weekly_tdcc_ingest.yml     # TDCC 每週集保股權分散 (週五晚 & 週六晨)
│       └── annual_archive.yml         # 每年初自動打包 Release、同步 R2 與修剪
├── data/
│   ├── twse/                          # 臺灣證券交易所 (日頻)
│   │   ├── exchangeReport_STOCK_DAY_ALL/
│   │   │   └── 2026/
│   │   │       ├── 20261104.parquet
│   │   │       └── 20261105.parquet
│   │   └── fund_T86/                  # 三大法人買賣超
│   ├── tpex/                          # 證券櫃檯買賣中心 (日頻)
│   │   └── tpex_mainboard_daily_close_quotes/
│   └── tdcc/                          # 臺灣集中保管結算所 (週頻)
│       └── share_distribution/
│           └── 2026/
│               ├── 20261106.parquet
│               └── 20261106.md5
├── manifests/                         # 每日抓取日誌狀態追蹤
│   └── 2026/
│       └── 20261105.json
├── scripts/
│   ├── fetch_twse_tpex.py             # 動態 OpenAPI 爬蟲 (排除動態參數、正規化)
│   ├── fetch_tdcc.py                  # TDCC 爬蟲 (含編碼自適應與 MD5 去重)
│   ├── archive_storage.py             # 打包、雜湊校驗與雲端上傳 (R2/S3)
│   └── query_duckdb.py                # DuckDB 跨日查詢與 MCP 範例
├── requirements.txt
├── .gitignore
└── README.md
```

---

## 本地快速啟動

### 1. 安裝環境依賴
```bash
pip install -r requirements.txt
```

### 2. 測試 TWSE/TPEx 採集 (煙霧測試)
```bash
# 限制僅擷取前 3 個端點進行快速驗證
python scripts/fetch_twse_tpex.py --limit 3
```

### 3. 測試 TDCC 集保戶股權分散表
```bash
python scripts/fetch_tdcc.py
# 再次執行測試冪等性（應輸出 [SKIP] No changes detected 且不覆蓋）
python scripts/fetch_tdcc.py
```

### 4. 驗證 DuckDB 查詢
```bash
python scripts/query_duckdb.py 2330
```

---

## GitHub Actions 設定指引

### 1. 啟用 Actions 寫入權限
請前往 GitHub 儲存庫：
`Settings` -> `Actions` -> `General` -> `Workflow permissions` -> 選擇 **`Read and write permissions`**。

### 2. 設定雲端冷儲存密鑰 (選填，若啟用 R2 / S3 歸檔)
於 `Settings` -> `Secrets and variables` -> `Actions` 新增：
* **Cloudflare R2**（推薦，免傳出流量費）：
  - `R2_ACCOUNT_ID`
  - `R2_ACCESS_KEY_ID`
  - `R2_SECRET_ACCESS_KEY`
  - `R2_BUCKET_NAME`
* **AWS S3**（備選）：
  - `AWS_ACCESS_KEY_ID`
  - `AWS_SECRET_ACCESS_KEY`
  - `AWS_REGION`
  - `S3_BUCKET_NAME`

---

## 重要優化特色說明

1. **路徑變數過濾 (Path Parameter Filtering)**：
   自動解析 Swagger 時，嚴格排除含有 `{stockNo}` 等路徑參數的端點，避免產生大量 404 請求與被交易所 WAF 封鎖。
2. **兩階段日頻排程**：
   - 每日 16:30 CST 採集收盤價與第一波三大法人。
   - 每日 21:30 CST 執行第二波覆蓋，確保融資融券與借券賣出完整入庫。
3. **TDCC 釋出延遲防禦**：
   週五 21:30 與週六 09:30 雙重排程，比對 MD5 與實際統計日期，避免因官方延遲釋出造成漏抓。
4. **DuckDB 與 MCP 支援**：
   Parquet 遵循標準列式分區結構，DuckDB 透過 `union_by_name=True` 支援高效的跨日期掃描與 Schema Drift 容錯，可直接外掛至 Model Context Protocol (MCP) Server 提供給 AI 即時分析。

