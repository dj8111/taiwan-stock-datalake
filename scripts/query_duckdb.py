#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DuckDB 查詢驗證與 MCP 範例 (query_duckdb.py)
展示如何使用 DuckDB 零拷貝查詢本機或雲端的 Parquet 數據湖分區。
"""

import sys
import duckdb
import pandas as pd

def run_sample_query(stock_code: str = "2330"):
    print(f"=== 執行 DuckDB 跨日 Parquet 查詢範例 (股票代碼: {stock_code}) ===")
    
    con = duckdb.connect(database=":memory:")
    
    # 支援使用 glob pattern 跨日期甚至跨年度查詢
    # union_by_name=True 能優雅抵禦 TWSE/TPEx 的 Schema Drift
    sql = f"""
    SELECT 
        collected_date AS 日期,
        Code AS 股票代號,
        Name AS 股票名稱,
        ClosingPrice AS 收盤價,
        Change AS 漲跌,
        TradeVolume AS 成交量
    FROM read_parquet('data/twse/exchangeReport_STOCK_DAY_ALL/*/*.parquet', union_by_name=True)
    WHERE Code = '{stock_code}'
    ORDER BY collected_date DESC
    LIMIT 10;
    """
    
    try:
        df = con.execute(sql).df()
        if df.empty:
            print("查無資料（可能尚未抓取該檔股票或檔案尚未生成）。")
        else:
            print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"DuckDB 查詢失敗 (若尚未執行爬蟲則為正常情況): {e}")
        return None

if __name__ == "__main__":
    code = sys.argv[1] if len(sys.argv) > 1 else "2330"
    run_sample_query(code)

