# --- obsidian_property ---
# scr名: 【自動】
# 概要: edinetDBから特定企業の全指標（財務データ・AI分析・企業情報）を統合してDL
# 処理grp: edinetdbAPI
# 処理順番: 0
# input: 無し
# output: edinet_full_report_{code}.csv
# mermaid: 
# tags: ["api","edinetDB", "download"]
# aliases: 
# created: 2026-02-25   
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：
# 指定した企業の全ての財務指標（24項目）、AI分析スコア、要約、企業情報を
# 1つの統合CSVファイル（edinet_full_report_{code}.csv）として抽出します。
# --- 概要 --


import os
import requests
import pandas as pd

# API Key provided by the user
API_KEY = "edb_ccee6327637b0b8a3bd2db16774e01f3"
BASE_URL = "https://edinetdb.jp/v1"
HEADERS = {"X-API-Key": API_KEY}

# -----------------------------------------------------------------------
# 出力設定: フォルダとファイル名を変数で指定
# -----------------------------------------------------------------------
OUTPUT_FOLDER = os.path.dirname(os.path.abspath(__file__))  # デフォルト: スクリプトと同じ場所
# OUTPUT_FOLDER = r"C:\Users\ensyu\Documents\Output"  # ← 別フォルダにしたい場合はここを変更

OUTPUT_FILENAME_FULL = "edinet_full_report_{code}.csv"  # 全指標統合ファイル
# -----------------------------------------------------------------------

def get_full_company_data(query="日本"):
    # 1. 企業検索
    print(f"\n--- Searching for: {query} ---")
    search_url = f"{BASE_URL}/search"
    search_res = requests.get(search_url, params={"q": query}, headers=HEADERS)
    
    if search_res.status_code != 200:
        print(f"Search Error: {search_res.status_code}")
        return

    companies = search_res.json().get("data", [])
    if not companies:
        print("No company found.")
        return
    
    # 最初の1社を対象にする
    company_info = companies[0]
    edinet_code = company_info.get("edinet_code")
    company_name = company_info.get("name")
    print(f"Target: {company_name} ({edinet_code})")

    # 2. 全財務指標の取得 (最大24項目、複数年度)
    print(f"Fetching Financial Indicators...")
    fin_url = f"{BASE_URL}/companies/{edinet_code}/financials"
    fin_res = requests.get(fin_url, headers=HEADERS)
    
    if fin_res.status_code != 200:
        print(f"Financials Error: {fin_res.status_code}")
        return
    
    fin_data = fin_res.json().get("data", [])
    if not fin_data:
        print("No financial data found.")
        return
    
    df_fin = pd.DataFrame(fin_data)

    # 3. AI分析結果の取得
    print(f"Fetching AI Analysis...")
    ana_url = f"{BASE_URL}/companies/{edinet_code}/analysis"
    ana_res = requests.get(ana_url, headers=HEADERS)
    
    analysis_result = {}
    if ana_res.status_code == 200:
        analysis_result = ana_res.json().get("data", {})
    
    # 4. データの統合
    # 全年度の行に「企業名」「証券コード」「AIスコア」「AI要約」などの情報を付加
    df_fin["company_name"] = company_name
    df_fin["sec_code"] = company_info.get("sec_code")
    df_fin["industry"] = company_info.get("industry")
    
    df_fin["ai_health_score"] = analysis_result.get("credit_score")
    df_fin["ai_summary"] = analysis_result.get("summary_ja") or analysis_result.get("summary")
    
    # 列の順番を整理（見やすいように主要情報を左側に）
    cols = ["company_name", "sec_code", "fiscal_year", "ai_health_score", "ai_summary"]
    remaining_cols = [c for c in df_fin.columns if c not in cols]
    df_fin = df_fin[cols + remaining_cols]

    # 5. CSV保存
    filename = OUTPUT_FILENAME_FULL.format(code=edinet_code)
    out_path = os.path.join(OUTPUT_FOLDER, filename)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    df_fin.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\n--- Process Complete ---")
    print(f"Integrated Full Report saved: {out_path}")
    
    # 画面には直近のサマリーを表示
    print("\nLatest Summary:")
    print(df_fin.iloc[-1:][["fiscal_year", "revenue", "operating_income", "ai_health_score"]].to_string(index=False))

if __name__ == "__main__":
    get_full_company_data("日本")
