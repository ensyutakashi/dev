# --- obsidian_property ---
# scr名: 【自動】
# 概要: EDINETから企業名を取得
# 処理grp: edinetdbAPI
# 処理順番: 0
# input: 無し
# output: edinet_company_names_yyyymmdd.csv
# mermaid: 
# tags: ["api","edinetdb", "download"]
# aliases: 
# created: 2026-02-25
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：EDINETから企業名を取得
# --- 概要 ---

import os
import requests
import pandas as pd
from datetime import datetime

BASE = "https://edinetdb.jp/v1"

# 出力フォルダ
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))  # デフォルト: スクリプトと同じ場所
# OUTPUT_DIR = r"C:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス\Quants\EdinetDB"
# ファイル名
OUTPUT_FILENAME = f"edinet_company_names_{datetime.now().strftime('%Y%m%d')}.csv"  

PER_PAGE = 1000  # 1リクエストあたりの取得件数

def main():
    key = os.getenv("EDB_API_KEY")
    if not key:
        raise RuntimeError("EDB_API_KEY is not set")
    headers = {"X-API-Key": key}

    # ---- 全件取得: ページネーションループ ----
    all_data = []
    page = 1
    while True:
        params = {"per_page": PER_PAGE, "page": page}
        r = requests.get(f"{BASE}/companies", params=params, headers=headers, timeout=60)
        if r.status_code != 200:
            print(f"Error: {r.status_code} (page={page})")
            try:
                print(r.text[:1000])
            except Exception:
                pass
            break
        j = r.json()
        if isinstance(j, dict) and "data" in j:
            chunk = j["data"]
        elif isinstance(j, list):
            chunk = j
        else:
            chunk = []

        all_data.extend(chunk)
        print(f"  page {page}: {len(chunk)} 件取得 (累計 {len(all_data)} 件)")

        # 取得件数が per_page 未満 → 最終ページ
        if len(chunk) < PER_PAGE:
            break
        page += 1
    # ---- ここまで ----

    df = pd.DataFrame(all_data)
    print(f"合計 {len(df)} 件、{len(df.columns)} 項目")

    folder   = OUTPUT_DIR      if OUTPUT_DIR      else os.path.dirname(__file__)
    filename = OUTPUT_FILENAME if OUTPUT_FILENAME else f"edinet_company_names_{datetime.now().strftime('%Y%m%d')}.csv"
    out = os.path.join(folder, filename)
    os.makedirs(folder, exist_ok=True)

    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"CSV saved: {out}")

if __name__ == "__main__":
    main()
