# --- obsidian_property ---
# scr名: 【自動】
# 概要: edinetDBから各種指標（ROE、売上、利益等）のランキングを取得してDL
# 処理grp: edinetdbAPI
# 処理順番: 0
# input: 無し
# output: edinet_ranking_{metric}_top{limit}.csv
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
# edinet_ranking_{metric}_top{limit}.csv 各種財務指標ランキングの上位企業を抽出
# 取得する指標のリスト（全14種類）
# 収益性: roe（ROE）, revenue（売上高）, operating_income（営業利益）, ordinary_income（経常利益）, net_income（純利益）
# 安全性: total_assets（総資産）, net_assets（純資産）, equity_ratio（自己資本比率）
# 投資指標: eps（EPS）, bps（BPS）, per（PER）, dividend_yield（配当利回り）, payout_ratio（配当性向）
# 総合評価: financial_health_score（AIによる財務健全性スコア）
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

# ファイル名のパターン ({metric} が指標名、{limit} が取得件数に置換されます)
OUTPUT_FILENAME_PATTERN = "edinet_ranking_{metric}_top{limit}.csv"
# -----------------------------------------------------------------------

# 取得する指標のリスト
# ウェブサイトのランキングセクションに基づき、正しいスラッグに修正
RANKING_METRICS = [
    "roe",              # ROE
    "operating-margin",  # 営業利益率
    "net-margin",        # 純利益率
    "roa",               # ROA
    "equity-ratio",      # 自己資本比率
    "per",               # PER
    "eps",               # EPS
    "dividend-yield",    # 配当利回り
    "payout-ratio",      # 配当性向
    "free-cf",           # フリーCF
    "revenue",           # 売上高
    "health-score",      # 財務健全性スコア
    "revenue-growth",    # 売上成長率
    "ni-growth",         # 純利益成長率
    "eps-growth",        # EPS成長率
]

def get_rankings(metrics, limit=100):
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    
    for metric in metrics:
        print(f"\n--- Fetching {metric.upper()} Ranking ---")
        url = f"{BASE_URL}/rankings/{metric}"
        params = {"limit": limit}
        
        try:
            response = requests.get(url, params=params, headers=HEADERS)
            
            if response.status_code != 200:
                print(f"Error ({metric}): {response.status_code}")
                continue
                
            data = response.json().get("data", [])
            df = pd.DataFrame(data)
            
            if df.empty:
                print(f"No data found for {metric}.")
                continue
            
            # 画面表示(上位5件)
            cols = ["name", "sec_code", "value"]
            # 列が存在するか確認
            valid_cols = [c for c in cols if c in df.columns]
            display_df = df[valid_cols].head(5).copy()
            
            # 列名のマッピング（見栄え用）
            col_map = {"name": "Company Name", "sec_code": "Sec Code", "value": "Value"}
            display_df.rename(columns=col_map, inplace=True)
            
            print(f"Top 5 {metric.upper()}:")
            print(display_df.to_string(index=False))
            
            # CSV保存
            filename = OUTPUT_FILENAME_PATTERN.format(metric=metric, limit=limit)
            out_path = os.path.join(OUTPUT_FOLDER, filename)
            df.to_csv(out_path, index=False, encoding="utf-8-sig")
            print(f"CSV saved: {out_path}")
            
        except Exception as e:
            print(f"An error occurred while fetching {metric}: {e}")

if __name__ == "__main__":
    get_rankings(RANKING_METRICS)
