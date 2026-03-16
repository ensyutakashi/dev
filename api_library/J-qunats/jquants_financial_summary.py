# --- obsidian_property ---
# scr名: 【自動】
# 概要: J-Quantsから個別企業の直近8Q(2年分)サマリー財務指標をDL
# 処理grp: J-quantsAPI
# 処理順番: 0
# input: 無し
# output: jquants_{TICKER_CODE}_financial_summary_YYYYMMDD.csv
# mermaid: 
# tags: ["api","jquants", "download"]
# aliases: 
# created: 2026-02-24
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：J-Quantsから個別企業の直近8Q(2年分)サマリー財務指標をDL
# 無料版で取得
# --- 概要 --


import os
import requests
import pandas as pd
from datetime import datetime

# =============================================
# 設定変数（必要に応じて変更してください）
# =============================================
OUTPUT_FOLDER   = os.path.dirname(__file__)            # 出力フォルダ（デフォルト: スクリプトと同じ場所）
TICKER_CODE     = "7203"                               # 銘柄コード
OUTPUT_FILENAME = f"jquants_{TICKER_CODE}_financial_summary_{datetime.now().strftime('%Y%m%d')}.csv"  # 出力ファイル名
# =============================================

API_KEY = "FYWH5Yy-RGk-0Fkitv-WhSB34hn2RP8fO6yF4P8Nz_o"

def get_api_key():
    return API_KEY

def main():
    api_key = get_api_key()
    code = TICKER_CODE
    url = "https://api.jquants.com/v2/fins/summary"
    headers = {"x-api-key": api_key}
    params = {"code": code}
    r = requests.get(url, headers=headers, params=params, timeout=60)
    if r.status_code != 200:
        print(f"Error: {r.status_code}")
        try:
            print(r.text[:1000])
        except Exception:
            pass
        return
    j = r.json()
    tgt = None
    if isinstance(j, dict):
        for k in ["data", "summary", "fins"]:
            if k in j:
                tgt = j[k]
                break
    if tgt is None:
        tgt = j if isinstance(j, list) else []
    df = pd.DataFrame(tgt)
    out = os.path.join(OUTPUT_FOLDER, OUTPUT_FILENAME)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"CSV saved: {out}")

if __name__ == "__main__":
    main()
