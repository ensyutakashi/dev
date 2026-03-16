# --- obsidian_property ---
# scr名: 【自動】
# 概要: J-Quantsから銘柄マスタをダウンロード
# 処理grp: J-quantsAPI
# 処理順番: 0
# input: 無し
# output: jquants_companies_name_YYYYMMDD.csv
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
# [!abstract] 概要：J-Quantsから銘柄マスタをダウンロード
# ????
# --- 概要 ---

import os
import requests
import pandas as pd
from datetime import datetime

API_KEY = "FYWH5Yy-RGk-0Fkitv-WhSB34hn2RP8fO6yF4P8Nz_o"

# 出力フォルダ
OUTPUT_FOLDER = r"C:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス\Quants\J-qunats"
# OUTPUT_FOLDER = os.path.dirname(__file__) スクリプトと同フォルダ
# ファイル名　→ 後ろに"_YYYYMMDD.csv"が自動追加
OUTPUT_FILE_BASE = "jquants_companies_name"


def get_api_key():
    return API_KEY

def main():
    api_key = get_api_key()
    url = "https://api.jquants.com/v2/equities/master"
    headers = {"x-api-key": api_key}
    r = requests.get(url, headers=headers, timeout=60)
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
        for k in ["data", "master", "issues", "listed"]:
            if k in j:
                tgt = j[k]
                break
    if tgt is None:
        tgt = j if isinstance(j, list) else []
    df = pd.DataFrame(tgt)
    today = datetime.now().strftime("%Y%m%d")
    out = os.path.join(OUTPUT_FOLDER, f"{OUTPUT_FILE_BASE}_{today}.csv")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"CSV saved: {out}")

if __name__ == "__main__":
    main()
