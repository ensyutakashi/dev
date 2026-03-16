# --- obsidian_property ---
# scr名: 【自動】
# 概要: 直近12週間を除く2年分の個別企業OHLCVをDL
# 処理grp: J-quantsAPI
# 処理順番: 0
# input: 無し
# output: jquants_{TICKER_CODE}_daily.csv
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
# [!abstract] 概要：J-Quantsから個別企業のOHLCVをDL,　
# 直近12週間を除く2年分の個別企業OHLCVを取得
# 価格と調整後価格を取得
# --- 概要 --

import requests
import pandas as pd
import os

# 設定変数
OUTPUT_FOLDER = os.path.dirname(os.path.abspath(__file__))  # デフォルト: スクリプトと同じ場所
# OUTPUT_FOLDER = r"C:\Users\ensyu\Documents\Output"  # ← 別フォルダにしたい場合はここを変更

TICKER_CODE = "7203"      # 銘柄コード
OUTPUT_FILENAME = f"jquants_{TICKER_CODE}_daily.csv"  # 出力ファイル名
API_KEY = "FYWH5Yy-RGk-0Fkitv-WhSB34hn2RP8fO6yF4P8Nz_o"  # 先ほど取得したキー
# APIエンドポイント (V2)
url = f"https://api.jquants.com/v2/equities/bars/daily?code={TICKER_CODE}"

# リクエストヘッダーにAPIキーをセット
headers = {
    "x-api-key": API_KEY
}

# データの取得
response = requests.get(url, headers=headers)

if response.status_code == 200:
    data = response.json()
    key_order = ['daily_quotes', 'bars', 'data']
    target = None
    for k in key_order:
        if isinstance(data, dict) and k in data:
            target = data[k]
            break
    if target is None:
        if isinstance(data, list):
            target = data
        elif isinstance(data, dict):
            target = [data]
        else:
            target = []
    df = pd.DataFrame(target)
    # 出力フォルダが存在しない場合は作成
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    output_path = os.path.join(OUTPUT_FOLDER, OUTPUT_FILENAME)
    # デバッグ情報
    print(f"OUTPUT_FOLDER: {OUTPUT_FOLDER}")
    print(f"OUTPUT_FILENAME: {OUTPUT_FILENAME}")
    print(f"output_path: {output_path}")
    print(f"df shape: {df.shape}")
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"CSV saved: {output_path}")
else:
    print(f"Error: {response.status_code}")
    try:
        print(response.text[:1000])
    except Exception:
        pass
