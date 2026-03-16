# --- obsidian_property ---
# scr名: 【自動】
# 概要: JPXからTOPIXウェイトリストをDL
# 処理grp: yfinance
# 処理順番: 0
# input: JPXのTOPIXウェイトリストURL
# output: 変数/topixs_company_names.csv
# mermaid: 
# tags: ["api","yfinance", "download"]
# aliases: 
# created: 2026-02-24
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：JPXからTOPIXウェイトリストをダウンロード
# note参考用
# https://note.com/botter_01/n/nbbec5830cc17
# --- 概要 ---

import pandas as pd
import os
from pathlib import Path

# === 設定 ===
# 定数
TOPIX_LIST_URL = "https://www.jpx.co.jp/automation/markets/indices/topix/files/topixweight_j.csv"
# 設定変数（ユーザーが変更可能）
OUTPUT_DIRECTORY = Path(r"C:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス\Quants\yfinance")
OUTPUT_FILENAME = "topixs_company_names.csv"

def download_topix_list(url=None, output_dir=None, filename=None):
    """TOPIXウェイトリストをダウンロードして保存する
    
    Args:
        url (str): ダウンロードURL（デフォルト: 設定定数TOPIX_LIST_URL）
        output_dir (str/Path): 保存先ディレクトリ（デフォルト: 設定定数OUTPUT_DIRECTORY）
        filename (str): 保存ファイル名（デフォルト: 設定定数OUTPUT_FILENAME）
    
    Returns:
        pd.DataFrame: TOPIXリストのDataFrame
        str: 保存したファイルパス
    """
    # デフォルト値設定（設定定数から）
    if url is None:
        url = TOPIX_LIST_URL
    if output_dir is None:
        output_dir = OUTPUT_DIRECTORY
    if filename is None:
        filename = OUTPUT_FILENAME
    
    # パスをPathオブジェクトに変換
    output_dir = Path(output_dir)
    
    # 出力ディレクトリの作成
    os.makedirs(output_dir, exist_ok=True)
    
    # データダウンロード
    print(f"TOPIXリストをダウンロード中: {url}")
    df_list = pd.read_csv(url, encoding="shift_jis").dropna()
    
    # 列名の英語化
    df_list = df_list.rename(columns={
        '日付': 'Date',
        'コード': 'Code',
        '銘柄名': 'CompanyName',
        '業種': 'Sector33CodeName',
        'ニューインデックス区分': 'ScaleCategory',
    })
    
    # CSV保存
    output_path = output_dir / filename
    df_list.to_csv(output_path, index=False, encoding="utf-8-sig")
    
    print(f"TOPIXリストを保存しました: {output_path}")
    print(f"銘柄数: {len(df_list)}件")
    
    return df_list, str(output_path)

# サンプル実行
if __name__ == "__main__":
    # デフォルト設定で実行
    df, path = download_topix_list()
    
    # カスタム設定で実行する場合の例
    # df, path = download_topix_list(
    #     url="https://www.jpx.co.jp/automation/markets/indices/topix/files/topixweight_j.csv",
    #     output_dir=r"C:\custom\path",
    #     filename="custom_topix_list.csv"
    # )