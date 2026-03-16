# --- obsidian_property ---
# scr名: 【自動】
# 概要: OHLCVをDL
# 処理grp: yfinance
# 処理順番: 0
# input: コードを指定してyfinanceAPIから
# output: ohlcv_yfinance.csv
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

import yfinance as yf
import pandas as pd
import os
from datetime import datetime
import time

# === 設定 ===
# 出力先フォルダ（変更可能）
OUTPUT_DIRECTORY = r"C:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス\Quants\yfinance"
# 出力ファイル名（変更可能）
OUTPUT_FILENAME = "ohlcv_yfinance.csv"
# 対象銘柄リスト（変更可能）
TARGET_TICKERS = ['7203', '6758', '9984']  # トヨタ、ソニー、ソフトバンク

def download_yf_daily(start="1900-01-01", end=None, chunk_size=50, tickers=None):
    """Download daily prices for tickers in manageable batches."""
    # デフォルト銘柄の設定
    if tickers is None:
        tickers = TARGET_TICKERS
    
    start_time = time.time()
    print(f"データ取得開始: {len(tickers)}銘柄 ({start}～)")

    tickers_yf = [f"{code}.T" for code in tickers]
    frames = []
    for i in range(0, len(tickers_yf), chunk_size):
        batch = tickers_yf[i : i + chunk_size]
        data = yf.download(
            tickers=batch,
            start=start,
            end=end,
            interval="1d",
            auto_adjust=False,  # 調整後終値を取得するためにFalseに
            progress=False,
        )
        if data.empty:
            continue
        if isinstance(data.columns, pd.MultiIndex):
            batch_df = data.stack(level=-1, future_stack=True).rename_axis(index=["Date", "Ticker"]).reset_index()
        else:
            batch_df = data.reset_index().assign(Ticker=batch[0])
        frames.append(batch_df)

    prices = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    
    # 処理時間と結果の表示
    end_time = time.time()
    processing_time = end_time - start_time
    
    if not prices.empty:
        print(f"データ取得完了: {len(prices)}件")
        print(f"処理時間: {processing_time:.2f}秒")
    else:
        print(f"データが取得できませんでした")
        print(f"処理時間: {processing_time:.2f}秒")
    
    return prices

def save_to_csv(df, output_dir=None, filename=None):
    """DataFrameをCSVファイルとして保存する"""
    if df.empty:
        print("保存するデータがありません")
        return
    
    # 出力ディレクトリの設定
    if output_dir is None:
        output_dir = OUTPUT_DIRECTORY
    
    # 出力ディレクトリの作成
    os.makedirs(output_dir, exist_ok=True)
    
    # ファイル名の生成
    if filename is None:
        filename = OUTPUT_FILENAME
    
    # CSV保存
    filepath = os.path.join(output_dir, filename)
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    print(f"CSVファイルを保存しました: {filepath}")
    print(f"データ件数: {len(df)}")
    return filepath

# サンプル実行
if __name__ == "__main__":
    # 設定変数から銘柄リストを使用
    df_ohlcv = download_yf_daily()
    df_ohlcv['Code'] = df_ohlcv['Ticker'].str[:4]
    
    # CSV出力
    save_to_csv(df_ohlcv)