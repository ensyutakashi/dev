import yfinance as yf
import pandas as pd
import os
from datetime import datetime

def fetch_toyota_data():
    # トヨタのティッカーシンボル（東京証券取引所）
    ticker_symbol = "7203.T"
    ticker_code = "7203"
    company_name = "toyota"

    print(f"{ticker_symbol} の株価データを取得中（最古から最新まで）...")

    try:
        # Tickerオブジェクトの作成
        toyota = yf.Ticker(ticker_symbol)

        # period="max" で全期間の日足を取得
        df = toyota.history(period="max", interval="1d")

        if df.empty:
            print("データを取得できませんでした。ティッカーシンボルを確認してください。")
            return

        # タイムゾーン情報を除去（Excel等での扱いやすさのため）
        df.index = df.index.tz_localize(None)

        # 現在の日付を取得してファイル名を作成
        current_date = datetime.now().strftime("%Y%m%d")
        output_file = f"ohlc_yfinance_{ticker_code}_{company_name}_{current_date}.csv"
        
        # CSVとして保存
        df.to_csv(output_file)

        print("-" * 30)
        print(f"取得完了: {output_file} に保存しました。")
        print(f"データ件数: {len(df)} 行")
        print(f"期間: {df.index.min().date()} ～ {df.index.max().date()}")
        print("-" * 30)
        print("最新の5件:")
        print(df.tail())

    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    fetch_toyota_data()
