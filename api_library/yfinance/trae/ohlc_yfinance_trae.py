import os
import shutil
import certifi
import pandas as pd
import yfinance as yf
from datetime import datetime

def fix_ssl_issue():
    """
    Windows環境かつパスに日本語が含まれる場合、curl_cffiがSSL証明書を読み込めない問題を回避するため、
    証明書ファイルを一時的にASCIIパス（ユーザーフォルダ直下）にコピーして環境変数を設定します。
    """
    try:
        # 現在の証明書パスを取得
        current_ca = certifi.where()
        
        # ASCII文字のみの安全なパスを作成 (C:\Users\Username\yfinance_cacert.pem)
        safe_dir = os.path.expanduser("~")
        safe_ca = os.path.join(safe_dir, "yfinance_cacert.pem")
        
        # コピー（存在しない場合や更新が必要な場合を考慮して上書きコピー）
        shutil.copy2(current_ca, safe_ca)
        
        # 環境変数を設定 (curlおよびrequests用)
        os.environ["CURL_CA_BUNDLE"] = safe_ca
        os.environ["REQUESTS_CA_BUNDLE"] = safe_ca
        os.environ["SSL_CERT_FILE"] = safe_ca
        
    except Exception as e:
        print(f"Warning: SSL証明書の回避設定に失敗しました: {e}")

def main():
    # SSL証明書パスの問題を回避
    fix_ssl_issue()
    
    # 設定
    ticker_symbol = "7203.T"  # トヨタ自動車
    output_dir = os.path.dirname(os.path.abspath(__file__))
    yyyymmdd = datetime.now().strftime("%Y%m%d")
    output_file = os.path.join(output_dir, f"ohlc_yfinance_7203_toyota_{yyyymmdd}.csv")

    print(f"【{ticker_symbol}】の株価データを取得中...")

    try:
        # データ取得 (全期間: period="max")
        # progress=False でプログレスバーを非表示
        df = yf.download(
            ticker_symbol,
            period="max",
            interval="1d",
            auto_adjust=False,
            actions=True,
            progress=False,
        )

        if df.empty:
            print("データが見つかりませんでした。")
            return

        if isinstance(df.columns, pd.MultiIndex):
            level_last = df.columns.get_level_values(-1)
            if level_last.nunique() == 1:
                df.columns = df.columns.get_level_values(0)

        # 日付順にソート (最古から最新へ)
        df = df.sort_index()

        for col in ["Dividends", "Stock Splits"]:
            if col not in df.columns:
                df[col] = 0.0

        df = df.reset_index()
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        elif "Datetime" in df.columns:
            df = df.rename(columns={"Datetime": "Date"})
            df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

        keep_cols = ["Date", "Open", "High", "Low", "Close", "Volume", "Dividends", "Stock Splits"]
        for col in keep_cols:
            if col not in df.columns:
                df[col] = pd.NA
        df = df[keep_cols]
        df.columns.name = None

        # CSVに保存
        df.to_csv(output_file, index=False)
        
        print(f"データを保存しました: {output_file}")
        print("-" * 30)
        print(f"データ期間: {df['Date'].min()} ～ {df['Date'].max()}")
        print(f"総レコード数: {len(df)}")
        print("-" * 30)
        
        # 確認用表示
        print("最初の5行:")
        print(df.head())
        print("-" * 30)
        print("最後の5行:")
        print(df.tail())

    except Exception as e:
        print(f"エラーが発生しました: {e}")

if __name__ == "__main__":
    main()
