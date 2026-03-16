import pandas as pd
import win32ui
import dde
import time
import os
import sys

# 設定
INPUT_FILE = r'c:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス\Quants\rakuten\楽天証券RSS_個別銘柄.xlsx'
OUTPUT_FILE = r'c:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス\Quants\rakuten\楽天証券RSS_10件実験.xlsx'
FETCH_COUNT = 10  # 実験用に10行

# 取得したい項目
ITEMS = [
    "現在値",
    "前日比",
    "前日比率",
    "始値",
    "高値",
    "安値",
    "出来高",
    "売買代金"
]

class RakutenRSSClient:
    def __init__(self):
        self.server = dde.CreateServer()
        self.server.Create("PythonDdeClient")
    
    def fetch_data(self, ticker, items):
        """
        指定した1つの銘柄についてRSSからデータを取得する
        """
        data = {"RSS_Ticker": ticker}
        try:
            conversation = dde.CreateConversation(self.server)
            # ConnectTo(Service, Topic) -> RSSではService="RSS", Topic=Tickerコード
            conversation.ConnectTo("RSS", ticker)
            
            for item in items:
                try:
                    val = conversation.Request(item)
                    if isinstance(val, bytes):
                        val = val.decode('shift-jis').strip()
                    data[item] = val
                except:
                    data[item] = "Error"
            return data
        except Exception as e:
            # 接続失敗(マーケットスピードII未起動など)
            for item in items:
                data[item] = f"ConnError"
            return data

def main():
    print("=== 楽天証券RSS Pythonデータ取得スクリプト ===")
    
    if not os.path.exists(INPUT_FILE):
        print(f"エラー: 入力ファイルが見つかりません: {INPUT_FILE}")
        return

    print(f"Excelファイルを読み込んでいます: {os.path.basename(INPUT_FILE)}")
    try:
        df_input = pd.read_excel(INPUT_FILE)
    except Exception as e:
        print(f"エラー: Excelの読み込みに失敗しました。 {e}")
        return

    # 実験用に最初のN行を抽出
    df_subset = df_input.head(FETCH_COUNT).copy()
    
    client = RakutenRSSClient()
    results = []
    
    print(f"実験として {FETCH_COUNT} 件のデータを取得します...")
    print("※注意: MarketSpeed IIが起動していない場合、'ConnError'となります。")

    for index, row in df_subset.iterrows():
        # コードの構築
        m_code = str(row.get('銘柄コード', ''))
        market = str(row.get('市場コード', ''))
        
        # 銘柄コードが空ならCode列から推論
        if not m_code or m_code == 'nan':
            raw_code = str(row.get('Code', ''))
            m_code = raw_code[:4] if len(raw_code) >= 4 else raw_code
        
        if not market or market == 'nan':
            market = 'T'
            
        ticker_rss = f"{m_code}.{market}"
        co_name = row.get('CoName', '不明')
        
        print(f"[{index+1}/{FETCH_COUNT}] {ticker_rss} ({co_name}) を取得中...")
        
        # データの取得
        fetched = client.fetch_data(ticker_rss, ITEMS)
        
        # 元の情報と結合
        res_row = {
            "Code": row.get('Code', ''),
            "CoName": co_name
        }
        res_row.update(fetched)
        results.append(res_row)
        
        time.sleep(0.1)

    # 保存
    df_output = pd.DataFrame(results)
    print(f"結果を保存します: {os.path.basename(OUTPUT_FILE)}")
    try:
        df_output.to_excel(OUTPUT_FILE, index=False)
        print("処理が正常に完了しました。")
    except Exception as e:
        print(f"エラー: 保存に失敗しました。 {e}")

if __name__ == "__main__":
    main()
