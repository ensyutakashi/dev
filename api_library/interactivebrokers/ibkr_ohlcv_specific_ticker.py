# --- obsidian_property ---
# scr名: 【自動】
# 概要: IB証券から特定銘柄のOHLCVをDL,toyotaは1984/1/4から存在
# 処理grp: IB証券API
# 処理順番: 0
# input: 無し
# output: ohlcv_ibkr_{TICKER_CODE}_yyyymmdd.csv
# mermaid: 
# tags: ["api","ibkr", "download"]
# aliases: 
# created: 2026-02-24
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：Interactive Brokersからから特定銘柄のOHLCVをDL
# toyotaは1984/1/4から存在
# --- 概要 ---


from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData
import threading
import time
import pandas as pd
import sys
import os
from datetime import datetime

# ============================================================
# 設定変数 --- ここを変更してください
# ============================================================
TICKER_CODE    = "7203"          # 取得する銘柄コード
OUTPUT_DIR     = r"C:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス\Quants\interactivebrokers"  # CSVを保存するフォルダ
OUTPUT_FILENAME = f"ohlcv_ibkr_{TICKER_CODE}_{datetime.now().strftime('%Y%m%d')}.csv"  # 保存ファイル名
# ============================================================

class ToyotaApp(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.hist_data = []
        self.finished = False
        self.head_timestamp = None
        self.data_started = False

    def nextValidId(self, orderId: int):
        print(f"Connected! (Order ID: {orderId})")
        
        # 銘柄定義
        self.contract = Contract()
        self.contract.symbol = TICKER_CODE
        self.contract.secType = "STK"
        self.contract.exchange = "SMART"
        self.contract.primaryExchange = "TSEJ"
        self.contract.currency = "JPY"
        
        # 1. 最古の日付を問い合わせ
        print("--- 最古のデータ日付を問い合わせ中... ---")
        self.reqHeadTimeStamp(8001, self.contract, "TRADES", 1, 1)

    def headTimestamp(self, reqId: int, headTimestamp: str):
        print(f"--- 取得可能な最古の日付: {headTimestamp} ---")
        self.head_timestamp = headTimestamp
        
        # 2. 全期間のデータをリクエスト
        # durationStr を十分長く設定 (1984年をカバーするため 45 Y)
        print("--- 全期間のヒストリカルデータをリクエスト中... ---")
        self.reqHistoricalData(8002, self.contract, "", "45 Y", "1 day", "TRADES", 1, 1, False, [])

    def historicalData(self, reqId: int, bar: BarData):
        self.hist_data.append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume])

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print(f"--- ヒストリカルデータの取得完了 (合計 {len(self.hist_data)} 件) ---")
        df = pd.DataFrame(self.hist_data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
        
        # CSVに保存
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        csv_path = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)
        df.to_csv(csv_path, index=False)
        print(f"--- データを {csv_path} に保存しました ---")
        
        self.finished = True
        self.disconnect()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode in [2104, 2106, 2107, 2158]:
            return
        
        print(f"Error: {reqId} {errorCode} {errorString}")
        
        if errorCode == 200: # セキュリティ定義エラー
            self.finished = True
            self.disconnect()
        
        # 接続エラー等の致命的なエラーの場合
        if errorCode in [1100, 1101, 1102]:
            self.finished = True

def main():
    app = ToyotaApp()
    # ClientId は重複を避けるため 3 に変更
    print("Connecting to 127.0.0.1:4001 (ClientId=3)...")
    app.connect("127.0.0.1", 4001, clientId=3)
    
    app_thread = threading.Thread(target=app.run, daemon=True)
    app_thread.start()
    
    # データの取得には時間がかかる可能性があるため、タイムアウトを長めに設定 (60秒)
    start_time = time.time()
    while not app.finished and time.time() - start_time < 60:
        time.sleep(1)
    
    if not app.finished:
        print("Timed out or finished without calling disconnect.")
        app.disconnect()
    
    sys.exit()

if __name__ == "__main__":
    main()