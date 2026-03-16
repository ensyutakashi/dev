# --- obsidian_property ---
# scr名: 【自動】
# 概要: IB証券 からOHLCVをDL (並列処理版)
# 処理grp: IB証券API
# 処理順番: 0
# input: jquants_companies_name_{date}.csv
# output: ohlcv_ibkr_{TICKER_CODE}_{CoName}_{yyyymmdd}.parquet
# mermaid: 
# tags: ["api","ibkr", "download"]
# aliases: 
# created: 2026-02-25ff
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：Interactive BrokersからOHLCVデータをダウンロード
# ファイルから銘柄リストを取得して並列処理
# 出力条件：
# MAX_STOCKS     = 30              # 取得銘柄の上限数
# CONCURRENT_REQUESTS = 1          # 同時並列リクエスト数 (推奨: 3〜5)
# EXCLUDE_S17    = ["99"]          # 除外S17コード (ETFなど)
# EXCLUDE_MKTNM  = ["TOKYO PRO MARKET"]  # 除外市場名(プロマーケットなど)
# OUTPUT_FORMAT  = "parquet"           # 出力形式 ("csv" または "parquet")
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
# 入力関連
INPUT_DIR      = r"C:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス\Quants\J-qunats"
INPUT_FILENAME = "jquants_companies_name_20260224.csv"

# 出力・フィルタ条件
MAX_STOCKS     = 30              # 取得銘柄の上限数
CONCURRENT_REQUESTS = 1          # 同時並列リクエスト数 (推奨: 3〜5)
EXCLUDE_S17    = ["99"]          # 除外S17コード (ETFなど)
EXCLUDE_MKTNM  = ["TOKYO PRO MARKET"]  # 除外市場名(プロマーケットなど)
OUTPUT_FORMAT  = "parquet"           # 出力形式 ("csv" または "parquet")

# 出力関連
OUTPUT_DIR     = r"C:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス\Quants\interactivebrokers\ibkr_ohlcv"
# ============================================================

class IBKRDownloader(EWrapper, EClient):
    def __init__(self, stock_list):
        EClient.__init__(self, self)
        self.stock_list = stock_list
        
        # 状態管理用
        self.next_index = 0
        self.active_req_ids = {}      # {reqId: stock_info}
        self.data_store = {}          # {reqId: [bars]}
        self.start_times = {}         # {reqId: start_time}
        self.completed_count = 0
        self.finished = False
        
        self.global_start_time = time.time()
        self.lock = threading.Lock()

    def nextValidId(self, orderId: int):
        print(f"Connected! (Order ID: {orderId})")
        self.process_queue()

    def process_queue(self):
        with self.lock:
            # 並列リクエスト上限までリクエストを発行
            while len(self.active_req_ids) < CONCURRENT_REQUESTS and self.next_index < len(self.stock_list):
                ticker, name = self.stock_list[self.next_index]
                req_id = self.next_index
                
                self.active_req_ids[req_id] = (ticker, name)
                self.data_store[req_id] = []
                self.start_times[req_id] = time.time()
                
                print(f"--- [{self.next_index + 1}/{len(self.stock_list)}] リクエスト送信: {ticker} ({name}) ---")
                
                # 銘柄定義
                contract = Contract()
                contract.symbol = ticker
                contract.secType = "STK"
                contract.exchange = "SMART"
                contract.primaryExchange = "TSEJ"
                contract.currency = "JPY"
                
                # リクエスト
                self.reqHistoricalData(req_id, contract, "", "45 Y", "1 day", "TRADES", 1, 1, False, [])
                
                self.next_index += 1

            # 全て完了したかチェック
            if not self.active_req_ids and self.next_index >= len(self.stock_list):
                print("\n--- すべての銘柄の処理が完了しました ---")
                self.finished = True
                self.disconnect()

    def historicalData(self, reqId: int, bar: BarData):
        if reqId in self.data_store:
            self.data_store[reqId].append([bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume])

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        ticker, name = self.active_req_ids.get(reqId, ("Unknown", "Unknown"))
        data = self.data_store.get(reqId, [])
        
        print(f"--- [{ticker}] 取得完了: {len(data)} 件 ---")
        
        if data:
            df = pd.DataFrame(data, columns=['Date', 'Open', 'High', 'Low', 'Close', 'Volume'])
            today = datetime.now().strftime("%Y%m%d")
            
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            
            if OUTPUT_FORMAT.lower() == "parquet":
                filename = f"ohlcv_ibkr_{ticker}_{name}_{today}.parquet"
                save_path = os.path.join(OUTPUT_DIR, filename)
                df.to_parquet(save_path, index=False)
            else:
                filename = f"ohlcv_ibkr_{ticker}_{name}_{today}.csv"
                save_path = os.path.join(OUTPUT_DIR, filename)
                df.to_csv(save_path, index=False)
                
            print(f"--- [{ticker}] 保存完了: {filename} ---")
        
        # 完了処理と時間表示
        end_time = time.time()
        stock_elapsed = end_time - self.start_times.get(reqId, end_time)
        total_elapsed = end_time - self.global_start_time
        print(f"--- [{ticker}] 処理時間: {stock_elapsed:.2f} 秒 (累計: {total_elapsed:.2f} 秒) ---")

        # 状態リセットして次へ
        with self.lock:
            if reqId in self.active_req_ids:
                del self.active_req_ids[reqId]
            if reqId in self.data_store:
                del self.data_store[reqId]
            self.completed_count += 1
            
        self.process_queue()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        if errorCode in [2104, 2106, 2107, 2158, 2108]:
            return
        
        ticker, name = self.active_req_ids.get(reqId, ("Unknown", "Unknown"))
        print(f"Error for {ticker} (ID {reqId}): {errorCode} {errorString}")
        
        # 銘柄特有のエラーの場合
        if errorCode in [200, 162, 321, 322, 167]:
            print(f"--- {ticker} をスキップします ---")
            with self.lock:
                if reqId in self.active_req_ids:
                    del self.active_req_ids[reqId]
            self.process_queue()
            return

        # 接続エラー等の致命的なエラー
        if errorCode in [1100, 1101, 1102, 502]:
            self.finished = True
            self.disconnect()

def main():
    # 1. CSVから銘柄リストを読み込み
    csv_path = os.path.join(INPUT_DIR, INPUT_FILENAME)
    if not os.path.exists(csv_path):
        print(f"Error: 入力ファイルが見つかりません: {csv_path}")
        return

    print(f"--- ファイルを読み込み中: {INPUT_FILENAME} ---")
    df_src = pd.read_csv(csv_path)
    
    # フィルタリング
    df_src = df_src[~df_src['S17'].astype(str).isin(EXCLUDE_S17)]
    df_src = df_src[~df_src['MktNm'].astype(str).isin(EXCLUDE_MKTNM)]
    
    stock_list = []
    for _, row in df_src.iterrows():
        code_full = str(row['Code'])
        ticker = code_full[:4]
        name = str(row['CoName'])
        stock_list.append((ticker, name))
        if len(stock_list) >= MAX_STOCKS:
            break

    if not stock_list:
        print("Error: 処理対象の銘柄がありません")
        return

    print(f"--- 処理対象: {len(stock_list)} 銘柄 / 同時実行数: {CONCURRENT_REQUESTS} ---")

    # 2. IBKR接続開始
    app = IBKRDownloader(stock_list)
    print("Connecting to 127.0.0.1:4001 (ClientId=3)...")
    app.connect("127.0.0.1", 4001, clientId=3)
    
    app_thread = threading.Thread(target=app.run, daemon=True)
    app_thread.start()
    
    # タイムアウト設定 (銘柄数に応じて調整)
    timeout = len(stock_list) * 45 + 30
    start_time = time.time()
    while not app.finished and time.time() - start_time < timeout:
        time.sleep(1)
    
    if not app.finished:
        print("\nTimed out. Disconnecting...")
        app.disconnect()
    
    print("\nプロセス終了")
    sys.exit()

if __name__ == "__main__":
    main()