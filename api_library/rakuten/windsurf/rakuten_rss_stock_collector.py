#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
楽天RSSで株式情報を自動収集するスクリプト
参考: https://qiita.com/heroshi/items/43718f55ab4f695fb0cd

要件:
- 楽天証券のマーケットスピードII RSSが接続されているExcelが必要
- 銘柄コードCSVファイルが必要
"""

import win32com.client
from openpyxl import Workbook
import pandas as pd
import time
import pywintypes
import datetime
import os
import sys

def main():
    print("楽天RSSで株式情報収集中...")
    
    # --- 設定 ---
    rss_list = [
        '銘柄コード', '銘柄名称', '現在日付', '現在値', '時価総額', '単位株数', '配当',
        'PER', 'PBR', '信用売残', '信用売残前週比', '信用買残', '信用買残前週比',
        '信用倍率', '配当落日', '中配落日', '権利落日', '決算発表日',
        '年初来高値', '年初来安値', '上場来高値', '上場来安値'
    ]
    
    cal_list = ['購入可能額', '配当利回り']
    add_list = rss_list + cal_list
    
    step_no = 500  # RSS 一度に取得する銘柄数
    
    # --- CSV から銘柄コードリスト取得 ---
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(script_dir, "stock_code.csv")
    
    if not os.path.exists(csv_path):
        print(f"エラー: 銘柄コードファイルが見つかりません: {csv_path}")
        print("先に get_stock_codes.py を実行して銘柄コードを取得してください")
        return
    
    try:
        df_codes = pd.read_csv(csv_path, encoding="utf-8")
        stock_codes = df_codes['コード'].tolist()
        print(f"銘柄数: {len(stock_codes)}")
    except Exception as e:
        print(f"エラー: 銘柄コードファイルの読み込みに失敗しました: {e}")
        return
    
    # --- Excel 起動 ---
    try:
        xl = win32com.client.GetObject(Class="Excel.Application")
    except pywintypes.com_error:
        print("エラー: Excel が開いていません。")
        print("マーケットスピードII RSSに接続したExcelを起動してから再実行してください")
        return
    
    xl.Visible = True
    
    # RSSのタイトル行を記入
    for col, item in enumerate(rss_list):
        xl.Cells(1, col + 2).Value = item
    
    # --- Excel 新規ブック作成（openpyxl側） ---
    wb_new = Workbook()
    if wb_new.active is None:
        ws_new = wb_new.create_sheet(title="Sheet1")
    else:
        ws_new = wb_new.active
    
    # ヘッダー行を追加
    ws_new.append(add_list)
    
    # --- Excel セル書き込み用再試行関数 ---
    def retry_excel_set(cell, value, formula=False, max_retry=10):
        for i in range(max_retry):
            try:
                if formula:
                    cell.Formula = value
                else:
                    cell.Value = value
                return True
            except pywintypes.com_error:
                wait = 1 * (i+1)  # 1秒, 2秒, 3秒...最大10秒待つ
                print(f"セル書き込みリトライ {i+1}回目 ({wait}秒待機)")
                time.sleep(wait)
        return False
    
    # --- RSS データ取得 ---
    total_processed = 0
    
    for i in range(0, len(stock_codes), step_no):
        batch_codes = stock_codes[i:i + step_no]
        
        # Excel 表クリア
        xl.Range(xl.Cells(2, 1), xl.Cells(step_no + 2, len(rss_list) + 1)).ClearContents()
        
        # RSS 表に銘柄コードと関数を書き込み
        for row, stock_no in enumerate(batch_codes):
            retry_excel_set(xl.Cells(row + 2, 1), str(stock_no))
            for col, item in enumerate(rss_list):
                formula = f'=RssMarket("{stock_no}","{item}")'
                retry_excel_set(xl.Cells(row + 2, col + 2), formula, formula=True)
        
        # RSS 表の値が更新されるまで少し待つ
        print(f"RSSデータ更新待機中... (バッチ {i//step_no + 1}/{(len(stock_codes)-1)//step_no + 1})")
        time.sleep(5)
        
        # Python 側でデータ取得
        batch_data = []
        for row, stock_no in enumerate(batch_codes):
            row_data = [xl.Cells(row + 2, col + 2).Value for col in range(len(rss_list))]
            
            # 計算列
            current_price = row_data[rss_list.index("現在値")]
            unit_share = row_data[rss_list.index("単位株数")]
            dividend = row_data[rss_list.index("配当")]
            
            # Excel から取得した値を安全に float に変換
            def to_float(val):
                try:
                    return float(val) if val is not None else 0.0
                except (ValueError, TypeError):
                    return 0.0
            
            current_price_f = to_float(current_price)
            unit_share_f = to_float(unit_share)
            dividend_f = to_float(dividend)
            
            purchase_amount = current_price_f * unit_share_f if current_price_f and unit_share_f else 0
            dividend_yield = (dividend_f / current_price_f * 100) if current_price_f else 0
            
            row_data.append(purchase_amount)
            row_data.append(dividend_yield)
            batch_data.append(row_data)
        
        # DataFrame（今回はフィルタなし）
        df_batch = pd.DataFrame(batch_data, columns=add_list)
        
        # Excel 書き込み
        for r in df_batch.values.tolist():
            ws_new.append(r)
        
        total_processed = min(i + step_no, len(stock_codes))
        print(f"{total_processed}/{len(stock_codes)} 銘柄処理完了")
    
    # --- 保存 ---
    now = datetime.datetime.now()
    output_dir = os.path.join(script_dir, "output")
    os.makedirs(output_dir, exist_ok=True)
    
    filename = os.path.join(output_dir, f"銘柄収集_{now:%Y%m%d_%H%M}.xlsx")
    wb_new.save(filename)
    
    print(f"\n完了: {filename}")
    print(f"全{len(stock_codes)}銘柄の情報を収集しました")

if __name__ == "__main__":
    main()
