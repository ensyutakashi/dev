#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
最新の銘柄コードを取得するスクリプト
日本取引所グループのサイトから銘柄情報をダウンロードしてCSVに変換

参考: https://www.jpx.co.jp/markets/indices/line-up/files/f_all_20241128.xlsx
"""

import requests
import pandas as pd
import os
import sys
from datetime import datetime
import time

def download_stock_codes():
    """日本取引所グループから銘柄コードをダウンロード"""
    
    # 東証、名証、福証、札証の全銘柄情報ExcelファイルURL
    # 最新のURLは日本取引所グループサイトで確認が必要
    excel_urls = [
        "https://www.jpx.co.jp/markets/indices/line-up/files/f_all_20241128.xlsx",  # 東証等
    ]
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    for url in excel_urls:
        try:
            print(f"ダウンロード中: {url}")
            
            # ファイル名をURLから取得
            filename = os.path.basename(url)
            filepath = os.path.join(script_dir, filename)
            
            # ダウンロード
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            print(f"ダウンロード完了: {filepath}")
            
            # Excelを読み込んでCSVに変換
            convert_excel_to_csv(filepath, script_dir)
            
        except Exception as e:
            print(f"エラー: {url} のダウンロードに失敗しました: {e}")
            continue

def convert_excel_to_csv(excel_path, output_dir):
    """ExcelファイルをCSVに変換"""
    
    try:
        print(f"Excelファイル読み込み中: {excel_path}")
        
        # Excelファイルを読み込み（複数シートの場合に対応）
        all_data = []
        
        # まずシート名一覧を取得
        xls = pd.ExcelFile(excel_path)
        print(f"シート一覧: {xls.sheet_names}")
        
        for sheet_name in xls.sheet_names:
            try:
                df = pd.read_excel(excel_path, sheet_name=sheet_name)
                print(f"シート '{sheet_name}': {len(df)} 行")
                
                # 必要な列があるか確認
                if 'コード' in df.columns or '証券コード' in df.columns:
                    all_data.append(df)
                else:
                    print(f"警告: シート '{sheet_name}' にコード列がありません")
                    print(f"列名: {df.columns.tolist()}")
                    
            except Exception as e:
                print(f"警告: シート '{sheet_name}' の読み込みに失敗しました: {e}")
                continue
        
        if not all_data:
            print("エラー: 有効なデータが見つかりませんでした")
            return
        
        # 全データを結合
        df_combined = pd.concat(all_data, ignore_index=True)
        print(f"結合後のデータ数: {len(df_combined)} 行")
        
        # コード列の名前を統一
        if '証券コード' in df_combined.columns:
            df_combined = df_combined.rename(columns={'証券コード': 'コード'})
        
        # コード列を文字列として処理（先頭の0を保持）
        if 'コード' in df_combined.columns:
            df_combined['コード'] = df_combined['コード'].astype(str).str.zfill(4)
        
        # CSVとして保存
        csv_path = os.path.join(output_dir, "stock_code.csv")
        df_combined.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        print(f"CSV保存完了: {csv_path}")
        print(f"銘柄数: {len(df_combined)}")
        
        # 基本情報を表示
        if '銘柄名' in df_combined.columns:
            print("\nサンプルデータ:")
            print(df_combined[['コード', '銘柄名']].head(10))
        
        return csv_path
        
    except Exception as e:
        print(f"エラー: Excelファイルの変換に失敗しました: {e}")
        return None

def get_alternative_stock_codes():
    """代替手段：Webスクレイピングで銘柄コードを取得"""
    
    print("代替手段で銘柄コードを取得します...")
    
    # ここにkabutan.jpなどのサイトからスクレイピングする処理を追加できる
    # 今回はサンプルとして基本的な東証一部の銘柄コードを生成
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # サンプルデータ（実際にはWebスクレイピングで取得）
    sample_codes = [
        {'コード': '7203', '銘柄名': 'トヨタ自動車', '市場': '東証プライム'},
        {'コード': '6758', '銘柄名': 'ソニーグループ', '市場': '東証プライム'},
        {'コード': '9984', '銘柄名': 'ソフトバンク', '市場': '東証プライム'},
        {'コード': '6861', '銘柄名': 'キーエンス', '市場': '東証プライム'},
        {'コード': '9983', '銘柄名': 'ファストリテイリング', '市場': '東証プライム'},
        {'コード': '8035', '銘柄名': '東京エレクトロン', '市場': '東証プライム'},
        {'コード': '6501', '銘柄名': '日立', '市場': '東証プライム'},
        {'コード': '6702', '銘柄名': '住友電気工業', '市場': '東証プライム'},
        {'コード': '4502', '銘柄名': '武田薬品工業', '市場': '東証プライム'},
        {'コード': '8316', '銘柄名': '三井住友FG', '市場': '東証プライム'},
    ]
    
    df = pd.DataFrame(sample_codes)
    
    csv_path = os.path.join(script_dir, "stock_code.csv")
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    
    print(f"サンプルCSV保存完了: {csv_path}")
    print(f"銘柄数: {len(df)}")
    
    return csv_path

def main():
    """メイン処理"""
    
    print("銘柄コード取得スクリプトを開始します")
    
    # 日本取引所グループからダウンロード
    csv_path = download_stock_codes()
    
    # ダウンロード失敗時は代替手段を使用
    if not csv_path or not os.path.exists(csv_path):
        print("\n日本取引所グループからのダウンロードに失敗しました")
        print("代替手段で銘柄コードを取得します...")
        csv_path = get_alternative_stock_codes()
    
    if csv_path and os.path.exists(csv_path):
        print(f"\n銘柄コードの取得が完了しました: {csv_path}")
        print("このファイルを使って株式情報を収集できます")
    else:
        print("\nエラー: 銘柄コードの取得に失敗しました")
        print("手動で銘柄コードCSVファイルを用意してください")

if __name__ == "__main__":
    main()
