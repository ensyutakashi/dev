# --- obsidian_property ---
# scr名: 【自動】
# 概要: TDnetよりPDF・XBRL差分ダウンロードし年別・月別フォルダへ整理
# 処理grp: TDnetダウンロード
# 処理順番: 4
# mermaid: "[[mermaid_TDnet適時開示情報ダウンロード]]"
# tags: ["tdnet", "download", "diff"]
# aliases: ["04_tdnet_pdf_download.py"]
# created: 2026-03-10
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：TDnetからPDF・XBRLファイルをダウンロードし、年別・月別フォルダに自動整理するスクリプト
# --- 概要 ---
import os
import glob
import csv
import requests
from datetime import datetime
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import shutil
import re

# =================================================================
# 1. 設定エリア
# =================================================================
TARGET_DIR = r'C:\Users\ensyu\_myfolder\work\dev\TDnet適時情報開示サービス'
# 環境変数から作業フォルダを取得（なければデフォルト）
WORKING_DIR = os.environ.get('TDNET_WORKING_DIR', os.path.join(TARGET_DIR, "TDnet_report_temp_validation_log_files"))
os.makedirs(WORKING_DIR, exist_ok=True)  # フォルダがなければ作成

# 最終保存先ベースフォルダ
PDF_BASE_DIR = r'\\LS720D7A9\TakashiBK\投資\TDNET\TDnet適時情報開示サービス\TDnet(決算短信)PDF'
XBRL_BASE_DIR = r'\\LS720D7A9\TakashiBK\投資\TDNET\TDnet適時情報開示サービス\TDnet(決算短信)XBRL'

# ダウンロード設定
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}
TIMEOUT = 30
RETRY_COUNT = 3
MAX_WORKERS = 15  # 並列ダウンロード数
# =================================================================

def get_timestamp_msg(msg):
    return f"{msg} {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}"

def extract_date_from_filename(filename):
    """ファイル名から年月を抽出"""
    # ファイル名形式: 連番_公開日(YYMMDD)_時刻(HHMM)_種別_決算月(YYMM)_四半期(nQ)_コード_会社名_表題[_XBRL]
    # 公開日部分(YYMMDD)を抽出
    match = re.search(r'_(\d{6})_', filename)
    if match:
        date_str = match.group(1)  # YYMMDD
        year = int(date_str[:2]) + 2000  # YY → 20YY
        month = int(date_str[2:4])
        return year, month
    return None, None

def create_target_directory(base_dir, year, month):
    """年別・月別フォルダを作成"""
    year_dir = os.path.join(base_dir, f"{year}年")
    # base_dirからベース名を取得（例: "TDnet(決算短信)PDF"）
    base_name = os.path.basename(base_dir)
    # 月別フォルダ名: TDnet(決算短信)PDF2026年03月
    month_dir = os.path.join(year_dir, f"{base_name}{year}年{month:02d}月")
    
    os.makedirs(year_dir, exist_ok=True)
    os.makedirs(month_dir, exist_ok=True)
    
    return month_dir

def get_direct_download_path(filename, base_dir):
    """直接ダウンロード先のパスを取得"""
    year, month = extract_date_from_filename(filename)
    
    if year is None or month is None:
        print(f"警告: {filename} から日付を抽出できませんでした")
        return None
    
    target_dir = create_target_directory(base_dir, year, month)
    return os.path.join(target_dir, filename)

def find_latest_diff_file(target_dir: str) -> str:
    """最新の差分データファイルを探す"""
    # 作業フォルダを優先的に探す
    search_dirs = [WORKING_DIR, target_dir]
    
    all_files = []
    for search_dir in search_dirs:
        pattern = os.path.join(search_dir, "TDNET差分データ_*.csv")
        files = glob.glob(pattern)
        all_files.extend(files)
    
    if not all_files:
        print("差分データファイルが見つかりません")
        return None # type: ignore
    
    # デバッグ用に全ファイルを表示
    print("検出された差分ファイル:")
    for f in sorted(all_files):
        print(f"  {os.path.basename(f)}")
    
    # ファイル名からタイムスタンプを抽出して最新を取得
    def extract_timestamp(filename):
        basename = os.path.basename(filename)
        # TDNET差分データ_YYYYMMDD_HHMMSS.csv からタイムスタンプ部分を抽出
        try:
            # 正規表現でタイムスタンプ部分を抽出
            import re
            match = re.search(r'TDNET差分データ_(\d{8})_(\d{6})\.csv', basename)
            if match:
                date_part = match.group(1)  # YYYYMMDD
                time_part = match.group(2)  # HHMMSS
                timestamp_str = f"{date_part}_{time_part}"
                return datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
        except Exception as e:
            print(f"タイムスタンプ抽出エラー: {basename} - {e}")
            return datetime.min
        return datetime.min
    
    # タイムスタンプでソートして最新を取得
    latest_file = max(all_files, key=extract_timestamp)
    print(f"最新差分ファイル: {os.path.basename(latest_file)}")
    return latest_file

def download_file(url: str, file_path: str, file_type: str) -> str:
    """ファイルをダウンロード"""
    if not url or url.strip() == "":
        return f"失敗: URLが空です"
    
    try:
        for attempt in range(RETRY_COUNT):
            try:
                response = requests.get(url, headers=HEADERS, timeout=TIMEOUT, stream=True)
                response.raise_for_status()
                
                # ディレクトリ作成
                os.makedirs(os.path.dirname(file_path), exist_ok=True)
                
                # ファイル保存
                with open(file_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                
                result = "成功" if os.path.getsize(file_path) > 0 else "失敗: 空ファイル"
                return get_timestamp_msg(result)
                
            except requests.RequestException as e:
                if attempt < RETRY_COUNT - 1:
                    time.sleep(2 ** attempt)  # 指数バックオフ
        
        return f"失敗: {str(e)}" # type: ignore
        
    except Exception as e:
        return f"失敗: {str(e)}"

def organize_downloaded_files():
    """ダウンロード済みファイルを年別・月別フォルダに整理（直接ダウンロード方式では不要）"""
    print("直接ダウンロード方式のため、この関数は使用されません")
    pass

def process_diff_file(csv_file_path: str):
    """差分CSVファイルを処理してPDF/XBRLをダウンロード"""
    print(f"\n=== 差分ファイル処理開始 ===")
    print(f"対象ファイル: {csv_file_path}")
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            # 列インデックスを特定
            filename_col = None
            pdf_url_col = None
            xbrl_url_col = None
            
            for i, col_name in enumerate(header):
                col_clean = col_name.strip('"')
                if 'ファイル名' in col_clean and '連番+公開日+時刻' in col_clean:
                    filename_col = i
                elif col_clean == '表題_URL':
                    pdf_url_col = i
                elif col_clean == 'XBRL_URL':
                    xbrl_url_col = i
            
            if filename_col is None:
                print("ファイル名列が見つかりません")
                return
            
            print(f"ファイル名列インデックス: {filename_col}")
            print(f"PDF URL列インデックス: {pdf_url_col}")
            print(f"XBRL URL列インデックス: {xbrl_url_col}")
            
            # タスクを収集
            tasks = []
            for row in reader:
                # ファイル名を取得
                if filename_col < len(row):
                    filename = row[filename_col].strip('"')
                    if not filename:
                        continue
                    
                    task = {"filename": filename, "pdf_url": None, "xbrl_url": None}
                    
                    # PDF URL
                    if pdf_url_col is not None and pdf_url_col < len(row):
                        pdf_url = row[pdf_url_col].strip('"')
                        if pdf_url and pdf_url.strip():
                            task["pdf_url"] = pdf_url
                    
                    # XBRL URL
                    if xbrl_url_col is not None and xbrl_url_col < len(row):
                        xbrl_url = row[xbrl_url_col].strip('"')
                        if xbrl_url and xbrl_url.strip() and xbrl_url != '""':
                            task["xbrl_url"] = xbrl_url
                    
                    # URLがある場合のみタスクに追加
                    if task["pdf_url"] or task["xbrl_url"]:
                        tasks.append(task)
            
            if not tasks:
                print("ダウンロード対象のURLがありません")
                return
            
            print(f"ダウンロード対象: {len(tasks)}件")
            
            # 並列ダウンロード実行
            dl_start_time = time.time()
            results = []
            
            def execute_task(task):
                result = {"filename": task["filename"], "pdf_msg": None, "xbrl_msg": None}
                
                # PDFダウンロード（直接ダウンロード）
                if task["pdf_url"]:
                    pdf_filename = f"{task['filename']}.pdf"
                    pdf_path = get_direct_download_path(pdf_filename, PDF_BASE_DIR)
                    if pdf_path:
                        result["pdf_msg"] = download_file(task["pdf_url"], pdf_path, "PDF")
                    else:
                        result["pdf_msg"] = "失敗: ダウンロードパス生成エラー"
                
                # XBRLダウンロード（直接ダウンロード）
                if task["xbrl_url"]:
                    xbrl_filename = f"{task['filename']}.zip"
                    xbrl_path = get_direct_download_path(xbrl_filename, XBRL_BASE_DIR)
                    if xbrl_path:
                        result["xbrl_msg"] = download_file(task["xbrl_url"], xbrl_path, "XBRL")
                    else:
                        result["xbrl_msg"] = "失敗: ダウンロードパス生成エラー"
                
                return result
            
            # 進捗表示付きで実行
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_task = {executor.submit(execute_task, t): t for t in tasks}
                completed_count = 0
                for future in as_completed(future_to_task):
                    results.append(future.result())
                    completed_count += 1
                    if completed_count % 10 == 0 or completed_count == len(tasks):
                        print(f"  進捗: {completed_count}/{len(tasks)} 件完了...", end="\r")
            
            dl_end_time = time.time()
            print(f"\nダウンロード完了。")
            
            # 結果集計
            pdf_success = sum(1 for r in results if r["pdf_msg"] and "成功" in r["pdf_msg"])
            xbrl_success = sum(1 for r in results if r["xbrl_msg"] and "成功" in r["xbrl_msg"])
            pdf_total = sum(1 for r in results if r["pdf_msg"])
            xbrl_total = sum(1 for r in results if r["xbrl_msg"])
            
            # --- 時間計算 ---
            total_elapsed = dl_end_time - dl_start_time
            avg_speed = total_elapsed / len(tasks) if tasks else 0

            print("\n" + "="*45)
            print(f" 【ダウンロード＆整理処理結果概要】")
            print(f"  DL純処理時間: {int(total_elapsed // 60)}分 {int(total_elapsed % 60)}秒")
            print(f"  平均DL速度  : {avg_speed:.2f} 秒/件")
            print(f"  PDF取得     : {pdf_success}/{pdf_total} 件")
            print(f"  XBRL取得    : {xbrl_success}/{xbrl_total} 件")
            print(f"  ※直接ダウンロード方式で年別・月別フォルダに保存完了")
            print("="*45)
            
    except Exception as e:
        print(f"処理エラー: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("=== TDnet PDF/XBRL ダウンロード＆整理ツール ===")
    
    # 処理開始時間を記録
    start_time = datetime.now()
    print(f"処理開始時間: {start_time.strftime('%Y/%m/%d %H:%M:%S')}")
    
    # 最新の差分データファイルを探す
    latest_file = find_latest_diff_file(TARGET_DIR)
    
    if latest_file:
        # 差分ファイルを処理
        process_diff_file(latest_file)
    else:
        print("最新の差分ファイルが見つかりませんでした")
    
    # 処理終了時間を記録して表示
    end_time = datetime.now()
    total_elapsed = end_time - start_time
    print(f"\n処理終了時間: {end_time.strftime('%Y/%m/%d %H:%M:%S')}")
    print(f"総処理時間: {int(total_elapsed.total_seconds() // 60)}分 {int(total_elapsed.total_seconds() % 60)}秒")
    print("=== 全処理完了 ===")

if __name__ == '__main__':
    main()
