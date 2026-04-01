# --- obsidian_property ---
# scr名: 【自動】
# 概要: 差分データをDB格納
# 処理grp: TDnetダウンロード
# 処理順番: 5
# mermaid: "[[mermaid_TDnet適時開示情報ダウンロード]]"
# tags: ["tdnet", "download", "diff"]
# aliases: ["05_tdnet_db_uploader.py"]
# created: 2026-02-17
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：差分データをDB格納
# --- 概要 ---

import os
import glob
import duckdb
from datetime import datetime
import re

# =================================================================
# 1. 設定エリア
# =================================================================
TARGET_DIR = r'C:\Users\ensyu\_myfolder\work\dev\TDnet適時情報開示サービス'
# 環境変数から作業フォルダを取得（なければデフォルト）
WORKING_DIR = os.environ.get('TDNET_WORKING_DIR', os.path.join(TARGET_DIR, "TDnet_report_temp_validation_log_files"))
os.makedirs(WORKING_DIR, exist_ok=True)  # フォルダがなければ作成
DB_PATH = r'C:\Users\ensyu\_myfolder\work\dev\TDnet適時情報開示サービス\tdnet.duckdb'

# NASバックアップ設定
NAS_BACKUP_ENABLED = True  # NASバックアップを有効にするかどうか
NAS_BACKUP_FOLDER = r'\\LS720D7A9\TakashiBK\投資\BackUp\DB'  # NASバックアップ先フォルダ
NAS_BACKUP_FILENAME_PREFIX = 'tdnet_backup_'  # バックアップファイル名の接頭辞
# =================================================================

def find_latest_diff_file(target_dir):
    """最新の差分データファイルを探す（元の機能を完全維持）"""
    # 作業フォルダを優先的に探す
    search_dirs = [WORKING_DIR, target_dir]
    
    all_files = []
    for search_dir in search_dirs:
        pattern = os.path.join(search_dir, "TDNET差分データ_*.csv")
        files = glob.glob(pattern)
        all_files.extend(files)
    
    if not all_files:
        print("差分データファイルが見つかりません")
        return None
    
    print("検出された差分ファイル:")
    for f in sorted(all_files):
        print(f"  {os.path.basename(f)}")
    
    # ファイル名からタイムスタンプを抽出して最新を取得
    def extract_timestamp(filename):
        basename = os.path.basename(filename)
        try:
            # 正規表現でタイムスタンプ部分を抽出
            match = re.search(r'TDNET差分データ_(\d{8})_(\d{6})\.csv', basename)
            if match:
                date_part = match.group(1)  # YYYYMMDD
                time_part = match.group(2)  # HHMMSS
                return datetime.strptime(f"{date_part}{time_part}", "%Y%m%d%H%M%S")
        except Exception:
            pass
        return datetime.min

    latest_file = max(all_files, key=extract_timestamp)
    return latest_file

def upload_csv_to_db(csv_path, db_path):
    """CSVをDuckDBにアップロード（エラー箇所を修正）"""
    try:
        print(f"DBアップロード開始: {os.path.basename(csv_path)}")
        
        con = duckdb.connect(db_path)
        
        # テーブルが存在しない場合は作成（全カラムをVARCHARとして定義）
        # 最初の1行からカラム名を取得
        id_col = "連番" # 重複チェック用の主キー候補
        
        # テーブルの存在確認
        table_exists = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'disclosure_info'").fetchone()[0] # type: ignore
        
        if not table_exists:
            # 初回作成: QUOTE='"' を指定して読み込む
            con.execute(f"""
                CREATE TABLE disclosure_info AS 
                SELECT * FROM read_csv_auto('{csv_path}', ALL_VARCHAR=TRUE, QUOTE='"')
            """)
        
        initial_count = con.execute("SELECT count(*) FROM disclosure_info").fetchone()[0] # type: ignore
        
        # 差分インポート（重複を除外して挿入）
        # QUOTEオプションを '"' に修正し、エスケープ処理を標準に合わせる
        import_query = f"""
            INSERT INTO disclosure_info 
            SELECT * FROM read_csv_auto(
                '{csv_path}', 
                ALL_VARCHAR=TRUE, 
                QUOTE='"'
            ) AS new_data
            WHERE NOT EXISTS (
                SELECT 1 FROM disclosure_info 
                WHERE disclosure_info.{id_col} = new_data.{id_col}
            )
        """
        
        con.execute(import_query)
        
        # 処理後の件数確認
        final_count = con.execute("SELECT count(*) FROM disclosure_info").fetchone()[0] # type: ignore
        uploaded_count = final_count - initial_count
        
        # CSVの総行数を確認（スキップ数算出用）
        total_csv_rows = con.execute(f"SELECT count(*) FROM read_csv_auto('{csv_path}', QUOTE='\"')").fetchone()[0] # type: ignore
        skipped_count = total_csv_rows - uploaded_count
        
        con.commit()
        con.close()
        
        print(f"\n=== アップロード結果 ===")
        print(f"アップロード成功: {uploaded_count} 件")
        print(f"スキップ（重複）: {skipped_count} 件")
        print(f"エラー: 0 件")
        print(f"✅ DBアップロード完了（総計: {final_count} 件）")
        
    except Exception as e:
        print(f"❌ アップロードエラー: {e}")
        import traceback
        traceback.print_exc()

def backup_db_to_nas(db_path, backup_folder, filename_prefix):
    """DBファイルをNASにバックアップする"""
    if not NAS_BACKUP_ENABLED:
        print("NASバックアップは無効になっています")
        return
    
    try:
        # バックアップ先フォルダの存在確認と作成
        os.makedirs(backup_folder, exist_ok=True)
        
        # タイムスタンプ付きのバックアップファイル名を生成
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"{filename_prefix}{timestamp}.duckdb"
        backup_path = os.path.join(backup_folder, backup_filename)
        
        print(f"\n=== NASバックアップ開始 ===")
        print(f"元ファイル: {db_path}")
        print(f"バックアップ先: {backup_path}")
        
        # DBファイルをコピー
        import shutil
        shutil.copy2(db_path, backup_path)
        
        # バックアップ成功を確認
        if os.path.exists(backup_path):
            file_size = os.path.getsize(backup_path)
            print(f"✅ NASバックアップ完了")
            print(f"ファイルサイズ: {file_size:,} バイト")
        else:
            print("❌ バックアップファイルの作成に失敗しました")
            
    except Exception as e:
        print(f"❌ NASバックアップエラー: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("=== TDnet DB アップローダー (特殊CSV対応・高速版) ===")
    
    latest_csv = find_latest_diff_file(TARGET_DIR)
    
    if latest_csv:
        upload_csv_to_db(latest_csv, DB_PATH)
        
        # DBアップロード成功後にNASバックアップを実行
        backup_db_to_nas(DB_PATH, NAS_BACKUP_FOLDER, NAS_BACKUP_FILENAME_PREFIX)
    else:
        print("処理対象のファイルが見つかりませんでした。")

if __name__ == "__main__":
    main()