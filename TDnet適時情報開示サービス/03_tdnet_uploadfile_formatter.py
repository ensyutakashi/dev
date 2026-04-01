# --- obsidian_property ---
# scr名: 【自動】
# 概要: TDnet差分データ加工(連番付加、ファイル名生成)
# 処理grp: TDnetダウンロード
# 処理順番: 3
# mermaid: "[[mermaid_TDnet適時開示情報ダウンロード]]"
# tags: ["tdnet", "download", "diff"]
# aliases: ["03_tdnet_uploadfile_formatter.py"]
# created: 2026-02-17
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：TDnet差分データ加工
# TDnet差分データ_YYMMDD_HHMMSS.csvに命名するファイル名やDBデータを作成する
#    - 種別(決算短信/中期経営など)
#    - 決算期
#    - 四半期(1Q,2Q,3Q,4Q)
#    - 禁則文字
# --- 概要 ---

import os
import glob
import re
import unicodedata
import csv
from datetime import datetime
from calendar import monthrange

# =================================================================
# 1. 設定エリア
# =================================================================
TARGET_DIR = r'C:\Users\ensyu\_myfolder\work\dev\TDnet適時情報開示サービス'
# 環境変数から作業フォルダを取得（なければデフォルト）
WORKING_DIR = os.environ.get('TDNET_WORKING_DIR', os.path.join(TARGET_DIR, "TDnet_report_temp_validation_log_files"))
os.makedirs(WORKING_DIR, exist_ok=True)  # フォルダがなければ作成
# =================================================================

ERA_TO_YEAR = {
    '令和': 2018, '平成': 1988, '昭和': 1925, '大正': 1911, '明治': 1867
}

# 禁則文字のマッピング（半角→全角）
mapping = {
    "/": "／",
    "\\": "￥",
    ":": "：",
    "*": "＊",
    "?": "？",
    '"': "＂",
    "<": "＜",
    ">": "＞",
    "|": "｜"
}

def normalize_text(text):
    if not text or not isinstance(text, str): 
        return ""
    normalized = unicodedata.normalize('NFKC', text)
    return re.sub(r'\s+', ' ', normalized).strip()

def era_to_western(era_name, era_year_str):
    base_year = ERA_TO_YEAR.get(era_name)
    if base_year is None: 
        return None
    era_year = 1 if era_year_str == '元' else int(era_year_str)
    return base_year + era_year

def extract_report_type(text):
    normalized = normalize_text(text)
    keywords = ["業績予想", "事業計画", "中期経営", "決算説明", "決算短信"]
    for kw in keywords:
        if kw in normalized: 
            return kw
    return None

def extract_fiscal_period(text):
    normalized = normalize_text(text)
    p1 = r'(\d{4})年([1-9]|1[0-2])月(期)?'
    m1 = re.search(p1, normalized)
    if m1: 
        return (int(m1.group(1)), int(m1.group(2)))
    p2 = r'(令和|平成|昭和|大正|明治)(元|\d+)年([1-9]|1[0-2])月(期)?'
    m2 = re.search(p2, normalized)
    if m2:
        year = era_to_western(m2.group(1), m2.group(2))
        if year: 
            return (year, int(m2.group(3)))
    return None

def extract_quarter(text):
    normalized = normalize_text(text)
    p1 = r'([1-4])\s*Q|Q\s*([1-4])'
    m1 = re.search(p1, normalized, re.IGNORECASE)
    if m1: 
        return f"{m1.group(1) or m1.group(2)}Q"
    p2 = r'第\s*([一二三四１２３４1-4])\s*四\s*半\s*期'
    m2 = re.search(p2, normalized)
    if m2:
        q_map = {'一':'1','二':'2','三':'3','四':'4','１':'1','２':'2','３':'3','４':'4','1':'1','2':'2','3':'3','4':'4'}
        return f"{q_map.get(m2.group(1), '4')}Q"
    if re.search(r'上半期|上期|中間期|中間', normalized): 
        return '2Q'
    if re.search(r'下半期|下期|通期', normalized): 
        return '4Q'
    return None

def find_latest_diff_file(target_dir):
    """最新のTDNET差分データファイルを探す"""
    # 作業フォルダを優先的に探す
    search_dirs = [WORKING_DIR, target_dir]
    
    latest_file = None
    latest_timestamp = None
    
    for search_dir in search_dirs:
        pattern = os.path.join(search_dir, "TDNET差分データ_*.csv")
        files = glob.glob(pattern)
        
        if not files:
            continue
        
        for file in files:
            filename = os.path.basename(file)
            # ファイル名からタイムスタンプ部分を抽出: TDNET差分データ_YYYYMMDD_HHMMSS.csv
            match = re.search(r'TDNET差分データ_(\d{8})_(\d{6})\.csv', filename)
            if match:
                date_str = match.group(1)
                time_str = match.group(2)
                timestamp_str = f"{date_str}_{time_str}"
                timestamp = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
            
            if latest_timestamp is None or timestamp > latest_timestamp:
                latest_timestamp = timestamp
                latest_file = file
    
    return latest_file

def generate_filename(seq_num, pub_date, time_str, report_type, fiscal_period, quarter, code, company_name, title, xbrl_data=None):
    """ファイル名を生成（ExcelのM列の形式を参考）"""
    # 連番（6桁ゼロ埋め）- numpy.int64型に対応
    try:
        seq_str = f"{int(seq_num):06d}"
    except (ValueError, TypeError):
        # int()変換に失敗した場合のフォールバック
        seq_str = str(seq_num).zfill(6)
        if len(seq_str) > 6:
            seq_str = seq_str[:6]
    
    # 公開日（YYMMDD形式）
    if isinstance(pub_date, str):
        try:
            # スペースで分割して最初の日付部分を取得
            date_part = pub_date.split()[0]
            # ハイフンもスラッシュも対応
            if '-' in date_part:
                pub_date = datetime.strptime(date_part, '%Y-%m-%d')
            else:
                pub_date = datetime.strptime(date_part, '%Y/%m/%d')
        except:
            pub_date = datetime.strptime(pub_date, '%Y/%m/%d')
    pub_date_str = pub_date.strftime('%y%m%d')
    
    # 時刻（HHMM形式）
    if isinstance(time_str, str):
        try:
            # 日付と時刻が組み合わさった形式に対応
            if ' ' in time_str:
                time_part = time_str.split()[-1]  # 最後の部分を時刻として取得
            else:
                time_part = time_str
            
            # 時刻の形式を試す
            time_formats = ['%H:%M:%S', '%H:%M', '%H%M']
            for fmt in time_formats:
                try:
                    time_obj = datetime.strptime(time_part, fmt)
                    time_str_formatted = time_obj.strftime('%H%M')
                    break
                except ValueError:
                    continue
            
            # どの形式でもパースできなかった場合
            if not time_str_formatted:
                time_str_formatted = "0000"
        except:
            time_str_formatted = "0000"
    else:
        time_str_formatted = "0000"
    
    # 種別（なければ〇〇〇〇）
    type_str = report_type if report_type else "〇〇〇〇"
    
    # 決算月（YYMM形式、なければYYMM）- エラーハンドリング強化
    period_str = "YYMM"
    if fiscal_period and isinstance(fiscal_period, str) and fiscal_period != "":
        try:
            # ハイフンもスラッシュも対応して最初の2要素を取得
            parts = fiscal_period.replace('-', '/').split('/')
            if len(parts) >= 2:
                year = int(parts[0]) % 100
                month = int(parts[1])
                period_str = f"{year:02d}{month:02d}"
            else:
                period_str = "YYMM"
        except (ValueError, TypeError, IndexError):
            period_str = "YYMM"
    
    # 四半期（なければQQ）
    quarter_str = quarter if quarter else "QQ"
    
    # コード（5桁、後ろにゼロを追加）
    if code:
        code_str = str(code).upper() + "0" * (5 - len(str(code)))
    else:
        code_str = "00000"
    
    # 会社名（禁則文字を除去）
    company_str = normalize_text(company_name)
    
    # 表題（禁則文字を除去、長さ制限）
    title_str = normalize_text(title)
    
    # ファイル名の長さを制限（全体的に長すぎる場合）
    max_title_length = 50  # 表題の最大長
    if len(title_str) > max_title_length:
        title_str = title_str[:max_title_length]
    
    # 禁則文字を除去（一時的にコメントアウト）
    # forbidden_chars = r'[\\/:*?"<>|]'  # Windowsの禁則文字
    # company_str = re.sub(forbidden_chars, '', company_str)
    # title_str = re.sub(forbidden_chars, '', title_str)
    
    # ファイル名を組み立て
    filename_parts = [
        seq_str,
        pub_date_str,
        time_str_formatted,
        type_str,
        period_str,
        quarter_str,
        code_str,
        company_str,
        title_str
    ]
    
    filename = "_".join(filename_parts)
    
    # XBRLデータがある場合は末尾に_XBRLを追加（Excelの計算式と同じ）
    # ただし、_XBRLを追加する前にファイル名が80文字以内に制限
    if len(filename) > 80:
        filename = filename[:80]
    
    # XBRL列にデータがある場合 - nan値チェックを強化
    if (xbrl_data and 
        str(xbrl_data).strip() and 
        str(xbrl_data).strip() != "" and 
        str(xbrl_data).lower() != 'nan'):
        filename += "_XBRL"
    
    return filename

def process_diff_file(file_path):
    """差分データファイルを処理して種別、決算期、四半期をセット"""
    if not file_path:
        return
    
    print(f"処理ファイル: {os.path.basename(file_path)}")
    
    try:
        # CSVリーダーを使用してファイルを読み込み
        with open(file_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                print("データがありません")
                return
        
        # 列名を検索
        try:
            title_col = header.index('表題')
            seq_col = header.index('連番')
            pub_date_col = header.index('公開日')
            time_col = header.index('時刻')
            code_col = header.index('コード')
            company_col = header.index('会社名')
            
            s_col = header.index('種別') if '種別' in header else None
            t_col = header.index('決算期') if '決算期' in header else None  
            u_col = header.index('四半期') if '四半期' in header else None
            j_col = header.index('種別') if '種別' in header else None  # J列（種別）
            k_col = header.index('決算期') if '決算期' in header else None  # K列（決算期）
            l_col = header.index('四半期') if '四半期' in header else None  # L列（四半期）
            
            # ファイル名列を検索（S列は除外）
            filename_col = None
            for i, col in enumerate(header):
                col_clean = col.strip('"')
                if 'ファイル名' in col_clean and '連番+公開日+時刻' in col_clean:
                    filename_col = i
                    break
        except ValueError as e:
            print(f"必要な列が見つかりません: {e}")
            return
        
        # ファイル名列がなければヘッダーに追加
        if filename_col is None:
            header.append('ファイル名')
            filename_col = len(header) - 1
        
        # 処理結果を格納するリスト
        processed_rows = []
        processed_rows.append(header)  # ヘッダーを追加
        
        updated_count = 0
        
        # データ行を処理
        with open(file_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f)
            next(reader)  # ヘッダーをスキップ
            
            for i, columns in enumerate(reader, 1):
                # 必要な列数を確保
                while len(columns) <= filename_col:
                    columns.append('""')
                
                title = columns[title_col].strip('"')
                seq_num = columns[seq_col].strip('"')
                pub_date = columns[pub_date_col].strip('"')
                time_str = columns[time_col].strip('"')
                code = columns[code_col].strip('"')
                company_name = columns[company_col].strip('"')
                
                # 既存のS列、T列、U列から値を取得
                existing_type = columns[s_col].strip('"') if s_col is not None and s_col < len(columns) else ""
                existing_period = columns[t_col].strip('"') if t_col is not None and t_col < len(columns) else ""
                existing_quarter = columns[u_col].strip('"') if u_col is not None and u_col < len(columns) else ""
                
                # 表題から新たに抽出
                report_type = extract_report_type(title)
                fiscal_period = extract_fiscal_period(title)
                quarter = extract_quarter(title)
                
                # 決算期をフォーマット
                period_str = ""
                if fiscal_period:
                    year, month = fiscal_period
                    last_day = monthrange(year, month)[1]
                    period_str = f"{year}/{month:02d}/{last_day:02d}"
                
                # 優先順位：既存値 > 抽出値
                final_type = existing_type if existing_type else (report_type or "")
                final_period = existing_period if existing_period else period_str
                final_quarter = existing_quarter if existing_quarter else (quarter or "")
                
                # ファイル名を生成
                try:
                    # XBRLデータを取得（E列）
                    xbrl_data = ""
                    if len(columns) > 4:  # E列（XBRL）が存在する場合
                        xbrl_data = columns[4].strip('"') if columns[4] else ""
                    
                    filename = generate_filename(
                        seq_num, pub_date, time_str, 
                        final_type, final_period, final_quarter,
                        code, company_name, title, xbrl_data
                    )
                except Exception as e:
                    print(f"ファイル名生成エラー（行 {i+1}）: {e}")
                    print(f"  データ: seq={seq_num}, date={pub_date}, time={time_str}, code={code}")
                    filename = "ERROR_FILENAME"
                
                # 結果を表示
                if final_type or final_period or final_quarter:
                    print(f"行 {i+1}: 種別={final_type}, 決算期={final_period}, 四半期={final_quarter}")
                    print(f"      ファイル名: {filename}")
                    updated_count += 1
                
                # J列、K列、L列の値を更新
                if j_col is not None and j_col < len(columns):
                    columns[j_col] = final_type
                if k_col is not None and k_col < len(columns):
                    columns[k_col] = final_period
                if l_col is not None and l_col < len(columns):
                    columns[l_col] = final_quarter
                
                # ファイル名列をセット（M列=13列目、0ベースで12）
                m_col_index = 12  # M列は13列目なので0ベースで12
                while len(columns) <= m_col_index:
                    columns.append("")
                columns[m_col_index] = filename
                
                processed_rows.append(columns)
        
        # 既存ファイルを上書き保存
        with open(file_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerows(processed_rows)
        
        # 結果出力
        print("-" * 50)
        print(f"【処理結果】")
        print(f"処理ファイル: {os.path.basename(file_path)}")
        print(f"総データ件数: {len(processed_rows)-1}件")
        print(f"更新成功数: {updated_count}件")
        print(f"ファイル名を追加しました")
        print(f"ファイルを上書き保存しました")
        print("-" * 50)
        
    except Exception as e:
        print(f"エラーが発生しました: {e}")

def convert_forbidden_chars_in_csv(csv_file_path):
    """CSVファイルのM列にある禁則文字を全角に変換し、P列に変更記録を残す"""
    print(f"\n=== 禁則文字変換処理開始 ===")
    print(f"対象ファイル: {csv_file_path}")
    
    try:
        # CSVリーダーを使用してファイルを読み込み
        with open(csv_file_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                print("データがありません")
                return
        
        # 列インデックスを特定
        m_col_index = None
        d_col_index = None
        p_col_index = None  # P列のインデックスを特定
        
        for i, col_name in enumerate(header):
            col_name_clean = col_name.strip('"')
            if 'ファイル名' in col_name_clean and '連番+公開日+時刻' in col_name_clean:
                m_col_index = i
            elif col_name_clean == '表題':
                d_col_index = i
            elif col_name_clean == '禁則文字':  # P列が「禁則文字」であることを確認
                p_col_index = i
        
        print(f"M列インデックス: {m_col_index}, D列インデックス: {d_col_index}, P列インデックス: {p_col_index}")
        
        if m_col_index is None:
            print("M列が見つかりません")
            return
        
        if p_col_index is None:
            print("P列（禁則文字）が見つかりません。処理を中断します。")
            return
        
        # 処理結果を格納するリスト
        processed_rows = []
        processed_rows.append(header)  # ヘッダーを追加
        
        change_count = 0
        
        # データ行を処理
        with open(csv_file_path, 'r', encoding='utf-8-sig', newline='') as f:
            reader = csv.reader(f)
            next(reader)  # ヘッダーをスキップ
            
            for i, columns in enumerate(reader, 1):
                # M列のファイル名を取得
                if m_col_index < len(columns):
                    filename = columns[m_col_index].strip('"')
                    original_filename = filename
                    
                    # 禁則文字を変換
                    changed_chars = []
                    for half, full in mapping.items():
                        if half in filename:
                            filename = filename.replace(half, full)
                            changed_chars.append(half)
                    
                    # 変換があった場合
                    if changed_chars:
                        columns[m_col_index] = filename
                        # P列（禁則文字）に変更記録をセット（カンマ区切りで記録）
                        change_record = ",".join(changed_chars)
                        if p_col_index < len(columns):
                            columns[p_col_index] = change_record
                        change_count += 1
                
                processed_rows.append(columns)
                
                if i % 1000 == 0:
                    print(f"  進捗: {i} 行目処理中...")
        
        # ファイルを上書き保存
        try:
            with open(csv_file_path, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f, quoting=csv.QUOTE_ALL)
                writer.writerows(processed_rows)
        except PermissionError:
            print(f"❌ エラー: ファイルが開かれています")
            print(f"ファイル名: {os.path.basename(csv_file_path)}")
            print(f"ファイルを閉じて再度実行してください")
            return
        
        print(f"✅ 禁則文字変換完了")
        print(f"禁則文字 {change_count}件処理済")
        
    except Exception as e:
        print(f"禁則文字変換エラー: {e}")
        import traceback
        traceback.print_exc()

def main():
    print("=== TDnet差分データ処理 ===")
    # 最新の差分データファイルを探す
    latest_file = find_latest_diff_file(TARGET_DIR)
    
    # コピーしたファイルを直接指定（テスト用）
    test_file = r"C:\Users\ensyu\_myfolder\work\dev\TDnet適時情報開示サービス\TDNET差分データ_20260211_162306 - コピー.csv"
    
    # コピーしたファイルが存在すればそちらを優先
    target_file = test_file if os.path.exists(test_file) else latest_file
    
    if target_file:
        print(f"処理対象ファイル: {os.path.basename(target_file)}")
        # 既存の処理を実行
        process_diff_file(target_file)
        
        # 禁則文字変換処理を追加
        convert_forbidden_chars_in_csv(target_file)
    else:
        print("処理対象ファイルが見つかりませんでした")

if __name__ == '__main__':
    main()
