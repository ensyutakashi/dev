# --- obsidian_property ---
# scr名: 【自動】
# 概要: TDnet差分抽出処理
# 処理grp: TDnetダウンロード
# 処理順番: 2
# mermaid: "[[mermaid_TDnet適時開示情報ダウンロード]]"
# tags: ["tdnet", "download", "diff"]
# aliases: ["02_tdnet_get_max_sequence_date.py"]
# created: 2026-02-17
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：TDnet差分抽出処理
# DB内の最新レコード（最大連番）の日付を基準に、TDnetからそれ以降のデータを取得し、未登録分のみを抽出
# 1. **DB比較対象データの取得**
#    - DBより最新日のデータを全て取得する (`DB比較対象データ_YYMMDD_HHMMSS.csv`)
#    - 例：最新データが 2026/02/01 13:00 なら、**02/01の全データ**を取得
# 2. **TDnetからのデータ抽出**
#    - TDnetよりDBと同日以降のデータを取得する (`TDNET抽出データ_YYMMDD_HHMMSS.csv`)
#    - 例：実行日が 02/10 なら、**02/01〜02/10の全データ**を取得
# 3. **差分データの作成**
#    - 「TDNET抽出データ」から「DB比較対象データ」を除外して保存 (`TDnet差分データ_YYMMDD_HHMMSS.csv`)
#    - 例：**02/01 14:00〜02/10** の未登録分のみが抽出される
# --- 概要 ---

import duckdb
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
import csv

BASE = "https://www.release.tdnet.info/inbs/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    )
}

OUTPUT_DIR = r"C:\Users\ensyu\_myfolder\work\dev\TDnet適時情報開示サービス"
# 環境変数から作業フォルダを取得（なければデフォルト）
WORKING_DIR = os.environ.get('TDNET_WORKING_DIR', os.path.join(OUTPUT_DIR, "TDnet_report_temp_validation_log_files"))
os.makedirs(WORKING_DIR, exist_ok=True)  # フォルダがなければ作成

def fetch_page_html(sess: requests.Session, page_path: str) -> Optional[str]:
    """ページHTML取得（簡易リトライ）。成功時は文字列、失敗時 None。"""
    url = BASE + page_path
    for i in range(3):  # リトライ 3回
        try:
            r = sess.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200 and "main-list-table" in r.text:
                r.encoding = r.apparent_encoding or "utf-8"
                return r.text
        except requests.RequestException:
            pass
        time.sleep(1 + i)
    return None

def parse_rows(html: str, date_str: str) -> List[Dict]:
    """main-list-table をパースして、行辞書のリストを返す。"""
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", id="main-list-table")
    rows: List[Dict] = []
    if not table:
        return rows

    pub_date = datetime.strptime(date_str, "%Y%m%d").date()
    for tr in table.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 7:
            continue

        # 0:時刻, 1:コード, 2:会社名
        t_time = tds[0].get_text(strip=True)
        t_code = tds[1].get_text(strip=True)
        t_name = tds[2].get_text(strip=True)

        # 3:表題（PDFリンク）
        title_td = tds[3]
        a = title_td.find("a")
        title_txt = title_td.get_text(strip=True)
        pdf_url = BASE + a["href"].lstrip("./") if (a and a.get("href")) else None # pyright: ignore[reportAttributeAccessIssue]

        # 4:XBRL（zipリンクがあるときのみ）
        x_td = tds[4]
        xa = x_td.find("a")
        x_url = BASE + xa["href"].lstrip("./") if (xa and xa.get("href")) else None # type: ignore
        x_text = "XBRL" if x_url else ""

        # 5:上場取引所, 6:更新履歴
        place = tds[5].get_text(strip=True)
        hist = tds[6].get_text(strip=True)

        # 時刻をDuckDBのTIMESTAMPフォーマットに変換（公開日+時刻）
        time_obj = datetime.strptime(t_time, "%H:%M")
        full_timestamp = datetime.combine(pub_date, time_obj.time())
        
        # 秒まで含めたフォーマット（2026-01-28 18:30:00）
        formatted_time = full_timestamp.strftime('%Y-%m-%d %H:%M:%S')

        rows.append({
            "時刻": formatted_time,
            "コード": t_code, 
            "会社名": t_name,
            "表題": title_txt, 
            "表題URL": pdf_url,
            "XBRL": x_text, 
            "XBRLURL": x_url,
            "上場取引所": place, 
            "更新履歴": hist,
            "公開日": pub_date.strftime('%Y-%m-%d'),
        })
    return rows

def scrape_one_day(date_str: str) -> List[Dict]:
    """指定日の全ページ（100件単位）を走査して結合。"""
    sess = requests.Session()
    all_rows: List[Dict] = []
    page = 1
    while True:
        page_path = f"I_list_{page:03d}_{date_str}.html"
        html = fetch_page_html(sess, page_path)
        if not html:
            break
        rows = parse_rows(html, date_str)
        if not rows:
            break
        all_rows.extend(rows)
        page += 1
        if page > 50:
            break
    return all_rows

def get_max_sequence_date():
    db_path = r"C:\Users\ensyu\_myfolder\work\dev\TDnet適時情報開示サービス\tdnet.duckdb"
    if not os.path.exists(db_path):
        print(f"エラー: ファイルが見つかりません: {db_path}")
        return None

    try:
        con = duckdb.connect(database=db_path, read_only=True)
        query = """
        SELECT 公開日, 連番, 会社名, 表題
        FROM disclosure_info 
        WHERE 連番 = (SELECT MAX(連番) FROM disclosure_info)
        """
        result = con.execute(query).fetchone()
        con.close()
        
        if result:
            print(f"連番の最大値: {result[1]}")
            print(f"公開日: {result[0]}")
            print(f"会社名: {result[2]}")
            print(f"表題: {result[3]}")
            return result[0]
        return None
    except Exception as e:
        print(f"エラー: {e}")
        return None

def download_data_for_date(target_date):
    if isinstance(target_date, str):
        target_date = datetime.strptime(target_date, '%Y-%m-%d').date()
    date_str = target_date.strftime('%Y%m%d')
    print(f"\n=== {date_str} のデータをダウンロード ===")
    try:
        day_start = time.time()
        rows = scrape_one_day(date_str)
        day_end = time.time()
        if rows:
            print(f"{date_str} 取得完了 ({len(rows)}件, {day_end - day_start:.2f}秒)")
            return rows
        return []
    except Exception as e:
        print(f"エラー: {e}")
        return []

def download_data_since_date(start_date):
    """指定日以降の全データを取得"""
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    
    all_data = []
    current_date = start_date
    today = datetime.now().date()
    
    while current_date <= today:
        date_str = current_date.strftime('%Y%m%d')
        print(f"\n=== {date_str} のデータをダウンロード ===")
        try:
            day_start = time.time()
            rows = scrape_one_day(date_str)
            day_end = time.time()
            if rows:
                all_data.extend(rows)
                print(f"{date_str} 取得完了 ({len(rows)}件, {day_end - day_start:.2f}秒)")
            else:
                print(f"{date_str} データなし")
        except Exception as e:
            print(f"{date_str} エラー: {e}")
        
        current_date += timedelta(days=1)
        
        # 1日の最大ループ防止
        if current_date > start_date + timedelta(days=365):
            print("警告: 365日を超えるため処理を停止します")
            break
    
    return all_data

def get_count_from_db(target_date):
    db_path = r"C:\Users\ensyu\_myfolder\work\dev\TDnet適時情報開示サービス\tdnet.duckdb"
    if not os.path.exists(db_path):
        return None
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        query = "SELECT COUNT(*) FROM disclosure_info WHERE 公開日 = ?"
        result = con.execute(query, [target_date]).fetchone()
        con.close()
        return result[0] if result else 0
    except Exception as e:
        print(f"データベースエラー: {e}")
        return None

def save_tdnet_data_to_csv(new_data: List[Dict], target_date: str):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = os.path.join(WORKING_DIR, f"TDNET抽出データ_{timestamp}.csv")
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
            writer.writerow(['時刻', 'コード', '会社名', '表題', '表題URL', 'XBRL', 'XBRLURL', '上場取引所', '更新履歴', '公開日'])
            for row in new_data:
                xbrl_url = row.get("XBRLURL", "")
                if xbrl_url is None or xbrl_url == "None":
                    xbrl_url = ""
                writer.writerow([
                    row["時刻"], 
                    row["コード"], 
                    row["会社名"], 
                    row["表題"], 
                    row.get("表題URL", ""), 
                    row["XBRL"], 
                    xbrl_url, 
                    row["上場取引所"], 
                    row["更新履歴"], 
                    row["公開日"]
                ])
        print(f"✅ TDnet抽出データを保存しました: {csv_filename}")
        return csv_filename
    except Exception as e:
        print(f"TDnetデータ保存エラー: {e}")
        return None

def save_db_data_to_csv(target_date: str, db_path: str):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = os.path.join(WORKING_DIR, f"DB比較対象データ_{timestamp}.csv")
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        query = """
        SELECT * FROM disclosure_info 
        WHERE 公開日 = ?
        """
        db_records = con.execute(query, [target_date]).fetchall()
        columns = [desc[0] for desc in con.description]
        con.close()
        
        with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
            writer.writerow(columns)
            for record in db_records:
                row = [str(field) if field is not None else "" for field in record]
                writer.writerow(row)
        print(f"✅ DB比較対象データ（全列）を保存しました: {csv_filename}")
        return csv_filename
    except Exception as e:
        print(f"DBデータ保存エラー: {e}")
        return None

def get_diff_by_key_comparison(all_new_data: List[Dict], db_path: str, compare_date: str) -> List[Dict]:
    """時刻,コード,会社名,表題,表題_URLでDBとTDNETデータを比較して不一致データのみ抽出"""
    if not all_new_data:
        return []
    
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        query = """
        SELECT 
            CAST(時刻 AS VARCHAR), 
            CAST(コード AS VARCHAR), 
            CAST(会社名 AS VARCHAR), 
            CAST(表題 AS VARCHAR),
            CAST(表題_URL AS VARCHAR)
        FROM disclosure_info 
        WHERE 公開日 = ?
        """
        db_records = con.execute(query, [compare_date]).fetchall()
        con.close()
        
        # セット作成（型や余計な空白の影響を排除）
        db_set = set()
        for record in db_records:
            db_set.add((
                str(record[0]).strip(),
                str(record[1]).strip(),
                str(record[2]).strip(),
                str(record[3]).strip(),
                str(record[4]).strip(),
            ))
        
        diff_data = []
        for row in all_new_data:
            comparison_key = (
                str(row["時刻"]).strip(),
                str(row["コード"]).strip(),
                str(row["会社名"]).strip(),
                str(row["表題"]).strip(),
                str(row.get("表題URL", "")).strip(),
            )
            if comparison_key not in db_set:
                diff_data.append(row)
        
        return diff_data
    except Exception as e:
        print(f"比較エラー: {e}")
        return []

def save_diff_to_csv_with_header_only(diff_data: List[Dict], data_type: str, db_path: str):
    """差分データをCSV保存（データがない場合はヘッダーのみ）"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = os.path.join(WORKING_DIR, f"TDNET差分データ_{timestamp}.csv")
    
    db_columns = get_db_columns(db_path)
    if not db_columns:
        print("DB列構成の取得に失敗しました")
        return None
    
    try:
        with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
            writer.writerow(db_columns)
            
            if diff_data:
                max_seq = get_max_sequence_number(db_path)
                sorted_data = sorted(diff_data, key=lambda x: (
                    x["公開日"],
                    x["時刻"], 
                    x["コード"], 
                    x["会社名"], 
                    x["表題"], 
                    x.get("表題URL", "")
                ))
                
                for i, row in enumerate(sorted_data):
                    current_seq = max_seq + i + 1
                    csv_row = []
                    for col in db_columns:
                        if col == "連番":
                            csv_row.append(current_seq)
                        elif col == "時刻":
                            csv_row.append(row["時刻"])
                        elif col == "コード":
                            csv_row.append(row["コード"])
                        elif col == "会社名":
                            csv_row.append(row["会社名"])
                        elif col == "表題":
                            csv_row.append(row["表題"])
                        elif col == "表題_URL":
                            csv_row.append(row.get("表題URL", ""))
                        elif col == "XBRL":
                            csv_row.append(row["XBRL"])
                        elif col == "XBRL_URL":
                            xbrl_url = row.get("XBRLURL", "")
                            csv_row.append("" if (xbrl_url is None or xbrl_url == "None") else xbrl_url)
                        elif col == "上場取引所":
                            csv_row.append(row["上場取引所"])
                        elif col == "更新履歴":
                            csv_row.append(row["更新履歴"])
                        elif col == "公開日":
                            csv_row.append(row["公開日"])
                        else:
                            csv_row.append("")
                    
                    writer.writerow(csv_row)
                
                print(f"✅ {data_type}を保存しました（{len(sorted_data)}件、連番: {max_seq+1}〜{max_seq+len(sorted_data)}）: {csv_filename}")
            else:
                print(f"✅ {data_type}はありません（ヘッダーのみ）: {csv_filename}")
        
        return csv_filename
    except Exception as e:
        print(f"CSV保存エラー: {e}")
        return None

def get_max_sequence_number(db_path: str) -> int:
    """連番の最大値を取得"""
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        result = con.execute("SELECT MAX(連番) FROM disclosure_info").fetchone()
        con.close()
        return result[0] if result and result[0] else 0
    except Exception as e:
        print(f"連番取得エラー: {e}")
        return 0

def get_db_columns(db_path: str) -> List[str]:
    """DBの列名を取得（S列を除外）"""
    try:
        con = duckdb.connect(database=db_path, read_only=True)
        result = con.execute("SELECT * FROM disclosure_info LIMIT 1")
        columns = [desc[0] for desc in result.description 
                  if desc[0] != 'S']
        con.close()
        return columns
    except Exception as e:
        print(f"列名取得エラー: {e}")
        return []

def save_diff_to_csv(diff_data: List[Dict], date_str: str, data_type: str, db_path: str):
    """データをソートしてDB列構成でCSV保存（連番付与）"""
    if not diff_data:
        print(f"✅ {data_type}はありません")
        return None

    db_columns = get_db_columns(db_path)
    if not db_columns:
        print("DB列構成の取得に失敗しました")
        return None
    
    max_seq = get_max_sequence_number(db_path)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = f"TDNET差分データ_{timestamp}.csv"
    try:
        sorted_data = sorted(diff_data, key=lambda x: (
            x["公開日"],
            x["時刻"], 
            x["コード"], 
            x["会社名"], 
            x["表題"], 
            x.get("表題URL", "")
        ))
        
        with open(csv_filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
            writer.writerow(db_columns)
            
            for i, row in enumerate(sorted_data):
                current_seq = max_seq + i + 1
                csv_row = []
                for col in db_columns:
                    if col == "連番":
                        csv_row.append(current_seq)
                    elif col == "時刻":
                        csv_row.append(row["時刻"])
                    elif col == "コード":
                        csv_row.append(row["コード"])
                    elif col == "会社名":
                        csv_row.append(row["会社名"])
                    elif col == "表題":
                        csv_row.append(row["表題"])
                    elif col == "表題_URL":
                        csv_row.append(row.get("表題URL", ""))
                    elif col == "XBRL":
                        csv_row.append(row["XBRL"])
                    elif col == "XBRL_URL":
                        xbrl_url = row.get("XBRLURL", "")
                        csv_row.append("" if (xbrl_url is None or xbrl_url == "None") else xbrl_url)
                    elif col == "上場取引所":
                        csv_row.append(row["上場取引所"])
                    elif col == "更新履歴":
                        csv_row.append(row["更新履歴"])
                    elif col == "公開日":
                        csv_row.append(row["公開日"])
                    else:
                        csv_row.append("")
                
                writer.writerow(csv_row)
        
        print(f"✅ {data_type}をDB列構成で保存しました（連番: {max_seq+1}〜{max_seq+len(sorted_data)}）: {csv_filename}")
        return csv_filename
    except Exception as e:
        print(f"CSV保存エラー: {e}")
        return None

def main():
    print("=== 連番最大値の日付取得と差分抽出（全期間ソート版） ===")
    start_time = time.time()
    
    db_path = r"C:\Users\ensyu\_myfolder\work\dev\TDnet適時情報開示サービス\tdnet.duckdb"
    max_date = get_max_sequence_date()
    
    if not max_date:
        print("データベースから日付を取得できませんでした")
        return
    
    max_date_str = max_date.strftime('%Y-%m-%d') if isinstance(max_date, datetime) else str(max_date).split()[0]
    print(f"max_date: {max_date_str}")
    
    print(f"\n=== {max_date_str} 以降の全データを取得開始 ===")
    all_new_data = download_data_since_date(max_date_str)
    
    if all_new_data:
        print(f"\n=== 取得結果 ===")
        print(f"総取得件数: {len(all_new_data)}件")
        
        diff_data = get_diff_by_key_comparison(all_new_data, db_path, max_date_str)
        save_diff_to_csv_with_header_only(diff_data, "不一致データ", db_path)

        max_date_data = [row for row in all_new_data if row["公開日"] == max_date_str]
        db_count = get_count_from_db(max_date_str)
        if db_count is not None:
            print(f"\n=== {max_date_str} の状況 ===")
            print(f"DB件数: {db_count}件 / DL件数: {len(max_date_data)}件 / 不一致: {len(diff_data)}件")

        save_tdnet_data_to_csv(all_new_data, max_date_str)
        save_db_data_to_csv(max_date_str, db_path)

    else:
        print("データが取得できませんでした")
    
    end_time = time.time()
    total_time = end_time - start_time
    minutes = int(total_time // 60)
    seconds = int(total_time % 60)
    print(f"\n処理時間: {minutes}分{seconds}秒")

if __name__ == "__main__":
    main()
