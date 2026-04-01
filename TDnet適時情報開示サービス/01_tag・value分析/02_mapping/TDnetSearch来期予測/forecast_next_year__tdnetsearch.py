# --- obsidian_property ---
# scr名: 【自動】
# 概要: TDnetSearchから来期売上,営利,経常,純利予測取得
# 処理grp: -
# 処理順番: 0
# mermaid: 
# tags: ["tdnet", "download"]
# aliases: ["tdnetSearch_sales_forecast.py"]
# created: 2026-03-31
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：TDnetSearchから来期売上,営利,経常,純利予測取得
# TDnetSearchの条件↓↓↓↓↓↓
# Expression date ※日付順
# Filter date=2025-01-30 ～ 2026-03-30
# Field　PER PBR 配当利回り 自己資本比率 売上 営業利益 経常利益 純利益 今期売上 今期営業利益 今期経常利益 今期純利益 来期売上 来期営業利益 来期経常利益 来期純利益 営業利益進捗率
# Option 過去のデータを表示 (最大10000件) ←必ずチェックを入れること!!
# TDnetSearchの条件↑↑↑↑↑↑
# このscriptの日付条件をセットする↓↓↓↓↓↓
# START_DATE = "2025-01-01"
# END_DATE = "2025-01-15"
# このscriptの日付条件をセットする↑↑↑↑↑↑
# --- 概要 ---

from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
import io
import sys
import re
import urllib.parse
from urllib.parse import urlparse, parse_qs

import pandas as pd
import requests
from bs4 import BeautifulSoup

# excel_formatter.py をインポート
sys.path.append(r"C:\Users\ensyu\_myfolder\work\dev\python")
try:
    from excel_formatter import format_excel
    EXCEL_FORMATTER_AVAILABLE = True
except Exception:
    EXCEL_FORMATTER_AVAILABLE = False

# 日本の祝日ライブラリ
try:
    import jpholiday
    HOLIDAY_AVAILABLE = True
except ImportError:
    HOLIDAY_AVAILABLE = False
    print("jpholidayがインストールされていません。pip install jpholiday でインストールしてください。")


# =========================
# 設定
# =========================
OUTPUT_DIR = Path(__file__).resolve().parent
OUTPUT_FILENAME = "tdnetSearch_sales_forecast"

START_DATE = "2025-01-01"
END_DATE = "2025-01-15"

BASE_URL_TEMPLATE = (
    "https://tdnet-search.appspot.com/analyze?"
    "expression=date&"
    "filter=date%3D{DATE}&"
    "field=PER+PBR+%E9%85%8D%E5%BD%93%E5%88%A9%E5%9B%9E%E3%82%8A+"
    "%E8%87%AA%E5%B7%B1%E8%B3%87%E6%9C%AC%E6%AF%94%E7%8E%87+"
    "%E5%A3%B2%E4%B8%8A+%E5%96%B6%E6%A5%AD%E5%88%A9%E7%9B%8A+"
    "%E7%B5%8C%E5%B8%B8%E5%88%A9%E7%9B%8A+%E7%B4%94%E5%88%A9%E7%9B%8A+"
    "%E4%BB%8A%E6%9C%9F%E5%A3%B2%E4%B8%8A+%E4%BB%8A%E6%9C%9F%E5%96%B6%E6%A5%AD%E5%88%A9%E7%9B%8A+"
    "%E4%BB%8A%E6%9C%9F%E7%B5%8C%E5%B8%B8%E5%88%A9%E7%9B%8A+%E4%BB%8A%E6%9C%9F%E7%B4%94%E5%88%A9%E7%9B%8A+"
    "%E6%9D%A5%E6%9C%9F%E5%A3%B2%E4%B8%8A+%E6%9D%A5%E6%9C%9F%E5%96%B6%E6%A5%AD%E5%88%A9%E7%9B%8A+"
    "%E6%9D%A5%E6%9C%9F%E7%B5%8C%E5%B8%B8%E5%88%A9%E7%9B%8A+%E6%9D%A5%E6%9C%9F%E7%B4%94%E5%88%A9%E7%9B%8A+"
    "%E5%96%B6%E6%A5%AD%E5%88%A9%E7%9B%8A%E9%80%B2%E6%8D%97%E7%8E%87&"
    "historical=on&graph=0"
)

RESULT_SCHEMA = [
    "順位",
    "スコア",
    "コード",
    "会社名",
    "PER",
    "PBR",
    "配当利回り",
    "自己資本比率",
    "売上",
    "営業利益",
    "経常利益",
    "純利益",
    "今期売上",
    "今期営業利益",
    "今期経常利益",
    "今期純利益",
    "来期売上",
    "来期営業利益",
    "来期経常利益",
    "来期純利益",
    "営業利益進捗率",
]

META_COLUMNS = ["日付", "曜日", "filter条件"]


# =========================
# 共通関数
# =========================
def flatten_columns(columns) -> list[str]:
    new_cols: list[str] = []
    for col in columns:
        if isinstance(col, tuple):
            parts = [str(x).strip() for x in col if str(x).strip() and not str(x).startswith("Unnamed")]
            new_cols.append("_".join(parts) if parts else "")
        else:
            text = str(col).strip()
            if text.startswith("Unnamed"):
                text = ""
            new_cols.append(text)
    return new_cols


def is_blank_like(value) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text == "" or text.lower() in {"nan", "none"}


def normalize_cell(value):
    if value is None:
        return None
    if isinstance(value, str):
        v = value.replace("\u3000", " ").strip()
        if v.lower() in {"nan", "none", ""}:
            return None
        return v
    return value


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = flatten_columns(df.columns)

    for col in df.columns:
        df[col] = df[col].map(normalize_cell)

    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all").reset_index(drop=True)
    return df


def looks_like_header_row(values: list) -> bool:
    texts = ["" if is_blank_like(v) else str(v).strip() for v in values]
    joined = "|".join(texts)
    header_keywords = [
        "順位", "スコア", "コード", "会社名", "PER", "PBR", "配当利回り", "自己資本比率",
        "売上", "営業利益", "経常利益", "純利益", "今期売上", "来期売上", "営業利益進捗率",
    ]
    score = sum(1 for kw in header_keywords if kw in joined)
    return score >= 3


def promote_first_row_to_header_if_needed(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    current_cols = [str(c).strip() for c in df.columns]
    unnamed_like = sum(1 for c in current_cols if c == "" or c.isdigit() or c.startswith("col_"))

    first_row = df.iloc[0].tolist()
    if looks_like_header_row(first_row) and unnamed_like >= max(1, len(current_cols) // 3):
        new_columns = []
        for idx, value in enumerate(first_row):
            text = "" if is_blank_like(value) else str(value).strip()
            new_columns.append(text if text else f"col_{idx}")
        df = df.iloc[1:].reset_index(drop=True)
        df.columns = new_columns

    return df


def choose_best_result_table(tables: list[pd.DataFrame]) -> pd.DataFrame:
    candidates: list[tuple[int, pd.DataFrame]] = []

    for i, raw_df in enumerate(tables):
        df = normalize_dataframe(raw_df)
        df = promote_first_row_to_header_if_needed(df)
        if df.empty:
            continue

        header_text = "|".join(str(c) for c in df.columns)
        data_preview = "|".join(str(x) for x in df.iloc[0].tolist()) if len(df) else ""

        score = 0
        for kw in RESULT_SCHEMA:
            if kw in header_text:
                score += 4
            if kw in data_preview:
                score += 2

        if len(df.columns) >= 10:
            score += 20
        elif len(df.columns) >= 5:
            score += 8
        else:
            score -= 5

        if len(df) >= 2:
            score += 3

        # 条件表らしいものは減点
        small_header_words = ["Expression", "Filter", "Field", "Option"]
        if any(word in header_text for word in small_header_words):
            score -= 20
        if len(df.columns) <= 2 and len(df) <= 5:
            score -= 10

        print(f"table[{i}] shape={df.shape} score={score} cols={list(df.columns)[:8]}")
        candidates.append((score, df))

    if not candidates:
        raise ValueError("本体の結果表が見つかりませんでした。")

    candidates.sort(key=lambda x: (x[0], x[1].shape[0] * x[1].shape[1]), reverse=True)
    best_df = candidates[0][1].copy()
    print(f"→ 採用テーブル shape={best_df.shape}")
    return best_df


def remove_embedded_header_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    rows_to_drop: list[int] = []
    for idx, row in df.iterrows():
        values = row.tolist()
        if looks_like_header_row(values):
            rows_to_drop.append(idx)
            continue

        first = "" if len(values) == 0 or is_blank_like(values[0]) else str(values[0]).strip()
        second = "" if len(values) < 2 or is_blank_like(values[1]) else str(values[1]).strip()
        if first in {"Expression", "Filter", "Field", "Option"} or second in {"Expression", "Filter", "Field", "Option"}:
            rows_to_drop.append(idx)

    if rows_to_drop:
        print(f"埋め込みヘッダー行を除去: {rows_to_drop}")
        df = df.drop(index=rows_to_drop).reset_index(drop=True)

    return df


def align_to_result_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    各日のテーブル列が揺れても、最終的に RESULT_SCHEMA に揃える。
    これをしないと日によって 0,1,2... 列に入ってしまい、Excel上で右にずれる。
    """
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    df = remove_embedded_header_rows(df)
    df = df.dropna(axis=1, how="all")
    df = df.dropna(axis=0, how="all").reset_index(drop=True)

    # 既に正式ヘッダーが付いている場合は、そのまま採用
    matched = [col for col in df.columns if col in RESULT_SCHEMA]
    if matched:
        aligned = pd.DataFrame(index=df.index)
        for col in RESULT_SCHEMA:
            aligned[col] = df[col] if col in df.columns else None
        return aligned

    # ヘッダーが壊れている場合は位置ベースで復元
    positional_cols = [col for col in df.columns if str(col).strip() != ""]
    aligned = pd.DataFrame(index=df.index)
    for i, name in enumerate(RESULT_SCHEMA):
        if i < len(positional_cols):
            aligned[name] = df[positional_cols[i]]
        else:
            aligned[name] = None

    return aligned


def save_excel(df: pd.DataFrame, output_path: Path) -> None:
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="result", index=False)
        ws = writer.sheets["result"]
        ws.auto_filter.ref = ws.dimensions

        # A列: Excelの日付型として yy/mm/dd 表示
        for cell in ws["A"][1:]:
            if cell.value is not None:
                cell.number_format = "yy/mm/dd"

        for col_cells in ws.columns:
            max_len = 0
            col_letter = col_cells[0].column_letter
            for cell in col_cells:
                val = "" if cell.value is None else str(cell.value)
                max_len = max(max_len, len(val))
            ws.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 30)


def add_date_info(df: pd.DataFrame, date_str: str) -> pd.DataFrame:
    df = df.copy()
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    excel_date = date_obj.date()
    weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    weekday = weekdays[date_obj.weekday()]

    df.insert(0, "日付", excel_date)
    df.insert(1, "曜日", weekday)
    return df


def extract_filter_condition(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    if "filter" not in params:
        return "条件なし"

    filter_encoded = params["filter"][0]
    filter_decoded = urllib.parse.unquote(filter_encoded)

    conditions: list[str] = []

    if "来期売上>=0" in filter_decoded and "来期売上<3000" in filter_decoded:
        conditions.append("来期売上：0-3000")
    elif "来期売上>=3000" in filter_decoded and "来期売上<5000" in filter_decoded:
        conditions.append("来期売上：3000-5000")
    elif "来期売上>=5000" in filter_decoded and "来期売上<7000" in filter_decoded:
        conditions.append("来期売上：5000-7000")
    elif "来期売上>=7000" in filter_decoded and "来期売上<10000" in filter_decoded:
        conditions.append("来期売上：7000-10000")
    elif "来期売上>=10000" in filter_decoded and "来期売上<15000" in filter_decoded:
        conditions.append("来期売上：10000-15000")
    elif "来期売上>=15000" in filter_decoded and "来期売上<20000" in filter_decoded:
        conditions.append("来期売上：15000-20000")
    elif "来期売上>=20000" in filter_decoded and "来期売上<30000" in filter_decoded:
        conditions.append("来期売上：20000-30000")
    elif "来期売上>=30000" in filter_decoded and "来期売上<50000" in filter_decoded:
        conditions.append("来期売上：30000-50000")
    elif "来期売上>=50000" in filter_decoded and "来期売上<100000" in filter_decoded:
        conditions.append("来期売上：50000-100000")
    elif "来期売上>=100000" in filter_decoded and "来期売上<200000" in filter_decoded:
        conditions.append("来期売上：100000-200000")
    elif "来期売上>=200000" in filter_decoded and "来期売上<500000" in filter_decoded:
        conditions.append("来期売上：200000-500000")
    elif "来期売上>=500000" in filter_decoded and "来期売上<1000000" in filter_decoded:
        conditions.append("来期売上：500000-1000000")
    elif "来期売上>1050000" in filter_decoded:
        conditions.append("来期売上：1050000超")
    elif "来期営業利益>0" in filter_decoded:
        conditions.append("来期営業利益>0")
    elif "来期営業利益<0" in filter_decoded:
        conditions.append("来期営業利益<0")
    elif "来期経常利益>0" in filter_decoded:
        conditions.append("来期経常利益>0")
    elif "来期経常利益<0" in filter_decoded:
        conditions.append("来期経常利益<0")
    elif "来期純利益>0" in filter_decoded:
        conditions.append("来期純利益>0")
    elif "来期純利益<0" in filter_decoded:
        conditions.append("来期純利益<0")

    return ", ".join(conditions) if conditions else filter_decoded


# =========================
# 取得処理
# =========================
def fetch_html(url: str) -> str:
    response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
    response.raise_for_status()
    response.encoding = response.apparent_encoding
    return response.text


def fetch_tables_from_html(html: str) -> list[pd.DataFrame]:
    return pd.read_html(io.StringIO(html))


def fetch_data_from_url(start_url: str, url_description: str) -> pd.DataFrame:
    all_pages: list[pd.DataFrame] = []
    page = 1
    current_url = start_url

    print(f"\n=== {url_description} データ取得開始 ===")

    while current_url:
        print(f"\n--- ページ {page} ---")
        print(f"URL取得中: {current_url}")
        try:
            html = fetch_html(current_url)
            tables = fetch_tables_from_html(html)
            print(f"見つかった表の数: {len(tables)}")
            if not tables:
                print("HTML内に表が見つかりませんでした。")
                break

            result_df = choose_best_result_table(tables)
            result_df = align_to_result_schema(result_df)
            result_df = result_df.dropna(axis=0, how="all").reset_index(drop=True)

            if result_df.empty:
                print("結果表が空でした。")
                break

            print(f"ページ{page}のデータ取得完了: {len(result_df)}件")
            all_pages.append(result_df)

            # 100件未満なら最終ページ
            if len(result_df) < 100:
                print("データ件数が100件未満のため、最終ページと判断します。")
                break

            if "page=" in current_url:
                next_url = re.sub(r"page=\d+", f"page={page + 1}", current_url)
            else:
                separator = "&" if "?" in current_url else "?"
                next_url = f"{current_url}{separator}page={page + 1}"

            current_url = next_url
            page += 1

        except Exception as e:
            print(f"エラーが発生しました: {e}")
            break

    if not all_pages:
        print(f"{url_description} データが取得できませんでした。")
        return pd.DataFrame(columns=RESULT_SCHEMA)

    combined_df = pd.concat(all_pages, ignore_index=True)
    combined_df = combined_df.reindex(columns=RESULT_SCHEMA)
    print(f"{url_description} 合計データ件数: {len(combined_df)}件")
    return combined_df


# =========================
# main
# =========================
def main() -> None:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_excel = OUTPUT_DIR / f"{OUTPUT_FILENAME}_{timestamp}.xlsx"

    print("データ取得開始...")
    print(f"取得期間: {START_DATE} から {END_DATE} まで")

    start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_date = datetime.strptime(END_DATE, "%Y-%m-%d")

    all_results: list[pd.DataFrame] = []
    current_date = start_date

    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        url = BASE_URL_TEMPLATE.replace("{DATE}", date_str)
        description = f"date={date_str}"

        print(f"\n=== {description} ===")
        df = fetch_data_from_url(url, description)

        if not df.empty:
            df = add_date_info(df, date_str)
            df.insert(2, "filter条件", extract_filter_condition(url))
            df = df.reindex(columns=META_COLUMNS + RESULT_SCHEMA)
            all_results.append(df)
            print(df.head())
        else:
            print(f"{description} からはデータを取得できませんでした。")

        current_date += timedelta(days=1)

    if not all_results:
        print("全ての日付からデータを取得できませんでした。")
        return

    final_df = pd.concat(all_results, ignore_index=True)
    final_df = final_df.reindex(columns=META_COLUMNS + RESULT_SCHEMA)

    print(f"\n最終合計データ件数: {len(final_df)}件")
    print("\n=== 最終列名 ===")
    print(final_df.columns.tolist())

    try:
        if EXCEL_FORMATTER_AVAILABLE:
            temp_raw = OUTPUT_DIR / f"temp_raw_{timestamp}.xlsx"
            save_excel(final_df, temp_raw)
            format_excel(temp_raw, output_excel, all_sheets=True)
            temp_raw.unlink(missing_ok=True)
            print(f"整形済みデータ保存完了: {output_excel}")
        else:
            save_excel(final_df, output_excel)
            print(f"保存完了: {output_excel}")
    except Exception as e:
        print(f"Excel整形に失敗しました: {e}")
        save_excel(final_df, output_excel)
        print(f"整形せずに保存しました: {output_excel}")


if __name__ == "__main__":
    main()
