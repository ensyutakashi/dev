# --- obsidian_property ---
# scr名: 【自動】
# 概要: Kabutanから最新の来期売上,営利,経常,純利予測取得
# 処理grp: -
# 処理順番: 0
# mermaid:
# tags: ["kabutan", "download"]
# aliases: ["kabutan_sales_forecast_from_excel.py"]
# created: 2026-03-31
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：入力ExcelのA列にある銘柄コードを読み込み、
# 株探 https://kabutan.jp/stock/finance?code=XXXX から
# 最新予想の 売上高・営業益・経常益・最終益 を取得して
# 新しいExcelに保存する
#
# ■ 入力Excelの想定
# - A列に銘柄コード
# - 1行目はヘッダーでもヘッダーなしでも可
# - コードは 2471 / "2471" / 02471 などでも可
#
# ■ 出力Excel
# - 入力コードごとの取得結果を一覧保存
# - スクリプトと同じフォルダに保存
# --- 概要 ---

from pathlib import Path
from datetime import datetime
from io import StringIO
import time
import re
import sys
import calendar

import pandas as pd
import requests

# excel_formatter.pyをインポート
sys.path.append(str(Path(__file__).parent.parent.parent.parent.parent / "python"))
from excel_formatter import format_excel


# =========================
# 設定
# =========================
CONFIG = {
    # 入力Excelファイル名
    # 例: codes.xlsx
    "input_excel_path": Path(__file__).parent / "codes.xlsx",

    # 読み込むシート名
    # None なら先頭シート
    "input_sheet_name": None,

    # A列を読む（0始まりで0=A列）
    "code_column_index": 0,

    # 先頭行をヘッダーとして扱うか
    # True: 1行目はヘッダー
    # False: 1行目もデータ
    "has_header": True,

    # リクエスト間隔（秒）
    "sleep_sec": 1.0,

    # タイムアウト
    "timeout_sec": 20,

    # 出力Excelファイル名
    "output_excel_path": Path(__file__).parent / f"kabutan_forecast_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
}


# =========================
# 共通処理
# =========================
def normalize_code(value) -> str | None:
    """
    ExcelのA列の値を銘柄コード文字列に整形する
    """
    if pd.isna(value):
        return None

    s = str(value).strip()

    if s == "":
        return None

    # 2471.0 のようなケース対策
    if re.fullmatch(r"\d+\.0+", s):
        s = s.split(".")[0]

    # 数字と英字のみを抽出（Aなどの英字を保持するため）
    s_digits = re.sub(r"[^\dA-Za-z]", "", s)
    if s_digits == "":
        return None

    return s_digits


def split_kessanki(value) -> tuple[str | None, datetime | None]:
    """
    決算期の文字列から「残り」と「YYYY.MM」を分割し、
    後者は月末日の日付型に変換する
    """
    if pd.isna(value):
        return None, None

    s = str(value).strip()
    if s == "":
        return None, None

    match = re.search(r"(\d{4})\.(\d{2})", s)
    if not match:
        return s, None

    year = int(match.group(1))
    month = int(match.group(2))
    last_day = calendar.monthrange(year, month)[1]
    date_value = datetime(year, month, last_day)

    remaining = (s[:match.start()] + s[match.end():]).strip()
    remaining = remaining if remaining != "" else None

    return remaining, date_value


def fetch_forecast_from_kabutan(code: str, session: requests.Session, timeout_sec: int = 20) -> dict:
    """
    株探の finance ページから最新予想の
    売上高 / 営業益 / 経常益 / 最終益 を取得する
    """
    url = f"https://kabutan.jp/stock/finance?code={code}"
    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    result = {
        "code": code,
        "url": url,
        "status": "NG",
        "決算期": None,
        "売上高": None,
        "営業益": None,
        "経常益": None,
        "最終益": None,
        "error": None,
    }

    try:
        res = session.get(url, headers=headers, timeout=timeout_sec)
        res.raise_for_status()
        res.encoding = res.apparent_encoding

        tables = pd.read_html(StringIO(res.text))

        best = None

        for df in tables:
            cols = [str(c) for c in df.columns]

            # 必須列がない表はスキップ
            if not all(x in cols for x in ["売上高", "営業益", "経常益", "最終益"]):
                continue

            c0 = df.columns[0]

            # 先頭列に「予」を含む行を探す
            mask = df[c0].astype(str).str.contains("予", na=False)
            if mask.any():
                row = df[mask].iloc[0]

                best = {
                    "決算期": row.get(c0, None),
                    "売上高": row.get("売上高", None),
                    "営業益": row.get("営業益", None),
                    "経常益": row.get("経常益", None),
                    "最終益": row.get("最終益", None),
                }
                break

        if best is None:
            result["error"] = "予想行が見つかりませんでした"
            return result

        result["status"] = "OK"
        result["決算期"] = best["決算期"]
        result["売上高"] = best["売上高"]
        result["営業益"] = best["営業益"]
        result["経常益"] = best["経常益"]
        result["最終益"] = best["最終益"]
        return result

    except Exception as e:
        result["error"] = str(e)
        return result


def read_codes_from_excel(
    excel_path: Path,
    sheet_name=None,
    code_column_index: int = 0,
    has_header: bool = True,
) -> tuple[list[dict], str]:
    """
    ExcelのA列からコード一覧を読み、B列の値も保持する
    """
    header = 0 if has_header else None

    # sheet_nameがNoneの場合は先頭シートを読み込む（dictを避けるため）
    if sheet_name is None:
        df = pd.read_excel(excel_path, header=header)
    else:
        df = pd.read_excel(excel_path, sheet_name=sheet_name, header=header)

    if df.shape[1] <= code_column_index:
        raise ValueError(f"A列相当の列が見つかりません。列数={df.shape[1]}")

    b_column_index = code_column_index + 1
    if df.shape[1] <= b_column_index:
        b_column_name = "B列"
        b_series = [None] * len(df)
    else:
        b_column_name = str(df.columns[b_column_index]) if has_header else "B列"
        b_series = df.iloc[:, b_column_index].tolist()

    code_series = df.iloc[:, code_column_index]

    rows = []
    for code_value, b_value in zip(code_series, b_series, strict=False):
        code = normalize_code(code_value)
        if code is not None:
            rows.append({"code": code, "b_value": b_value})

    return rows, b_column_name


def save_result_to_excel(result_df: pd.DataFrame, output_path: Path, b_column_name: str) -> None:
    """
    結果をExcel保存し、整形する
    """
    # 一時ファイルに保存
    temp_path = output_path.with_suffix('.temp.xlsx')
    
    with pd.ExcelWriter(temp_path, engine="openpyxl") as writer:
        result_df.to_excel(writer, index=False, sheet_name="forecast")

        ws = writer.sheets["forecast"]

        # 列幅を少し見やすく
        header_cells = {cell.value: cell.column_letter for cell in ws[1]}
        widths = {
            "code": 12,
            "売上": 14,
            "営利": 14,
            "経常": 14,
            "純利": 14,
            "決算": 18,
            "決算日": 10,
            "url": 45,
            "status": 10,
            "error": 40,
        }
        
        for header, width in widths.items():
            col = header_cells.get(header)
            if col:
                ws.column_dimensions[col].width = width

        # 入力B列の列幅
        b_header = header_cells.get(b_column_name)
        if b_header:
            ws.column_dimensions[b_header].width = 20

        # 決算日をYY/MM表示（実値は月末日付）
        date_col = header_cells.get("決算日")
        if date_col:
            for cell in ws[date_col][1:]:
                if cell.value is not None:
                    cell.number_format = "yy/mm"
    
    # excel_formatterで整形
    try:
        format_excel(temp_path, output_path, all_sheets=True)
        # 一時ファイルを削除
        temp_path.unlink()
        print(f"[OK] Excelを整形しました: {output_path}")
    except Exception as e:
        print(f"[WARNING] Excel整形に失敗しました: {e}")
        # 整形に失敗した場合は一時ファイルをリネーム
        temp_path.rename(output_path)


def main():
    start_time = datetime.now()
    input_excel_path = Path(CONFIG["input_excel_path"])
    output_excel_path = Path(CONFIG["output_excel_path"])

    if not input_excel_path.exists():
        raise FileNotFoundError(f"入力Excelが見つかりません: {input_excel_path}")

    print(f"入力Excel: {input_excel_path}")

    rows, b_column_name = read_codes_from_excel(
        excel_path=input_excel_path,
        sheet_name=CONFIG["input_sheet_name"],
        code_column_index=CONFIG["code_column_index"],
        has_header=CONFIG["has_header"],
    )

    if not rows:
        raise ValueError("A列から有効な銘柄コードを読み込めませんでした。")

    print(f"読込コード数: {len(rows)}")

    results = []
    session = requests.Session()

    for i, row in enumerate(rows, start=1):
        code = row["code"]
        print(f"[{i}/{len(rows)}] 取得中: code={code}")
        result_row = fetch_forecast_from_kabutan(
            code=code,
            session=session,
            timeout_sec=CONFIG["timeout_sec"],
        )
        result_row[b_column_name] = row["b_value"]
        results.append(result_row)

        if i < len(rows):
            time.sleep(CONFIG["sleep_sec"])

    result_df = pd.DataFrame(results)

    if "決算期" in result_df.columns:
        split_values = result_df["決算期"].apply(split_kessanki)
        result_df["決算期_残り"] = split_values.apply(lambda x: x[0])
        result_df["決算期_年月"] = split_values.apply(lambda x: x[1])
        result_df = result_df.drop(columns=["決算期"])

        result_df = result_df.rename(
            columns={
                "売上高": "売上",
                "営業益": "営利",
                "経常益": "経常",
                "最終益": "純利",
                "決算期_残り": "決算",
                "決算期_年月": "決算日",
            }
        )

        ordered_cols = [
            "code",
            b_column_name,
            "売上",
            "営利",
            "経常",
            "純利",
            "決算",
            "決算日",
            "url",
            "status",
            "error",
        ]
        remaining_cols = [c for c in result_df.columns if c not in ordered_cols]
        result_df = result_df[[c for c in ordered_cols if c in result_df.columns] + remaining_cols]

    # 数値列をできるだけ数値化
    for col in ["売上", "営利", "経常", "純利"]:
        result_df[col] = pd.to_numeric(result_df[col], errors="coerce")

    save_result_to_excel(result_df, output_excel_path, b_column_name)

    print("保存完了")
    print(f"出力Excel: {output_excel_path}")
    end_time = datetime.now()
    elapsed = end_time - start_time
    elapsed_seconds = int(elapsed.total_seconds())
    elapsed_minutes, elapsed_secs = divmod(elapsed_seconds, 60)
    print(f"実行開始時間: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"実行終了時間: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"経過時間: {elapsed_minutes:02d}:{elapsed_secs:02d}")

if __name__ == "__main__":
    main()