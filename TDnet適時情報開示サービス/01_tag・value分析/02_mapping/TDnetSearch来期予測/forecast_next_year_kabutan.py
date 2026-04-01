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

    # 数字以外を除去したい場合
    s_digits = re.sub(r"[^\d]", "", s)
    if s_digits == "":
        return None

    return s_digits


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


def read_codes_from_excel(excel_path: Path, sheet_name=None, code_column_index: int = 0, has_header: bool = True) -> list[str]:
    """
    ExcelのA列からコード一覧を読む
    """
    header = 0 if has_header else None

    # sheet_nameがNoneの場合は先頭シートを読み込む（dictを避けるため）
    if sheet_name is None:
        df = pd.read_excel(excel_path, header=header)
    else:
        df = pd.read_excel(excel_path, sheet_name=sheet_name, header=header)

    if df.shape[1] <= code_column_index:
        raise ValueError(f"A列相当の列が見つかりません。列数={df.shape[1]}")

    code_series = df.iloc[:, code_column_index]

    codes = []
    for v in code_series:
        code = normalize_code(v)
        if code is not None:
            codes.append(code)

    return codes


def save_result_to_excel(result_df: pd.DataFrame, output_path: Path) -> None:
    """
    結果をExcel保存し、整形する
    """
    # 一時ファイルに保存
    temp_path = output_path.with_suffix('.temp.xlsx')
    
    with pd.ExcelWriter(temp_path, engine="openpyxl") as writer:
        result_df.to_excel(writer, index=False, sheet_name="forecast")

        ws = writer.sheets["forecast"]

        # 列幅を少し見やすく
        widths = {
            "A": 12,  # code
            "B": 45,  # url
            "C": 10,  # status
            "D": 18,  # 決算期
            "E": 14,  # 売上高
            "F": 14,  # 営業益
            "G": 14,  # 経常益
            "H": 14,  # 最終益
            "I": 40,  # error
        }
        for col, width in widths.items():
            ws.column_dimensions[col].width = width
    
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
    input_excel_path = Path(CONFIG["input_excel_path"])
    output_excel_path = Path(CONFIG["output_excel_path"])

    if not input_excel_path.exists():
        raise FileNotFoundError(f"入力Excelが見つかりません: {input_excel_path}")

    print(f"入力Excel: {input_excel_path}")

    codes = read_codes_from_excel(
        excel_path=input_excel_path,
        sheet_name=CONFIG["input_sheet_name"],
        code_column_index=CONFIG["code_column_index"],
        has_header=CONFIG["has_header"],
    )

    if not codes:
        raise ValueError("A列から有効な銘柄コードを読み込めませんでした。")

    print(f"読込コード数: {len(codes)}")

    results = []
    session = requests.Session()

    for i, code in enumerate(codes, start=1):
        print(f"[{i}/{len(codes)}] 取得中: code={code}")
        row = fetch_forecast_from_kabutan(
            code=code,
            session=session,
            timeout_sec=CONFIG["timeout_sec"],
        )
        results.append(row)

        if i < len(codes):
            time.sleep(CONFIG["sleep_sec"])

    result_df = pd.DataFrame(results)

    # 数値列をできるだけ数値化
    for col in ["売上高", "営業益", "経常益", "最終益"]:
        result_df[col] = pd.to_numeric(result_df[col], errors="coerce")

    save_result_to_excel(result_df, output_excel_path)

    print("保存完了")
    print(f"出力Excel: {output_excel_path}")


if __name__ == "__main__":
    main()