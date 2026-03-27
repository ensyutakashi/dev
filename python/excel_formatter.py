# --- obsidian_property ---
# scr名: 【自動】
# 概要: Excel表整形
# 処理grp: 共通tool
# 処理順番: 
# mermaid: "[[mermaid_???]]"
# tags: ["tool", "excel"]
# aliases: ["excel_formatter.py"]
# created: 2026-03-19
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：Excel表整形

"""
font:源ノ角ゴシック Code JP R
background:white
列幅オートフィット
header:太字
先頭行の固定
"""

from __future__ import annotations

import argparse
from pathlib import Path

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill


def format_excel(input_path: Path, output_path: Path, all_sheets: bool = False) -> None:
    """Excelファイルを整形する
    
    Args:
        input_path: 入力Excelファイルパス
        output_path: 出力Excelファイルパス
        all_sheets: 全シートを整形する場合はTrue、アクティブシートのみの場合はFalse
    """
    # 入力ファイルを読み込み
    wb = load_workbook(input_path)
    
    # フォント設定（源ノ角ゴシック Code JP R）
    try:
        # Windowsの場合のフォント名
        font_name = "源ノ角ゴシック Code JP R"
        font = Font(name=font_name, size=10)
    except:
        # フォントが見つからない場合のフォールバック
        font = Font(name="Yu Gothic", size=10)
    
    # 背景を白に設定
    white_fill = PatternFill("solid", fgColor="FFFFFF")
    
    # 整形対象のシートを決定
    if all_sheets:
        # 全シートを整形
        target_sheets = wb.worksheets
        print(f"[INFO] 全{len(target_sheets)}シートを整形します")
    else:
        # アクティブシートのみを整形（従来の動作）
        target_sheets = [wb.active]
        print(f"[INFO] アクティブシートのみを整形します")
    
    # 各シートを整形
    for sheet in target_sheets:
        sheet_name = sheet.title
        print(f"[INFO] シート「{sheet_name}」を整形中...")
        
        # 使用済みセルの背景を白に設定
        for row in sheet.iter_rows():
            for cell in row:
                cell.fill = white_fill
                # ヘッダー行（1行目）は太字にする
                if cell.row == 1:
                    cell.font = Font(name=font_name if font_name else "Yu Gothic", size=10, bold=True)
                else:
                    cell.font = font

        # 列のオートフィット
        for column in sheet.columns:
            max_length = 0
            column_letter = column[0].column_letter
            
            for cell in column:
                if cell.value is not None:
                    # セルの値を文字列に変換して長さを取得
                    cell_length = len(str(cell.value))
                    if cell_length > max_length:
                        max_length = cell_length
            
            # 列幅を設定（少し余裕を持たせる）
            adjusted_width = min(max_length + 2, 50)  # 最大50文字に制限
            sheet.column_dimensions[column_letter].width = adjusted_width

        # 未使用セルもグリッド線を非表示にして白く見せる
        sheet.sheet_view.showGridLines = False
        
        print(f"[OK] シート「{sheet_name}」の整形完了")

    # 出力ファイルを保存
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(output_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Excelファイルを整形する")
    parser.add_argument("--input", required=True, help="入力Excelファイル")
    parser.add_argument("--output", required=True, help="出力Excelファイル")
    parser.add_argument("--all-sheets", action="store_true", 
                       help="全シートを整形する（デフォルトはアクティブシートのみ）")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    
    input_path = Path(args.input)
    output_path = Path(args.output)
    
    if not input_path.exists():
        print(f"[ERROR] 入力ファイルが見つかりません: {input_path}")
        return 1
    
    try:
        format_excel(input_path, output_path, all_sheets=args.all_sheets)
        sheet_info = "全シート" if args.all_sheets else "アクティブシート"
        print(f"[OK] Excelを{sheet_info}整形しました: {output_path}")
        return 0
    except Exception as exc:
        print(f"[ERROR] 整形に失敗しました: {exc}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
