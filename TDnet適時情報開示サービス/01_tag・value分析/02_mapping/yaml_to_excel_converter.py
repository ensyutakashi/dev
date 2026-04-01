# --- obsidian_property ---
# scr名: 【自動】
# 概要: YAMLマッピングをExcelに変換
# 処理grp: 
# 処理順番: 
# mermaid:
# tags: ["tool", "converter"]
# aliases: []
# created: 2026-03-31
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---


from pathlib import Path
import yaml
import pandas as pd

# excel_formatter.pyをインポート
import sys
sys.path.append(str(Path(__file__).parent.parent.parent.parent / "python"))
from excel_formatter import format_excel

def yaml_mapping_to_excel(yaml_path: Path, excel_path: Path) -> None:
    """
    YAMLマッピングファイルをExcelに変換する
    """
    # YAMLファイルをテキストとして読み込み
    with open(yaml_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Excel用のデータを作成
    rows = []
    row_num = 1
    current_jp_term = ""
    term_count = {}  # 各項目の出現回数をカウント
    
    for line in lines:
        line = line.rstrip()
        
        # 空行とコメント行をスキップ
        if not line or line.strip().startswith('#'):
            continue
        
        # concept_mapセクションをスキップ
        if line.strip() == 'concept_map:':
            continue
        
        # 日本語項目名の行（2スペースインデントで:で終わる）
        if line.startswith('  ') and ':' in line and not line.startswith('    '):
            current_jp_term = line.split(':')[0].strip()
            term_count[current_jp_term] = 0  # カウントをリセット
            continue
        
        # XBRLタグの行（4スペース以上インデントで-で始まる）
        if line.strip().startswith('- ') and current_jp_term:
            tag_line = line.strip()[2:]  # "- "を削除
            
            # コメントとXBRLタグを分離
            if '#' in tag_line:
                xbrl_tag = tag_line.split('#')[0].strip()
                comment = tag_line.split('#', 1)[1].strip()
            else:
                xbrl_tag = tag_line.strip()
                comment = ""
            
            # 最初の行のみ日本語項目名を表示
            if term_count[current_jp_term] == 0:
                jp_term_display = current_jp_term
            else:
                jp_term_display = ""
            
            term_count[current_jp_term] += 1
            
            rows.append({
                'No': row_num,
                '日本語項目名': jp_term_display,
                'XBRLタグ': xbrl_tag,
                'コメント': comment
            })
            row_num += 1
    
    # DataFrameを作成
    df = pd.DataFrame(rows)
    
    # 一時ファイルに保存
    temp_path = excel_path.with_suffix('.temp.xlsx')
    
    with pd.ExcelWriter(temp_path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name='mapping')
        
        # 列幅を調整
        ws = writer.sheets['mapping']
        ws.column_dimensions['A'].width = 8   # No
        ws.column_dimensions['B'].width = 20  # 日本語項目名
        ws.column_dimensions['C'].width = 35  # XBRLタグ
        ws.column_dimensions['D'].width = 50  # コメント
    
    # excel_formatterで整形
    try:
        format_excel(temp_path, excel_path, all_sheets=True)
        temp_path.unlink()
        print(f"[OK] Excelを整形しました: {excel_path}")
    except Exception as e:
        print(f"[WARNING] Excel整形に失敗しました: {e}")
        temp_path.rename(excel_path)

def main():
    yaml_path = Path(__file__).parent / "mapping_jp.yaml"
    excel_path = Path(__file__).parent / "mapping_jp.xlsx"
    
    if not yaml_path.exists():
        raise FileNotFoundError(f"YAMLファイルが見つかりません: {yaml_path}")
    
    print(f"YAMLファイル: {yaml_path}")
    print(f"Excel出力: {excel_path}")
    
    yaml_mapping_to_excel(yaml_path, excel_path)
    print("変換完了")

if __name__ == "__main__":
    main()
