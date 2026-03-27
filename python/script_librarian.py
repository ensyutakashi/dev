# --- obsidian_property ---
# scr名: 【自動】
# 概要: pythonスクリプトライブラリアン
# 処理grp: script管理
# 処理順番: 0
# input: 無し
# output: python_script_list.duckdb
# mermaid: 
# tags: ["script管理", "python"]
# aliases: 
# created: 2026-02-24
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：pythonスクリプトライブラリアン
# 対象フォルダを指定して、その中のpythonスクリプトをリストアップし、DBに保存する
# --- 概要 ---

import os
import duckdb
import time
import glob
from datetime import datetime

# --- 設定 設定 設定 設定 設定 設定 設定 設定 設定 -----------------------
# 1. 対象フォルダ（複数指定可能）
SOURCE_DIRS = [
    r"C:\Users\ensyu\_myfolder\work\dev\TDnet適時情報開示サービス",
    r"C:\Users\ensyu\_myfolder\work\dev\python",        #共通のscript
    # r"C:\Users\ensyu\_myfolder\work\dev\other_project",
    # r"C:\Users\ensyu\Documents\Speculation\other_folder",
]

# 元の形式との互換性のため
SOURCE_DIR = SOURCE_DIRS[0] if SOURCE_DIRS else ""

# 対象外フォルダ
EXCLUDE_DIRS = {
    ".venv", "__pycache__", ".git", ".ipynb_checkpoints", 
    "chrome-win64", "chromedriver-win64", "BackUp", "_archive", 
    "venv", "venv312", "kabusapi-ExcelAddin-v1.11.0"
}

# 2. Obsidianの出力先ベースフォルダ
OBSIDIAN_BASE_DIR = r"C:\Users\ensyu\_myfolder\obsidian\ensyu_star_capital\python"



# 3. DB保存先（パス＆ファイル名）
DB_FOLDER = os.path.dirname(os.path.abspath(__file__)) # スクリプトのあるフォルダ
# DB_FOLDER = r"C:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス" # フォルダパス
DB_NAME = "python_script_list.duckdb" # ファイル名

# --- 設定 設定 設定 設定 設定 設定 設定 設定 設定 -----------------------


def extract_metadata(lines):
    """# --- obsidian_property --- セクションからメタデータを抽出"""
    # 依頼に基づき名称変更: script_name→scr名, description→概要, 処理グループ→処理grp
    metadata = {"概要": "", "mermaid": "", "scr名": "", "updated": "", "folder": "", "file": "", "処理順番": "", "tags": "", "aliases": "", "created": "", "処理grp": "", "cssclasses": "", "input": "", "output": ""}
    in_metadata = False
    
    for line in lines:
        line = line.strip()
        if line == "# --- obsidian_property ---":
            in_metadata = not in_metadata
            continue
            
        if in_metadata and line.startswith("#"):
            line = line[1:].strip()
            if ":" in line:
                key, value = line.split(":", 1)
                key = key.strip()
                value = value.strip()
                metadata[key] = value
    
    return metadata

def extract_overview(lines):
    """# --- 概要 --- セクションから概要を抽出"""
    overview_lines = []
    in_overview = False
    
    for line in lines:
        line_stripped = line.strip()
        if line_stripped == "# --- 概要 ---":
            in_overview = not in_overview
            continue
            
        if in_overview:
            if line_stripped:
                if line_stripped.startswith("#"):
                    clean_line = ">" + line_stripped[1:]
                    overview_lines.append(clean_line)
                else:
                    overview_lines.append(f"> {line_stripped}")
    
    return "\n".join(overview_lines).strip() if overview_lines else ""

def extract_overview_plain(lines):
    overview_lines = []
    in_overview = False
    first_processed = False
    for line in lines:
        s = line.strip()
        if s == "# --- 概要 ---":
            in_overview = not in_overview
            continue
        if in_overview:
            if not s:
                continue
            if s.startswith("#"):
                s = s[1:].strip()
            if not first_processed and s.startswith("[!abstract]"):
                s = s[len("[!abstract]"):].strip()
            first_processed = True
            if s.startswith(">"):
                s = s[1:].strip()
            overview_lines.append(s)
    return "\n".join(overview_lines).strip() if overview_lines else ""
def normalize_mermaid(value: str) -> str:
    v = (value or "").strip()
    if not v:
        return ""
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        v = v[1:-1].strip()
    if v.startswith("[[") and v.endswith("]]"):
        v = v[2:-2].strip()
    if not v.lower().endswith(".md"):
        v = f"{v}.md"
    return v

def parse_timestamp(value: str, fallback_dt: datetime) -> datetime:
    s = (value or "").strip()
    fmts = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            # 補完: 時分までの場合は秒を00に
            if fmt == "%Y-%m-%d %H:%M":
                return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, 0)
            # 補完: 日付のみの場合は00:00:00に
            if fmt == "%Y-%m-%d":
                return datetime(dt.year, dt.month, dt.day, 0, 0, 0)
            return dt
        except Exception:
            continue
    return fallback_dt

def strip_file_scheme(p: str) -> str:
    s = (p or "").strip()
    if s.lower().startswith("file:///"):
        s = s[8:]
    return s.replace("\\", "/")
def extract_info(file_path):
    """ファイルから概要とコード内容を抽出"""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
            content = "".join(lines)
            description = "説明なし"
            if not lines:
                return description, content, {}

            metadata = extract_metadata(lines)
            
            overview = extract_overview(lines)
            overview_plain = extract_overview_plain(lines)
            if overview:
                description = overview
            elif metadata.get("概要"):
                description = metadata["概要"]
            else:
                first_line = lines[0].strip()
                if first_line.startswith(('"""', "'''")):
                    desc_lines = []
                    quote_type = first_line[:3]
                    first_content = first_line[3:]
                    if first_content:
                        desc_lines.append(first_content)
                    for line in lines[1:]:
                        if quote_type in line:
                            desc_lines.append(line.split(quote_type)[0])
                            break
                        desc_lines.append(line.strip())
                    description = " ".join(desc_lines).strip()
                elif first_line.startswith("#"):
                    description = first_line.replace("#", "").strip()
                
            if overview_plain:
                metadata["abstract"] = overview_plain
            else:
                metadata["abstract"] = metadata.get("概要", "")
            return description, content, metadata
    except Exception as e:
        return f"エラー: {str(e)}", "", {}

def run():
    start_time = time.time()
    
    # 各ソースフォルダに対応するObsidian出力先をクリーンアップ
    for source_dir in SOURCE_DIRS:
        if not os.path.exists(source_dir):
            continue
            
        # ソースフォルダ名を取得
        source_folder_name = os.path.basename(source_dir.rstrip('\\/'))
        obsidian_target_dir = os.path.join(OBSIDIAN_BASE_DIR, source_folder_name)
        
        # Obsidian出力先のクリーンアップ（サブフォルダも含めて再帰的に削除）
        if os.path.exists(obsidian_target_dir):
            import shutil
            try:
                shutil.rmtree(obsidian_target_dir)
                print(f"Obsidian出力先をクリーンアップしました: {obsidian_target_dir}")
            except Exception as e:
                print(f"クリーンアップエラー: {e}")
        
        # 再作成
        os.makedirs(obsidian_target_dir, exist_ok=True)
        
    if not os.path.exists(DB_FOLDER):
        os.makedirs(DB_FOLDER)

    db_full_path = os.path.join(DB_FOLDER, DB_NAME)
    db_success = False
    db_used_name = DB_NAME
    
    try:
        con = duckdb.connect(db_full_path)
        db_success = True
    except Exception as e:
        print(f"DB接続エラー: {e}")
        alt_path = os.path.join(DB_FOLDER, "python_script_list_unlocked.duckdb")
        print(f"代替DBに接続します: {alt_path}")
        con = duckdb.connect(alt_path)
        db_used_name = "python_script_list_unlocked.duckdb"
    con.execute("DROP TABLE IF EXISTS scripts")
    con.execute("""
        CREATE TABLE IF NOT EXISTS scripts (
            folder TEXT,
            scr名 TEXT,
            概要 TEXT,
            処理grp TEXT,
            処理順番 DECIMAL(7, 3),
            input TEXT,
            output TEXT,
            mermaid TEXT,
            tags TEXT,
            aliases TEXT,
            created DATE,
            updated TIMESTAMP,
            file TEXT,
            abstract TEXT,
            content TEXT
        )
    """)

    processed_files = []

    # 複数の対象フォルダを処理
    for source_dir in SOURCE_DIRS:
        if not os.path.exists(source_dir):
            print(f"警告: フォルダが存在しません: {source_dir}")
            continue
            
        # ソースフォルダ名を取得してObsidian出力先を決定
        source_folder_name = os.path.basename(source_dir.rstrip('\\/'))
        obsidian_target_dir = os.path.join(OBSIDIAN_BASE_DIR, source_folder_name)
        
        print(f"処理中のフォルダ: {source_dir}")
        print(f"Obsidian出力先: {obsidian_target_dir}")
        
        for root, dirs, files in os.walk(source_dir):
            dirs[:] = [d for d in dirs if d not in EXCLUDE_DIRS]

            for file in files:
                if file.endswith(".py") and file != "index_scripts.py":
                    full_path = os.path.join(root, file)
                    desc, code, metadata = extract_info(full_path)
                    mtime = datetime.fromtimestamp(os.path.getmtime(full_path))

                    # 自動補完用の値
                    formatted_folder = f"file:///{os.path.abspath(root).replace(chr(92), '/')}"
                    formatted_file = f"file:///{os.path.abspath(full_path).replace(chr(92), '/')}"
                    formatted_time = mtime.strftime('%Y-%m-%d %H:%M')

                    # メタデータの自動セット（既存値が【自動】または空の場合）
                    if metadata.get('scr名') in ['', '【自動】']: metadata['scr名'] = file
                    if metadata.get('updated') in ['', '【自動】']: metadata['updated'] = formatted_time
                    if metadata.get('folder') in ['', '【自動】']: metadata['folder'] = formatted_folder
                    if metadata.get('file') in ['', '【自動】']: metadata['file'] = formatted_file
                    # cssclasses のデフォルトセット
                    if metadata.get('cssclasses') in ['', '【自動】']: metadata['cssclasses'] = "python_script"

                    # 相対パスに基づいてObsidian用のサブフォルダを作成
                    folder_rel_path = os.path.relpath(root, source_dir)
                    if folder_rel_path == '.':
                        obsidian_subdir = obsidian_target_dir
                    else:
                        obsidian_subdir = os.path.join(obsidian_target_dir, folder_rel_path)
                        os.makedirs(obsidian_subdir, exist_ok=True)
                    
                    md_filename = f"{file}.md"
                    output_path = os.path.join(obsidian_subdir, md_filename)
                    # 相対パスをDB用に取得
                    folder_rel_path = os.path.relpath(root, source_dir)
                    if folder_rel_path == '.':
                        folder_rel_path = ''
                    
                    proc_raw = str(metadata.get('処理順番', '')).strip()
                    try:
                        proc_value = float(proc_raw) if proc_raw else None
                    except Exception:
                        proc_value = None

                    con.execute("""
                        INSERT INTO scripts (folder, scr名, 概要, 処理grp, 処理順番, input, output, mermaid, tags, aliases, created, updated, file, abstract, content)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        strip_file_scheme(metadata.get('folder', '')),
                        metadata.get('scr名', ''),
                        metadata.get('概要', ''),
                        metadata.get('処理grp', ''),
                        proc_value,
                        metadata.get('input', ''),
                        metadata.get('output', ''),
                        normalize_mermaid(metadata.get('mermaid', '')),
                        metadata.get('tags', '["python_script", "tools"]'),
                        metadata.get('aliases', '[]'),
                        parse_timestamp(metadata.get('created', mtime.strftime('%Y-%m-%d')), mtime).date(),
                        parse_timestamp(metadata.get('updated', formatted_time), mtime),
                        strip_file_scheme(metadata.get('file', '')),
                        metadata.get('abstract', ''),
                        code
                    ))

                    with open(output_path, "w", encoding="utf-8") as f:
                        f.write("---\n")
                        f.write(f"scr名: {metadata.get('scr名', '')}\n")
                        f.write(f"概要: {metadata.get('概要', '')}\n")
                        f.write(f"処理grp: {metadata.get('処理grp', '')}\n") # 名称変更箇所
                        f.write(f"処理順番: {metadata.get('処理順番', '')}\n")
                        f.write(f"input: {metadata.get('input', '')}\n")
                        f.write(f"output: {metadata.get('output', '')}\n")
                        f.write(f"mermaid: {metadata.get('mermaid', '')}\n")
                        f.write(f"tags: {metadata.get('tags', '[\"python_script\", \"tools\"]')}\n")
                        f.write(f"aliases: {metadata.get('aliases', '[]')}\n")
                        f.write(f"created: {metadata.get('created', mtime.strftime('%Y-%m-%d'))}\n")
                        f.write(f"updated: {metadata.get('updated', '')}\n")
                        # folder と file をリスト形式に修正
                        f.write("folder:\n")
                        f.write(f"  - {metadata.get('folder', '')}\n")
                        f.write("file:\n")
                        f.write(f"  - {metadata.get('file', '')}\n")
                        f.write(f"cssclasses: {metadata.get('cssclasses', '')}\n") # cssclasses追加
                        f.write("---\n")
                        f.write("---\n\n")
                        
                        f.write(f"{desc}\n\n")

                        f.write("## スクリプト情報\n")
                        f.write(f"- **フォルダ**:{metadata.get('folder', '')}\n")
                        f.write(f"- **ファイル**:{metadata.get('file', '')}\n")
                        f.write("\n---\n\n")
                        f.write("## ソースコード\n\n")
                        f.write("```python\n")
                        f.write(code)
                        f.write("\n```\n")
                    
                    processed_files.append(file)

    con.close()
    
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Obsidian出力結果の確認
    obsidian_success = len(processed_files) > 0
    obsidian_files = 0
    
    # 各ソースフォルダに対応するObsidianフォルダのファイル数をカウント
    for source_dir in SOURCE_DIRS:
        if not os.path.exists(source_dir):
            continue
        source_folder_name = os.path.basename(source_dir.rstrip('\\/'))
        obsidian_target_dir = os.path.join(OBSIDIAN_BASE_DIR, source_folder_name)
        if os.path.exists(obsidian_target_dir):
            obsidian_files += len([f for f in os.listdir(obsidian_target_dir) if f.endswith('.md')])

    print("-" * 30)
    print(f"完了！")
    print(f"処理ファイル数: {len(processed_files)} 個")
    print(f"実行時間: {elapsed_time:.2f} 秒")
    
    # DB結果
    if db_success:
        print(f"DB:成功　\"{db_used_name}\"に保存")
    else:
        print(f"DB:失敗　\"{db_used_name}\"に代替保存")
    
    # Obsidian結果
    if obsidian_success:
        print(f"obsidian:成功 ({obsidian_files}個のファイル)")
    else:
        print(f"obsidian:失敗")
    
    print("-" * 30)

if __name__ == "__main__":
    run()
