# --- obsidian_property ---
# scr名: 【自動】
# 概要: TDnet統合制御スクリプト
# 処理grp: TDnetダウンロード
# 処理順番: 1
# mermaid: "[[mermaid_TDnet適時開示情報ダウンロード]]"
# tags: ["tdnet", "download", "control", "automation"]
# aliases: ["01_run.py"]
# created: 2026-03-13
# updated: 【自動】
# folder:【自動】
# file:【自動】
# cssclasses: python_script
# --- obsidian_property ---

# --- 概要 ---
# [!abstract] 概要：TDnet統合制御スクリプト
# TDnetからPDF/XBRLをDLする5Python filesを実行する
#  1.  01_run.py　→　統合制御スクリプト
#  2.  02_tdnet_get_max_sequence_date.py　→　DB内の最新レコードを基準にTDnetから差分データを取得
#  3.  03_tdnet_uploadfile_formatter.py　→　差分データに種別・決算期・四半期・ファイル名を付与し、データ整備
#  4.  04_tdnet_pdf_download.py　　　　　→　差分データからPDF/XBRLを並列ダウンロード
#  5.  05_tdnet_db_uploader.py　　　　　　→　ダウンロードしたファイルデータをDBにアップロード
#  6.  06_tdnet_folderfile_count.py　　　 →　DBとPDF/XBRL,TDnetに誤差がないか確認
# - 各スクリプトの進捗管理と実行時間計測
# - 詳細なログ出力とエラーハンドリング
# - 処理結果のサマリー表示
# ■■■対話モード■■■
# python 01_run.py
# ■■■自動実行モード■■■
# python 01_run.py --auto
# --- 概要 ---

import os
import sys
import subprocess
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import json
from pathlib import Path

# =================================================================
# 1. 設定エリア
# =================================================================
# スクリプト格納ディレクトリ
SCRIPT_DIR = r'C:\Users\ensyu\_myfolder\work\dev\TDnet適時情報開示サービス'

# 実行するスクリプトの定義（実行順）
SCRIPTS = [
    {
        "name": "TDnet差分抽出",
        "file": "02_tdnet_get_max_sequence_date.py",
        "description": "DB内の最新レコードを基準にTDnetから差分データを取得",
        "expected_time": 300,  # 期待実行時間（秒）
        "critical": True  # 重要な処理（失敗したら中断）
    },
    {
        "name": "差分データ加工",
        "file": "03_tdnet_uploadfile_formatter.py", 
        "description": "差分データに種別・決算期・四半期・ファイル名を付与",
        "expected_time": 60,
        "critical": True
    },
    {
        "name": "PDF/XBRLダウンロード",
        "file": "04_tdnet_pdf_download.py",
        "description": "差分データからPDF/XBRLを並列ダウンロード",
        "expected_time": 600,
        "critical": True
    },
    {
        "name": "DBアップロード",
        "file": "05_tdnet_db_uploader.py",
        "description": "差分データをDuckDBに格納",
        "expected_time": 30,
        "critical": True
    },
    {
        "name": "ファイル数確認",
        "file": "06_tdnet_folderfile_count.py",
        "description": "DBとフォルダのファイル数一致を確認",
        "expected_time": 180,
        "critical": False  # 重要でない処理（失敗しても続行）
    }
]

# 作業ファイル用フォルダ（全てのファイルを格納）
WORKING_DIR = os.path.join(SCRIPT_DIR, "TDnet_report_temp_validation_log_files")
os.makedirs(WORKING_DIR, exist_ok=True)
# ログも作業フォルダに保存
LOG_DIR = WORKING_DIR

# =================================================================

class TDnetController:
    """TDnet統合制御クラス"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.results: List[Dict] = []
        self.setup_logging()
        
    def setup_logging(self):
        """ロギング設定"""
        log_filename = f"tdnet_controller_{self.start_time.strftime('%Y%m%d_%H%M%S')}.log"
        log_path = os.path.join(LOG_DIR, log_filename)
        
        # ロガー設定
        self.logger = logging.getLogger('TDnetController')
        self.logger.setLevel(logging.INFO)
        
        # ファイルハンドラ
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # コンソールハンドラ
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # フォーマッター
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y/%m/%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        # ハンドラー追加
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self.log_path = log_path
        
    def log_script_start(self, script_info: Dict):
        """スクリプト開始ログ"""
        self.logger.info("=" * 60)
        self.logger.info(f"【開始】{script_info['name']}")
        self.logger.info(f"説明: {script_info['description']}")
        self.logger.info(f"スクリプト: {script_info['file']}")
        self.logger.info(f"期待実行時間: {script_info['expected_time']}秒")
        
    def log_script_end(self, script_info: Dict, result: Dict):
        """スクリプト終了ログ"""
        elapsed = result['elapsed_time']
        status = "成功" if result['success'] else "失敗"
        
        self.logger.info(f"【完了】{script_info['name']} - {status}")
        self.logger.info(f"実行時間: {elapsed:.2f}秒 ({int(elapsed//60)}分{int(elapsed%60)}秒)")
        
        if not result['success']:
            self.logger.error(f"エラー: {result.get('error', '不明なエラー')}")
        
        self.logger.info("=" * 60)
        
    def execute_script(self, script_info: Dict) -> Dict:
        """スクリプトを実行"""
        script_path = os.path.join(SCRIPT_DIR, script_info['file'])
        
        if not os.path.exists(script_path):
            error_msg = f"スクリプトファイルが見つかりません: {script_path}"
            self.logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'elapsed_time': 0,
                'output': ''
            }
        
        self.log_script_start(script_info)
        
        try:
            start_time = time.time()
            
            # スクリプト実行
            env = os.environ.copy()
            env['PYTHONIOENCODING'] = 'utf-8'
            env['TDNET_WORKING_DIR'] = WORKING_DIR  # 作業フォルダを環境変数で渡す
            
            result = subprocess.run(
                [sys.executable, script_path],
                cwd=SCRIPT_DIR,
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace',
                env=env
            )
            
            elapsed_time = time.time() - start_time
            
            # 実行結果をログに出力
            if result.stdout:
                self.logger.info("=== 標準出力 ===")
                for line in result.stdout.split('\n'):
                    if line.strip():
                        self.logger.info(f"  {line}")
            
            if result.stderr:
                self.logger.warning("=== 標準エラー出力 ===")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        self.logger.warning(f"  {line}")
            
            # 成功判定 - 標準エラー出力とリターンコードで判定
            success = result.returncode == 0
            
            # 標準エラー出力に重要なエラーメッセージが含まれる場合は失敗と判定
            if result.stderr:
                error_patterns = [
                    'IO Error: Cannot open file',
                    'プロセスはファイルにアクセスできません',
                    '別のプロセスが使用中です',
                    'File is already open in',
                    'データベースから日付を取得できませんでした'
                ]
                
                for pattern in error_patterns:
                    if pattern in result.stderr:
                        success = False
                        break
            
            result_dict = {
                'success': success,
                'elapsed_time': elapsed_time,
                'output': result.stdout,
                'error': result.stderr if not success else None,
                'return_code': result.returncode
            }
            
            self.log_script_end(script_info, result_dict)
            return result_dict
            
        except Exception as e:
            error_msg = f"スクリプト実行エラー: {str(e)}"
            self.logger.error(error_msg)
            
            result_dict = {
                'success': False,
                'error': error_msg,
                'elapsed_time': 0,
                'output': ''
            }
            
            self.log_script_end(script_info, result_dict)
            return result_dict
    
    def run_all_scripts(self) -> bool:
        """全スクリプトを実行"""
        self.logger.info("TDnet統合制御スクリプトを開始します")
        self.logger.info(f"実行開始時刻: {self.start_time.strftime('%Y/%m/%d %H:%M:%S')}")
        self.logger.info(f"ログファイル: {self.log_path}")
        
        overall_success = True
        
        for i, script_info in enumerate(SCRIPTS, 1):
            self.logger.info(f"\n{'='*80}")
            self.logger.info(f"ステップ {i}/{len(SCRIPTS)}: {script_info['name']}")
            self.logger.info(f"{'='*80}")
            
            result = self.execute_script(script_info)
            
            # 結果を保存
            result['script_name'] = script_info['name']
            result['script_file'] = script_info['file']
            result['expected_time'] = script_info['expected_time']
            result['critical'] = script_info['critical']
            result['start_time'] = datetime.now().strftime('%Y/%m/%d %H:%M:%S')
            
            self.results.append(result)
            
            # 重要な処理が失敗したら中断
            if not result['success'] and script_info['critical']:
                self.logger.error(f"重要な処理 '{script_info['name']}' が失敗したため、処理を中断します")
                overall_success = False
                break
        
        return overall_success
    
    def generate_summary(self):
        """実行結果サマリーを生成"""
        total_time = sum(r['elapsed_time'] for r in self.results)
        successful_scripts = sum(1 for r in self.results if r['success'])
        failed_scripts = len(self.results) - successful_scripts
        
        self.logger.info("\n" + "="*80)
        self.logger.info("【実行結果サマリー】")
        self.logger.info("="*80)
        
        # 基本情報
        self.logger.info(f"開始時刻: {self.start_time.strftime('%Y/%m/%d %H:%M:%S')}")
        self.logger.info(f"終了時刻: {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}")
        self.logger.info(f"総実行時間: {int(total_time//60)}分{int(total_time%60)}秒")
        self.logger.info(f"成功スクリプト: {successful_scripts}/{len(SCRIPTS)}")
        self.logger.info(f"失敗スクリプト: {failed_scripts}")
        
        # 詳細結果
        self.logger.info("\n【詳細結果】")
        for i, result in enumerate(self.results, 1):
            status = "✅" if result['success'] else "❌"
            elapsed = result['elapsed_time']
            expected = result['expected_time']
            time_ratio = elapsed / expected if expected > 0 else 1
            
            # 時間評価
            if time_ratio <= 1.5:
                time_status = "🟢"
            elif time_ratio <= 2.0:
                time_status = "🟡"
            else:
                time_status = "🔴"
            
            self.logger.info(
                f"{i}. {status} {result['script_name']} - "
                f"{int(elapsed//60)}分{int(elapsed%60)}秒 {time_status}"
            )
            
            if not result['success']:
                self.logger.info(f"   エラー: {result.get('error', '不明なエラー')}")
        
        # ファイル出力
        self.save_summary_to_file()
        
    def save_summary_to_file(self):
        """サマリーをJSONファイルに保存"""
        summary_data = {
            'execution_info': {
                'start_time': self.start_time.isoformat(),
                'end_time': datetime.now().isoformat(),
                'log_file': self.log_path,
                'total_scripts': len(SCRIPTS),
                'successful_scripts': sum(1 for r in self.results if r['success']),
                'failed_scripts': sum(1 for r in self.results if not r['success']),
                'total_execution_time': sum(r['elapsed_time'] for r in self.results)
            },
            'script_results': self.results
        }
        
        summary_filename = f"execution_summary_{self.start_time.strftime('%Y%m%d_%H%M%S')}.json"
        summary_path = os.path.join(LOG_DIR, summary_filename)
        
        try:
            with open(summary_path, 'w', encoding='utf-8') as f:
                json.dump(summary_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"\n実行サマリーを保存しました: {summary_path}")
        except Exception as e:
            self.logger.error(f"サマリー保存エラー: {e}")
    
    def run_interactive_mode(self):
        """対話モードで実行"""
        print("TDnet統合制御スクリプト")
        print("=" * 50)
        
        print("\n実行するスクリプト:")
        for i, script in enumerate(SCRIPTS, 1):
            critical_mark = "【重要】" if script['critical'] else ""
            print(f"{i}. {script['name']} {critical_mark}")
            print(f"   {script['description']}")
        
        print(f"\nログ保存先: {LOG_DIR}")
        
        response = input("\n全スクリプトを実行しますか？ (y/N): ").strip().lower()
        
        if response in ['y', 'yes']:
            success = self.run_all_scripts()
            self.generate_summary()
            
            if success:
                print("\n🎉 全処理が正常に完了しました！")
            else:
                print("\n⚠️ 一部の処理でエラーが発生しました。ログを確認してください。")
        else:
            print("実行をキャンセルしました。")

def main():
    """メイン処理"""
    controller = TDnetController()
    
    # コマンドライン引数でモードを切り替え
    if len(sys.argv) > 1 and sys.argv[1] == '--auto':
        # 自動実行モード
        success = controller.run_all_scripts()
        controller.generate_summary()
        sys.exit(0 if success else 1)
    else:
        # 対話モード
        controller.run_interactive_mode()

if __name__ == '__main__':
    main()
