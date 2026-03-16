import yfinance as yf
import pandas as pd
import time
import os
import shutil
import tempfile
import certifi

# ============================================================
# SSL証明書パスに日本語が含まれる場合の回避策 (curl_cffi対策)
# ============================================================
def setup_ssl():
    try:
        original_cert = certifi.where()
        # 一時ディレクトリ（日本語を含まない可能性が高い場所）にコピー
        temp_dir = tempfile.gettempdir() 
        safe_cert_path = os.path.join(temp_dir, "cacert_yfinance.pem")
        shutil.copy2(original_cert, safe_cert_path)
        os.environ["CURL_CA_BUNDLE"] = safe_cert_path
        os.environ["SSL_CERT_FILE"] = safe_cert_path
        print(f"  SSL証明書を再設定しました: {safe_cert_path}")
    except Exception as e:
        print(f"  Warning: SSL証明書の再設定に失敗しました: {e}")

setup_ssl()

# ============================================================
# 設定：ここを変更してください
# ============================================================
TICKER_CODE = "7203.T"   # 取得したい銘柄コード
# スクリプトと同じフォルダに保存するように設定
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, f"{TICKER_CODE.replace('.', '_')}_全データ.xlsx")
# ============================================================

ticker = yf.Ticker(TICKER_CODE)

def safe_save(writer, data_func, sheet_name):
    """データが空・None・エラーでも止まらず安全に保存する"""
    try:
        # プロパティへのアクセス自体でエラーが出るのを防ぐために関数(lambda)を受け取る
        data = data_func()
        
        if data is None:
            print(f"  [SKIP] {sheet_name} (None)")
            return
            
        # FastInfoなどの独自オブジェクト対策（辞書に変換）
        if hasattr(data, "items") and not isinstance(data, (dict, pd.DataFrame, pd.Series)):
            # 独自オブジェクトから辞書風にデータを抜く（yfinanceの仕様変更対策）
            try:
                data = {k: getattr(data, k) for k in dir(data) if not k.startswith('_') and not callable(getattr(data, k))}
            except:
                pass

        if isinstance(data, pd.DataFrame) and data.empty:
            print(f"  [SKIP] {sheet_name} (Empty DataFrame)")
            return
        if isinstance(data, pd.Series) and data.empty:
            print(f"  [SKIP] {sheet_name} (Empty Series)")
            return
        if isinstance(data, (list, tuple)) and len(data) == 0:
            print(f"  [SKIP] {sheet_name} (Empty List)")
            return
        if isinstance(data, dict) and len(data) == 0:
            print(f"  [SKIP] {sheet_name} (Empty Dict)")
            return

        # インデックスのタイムゾーン除去（Excel保存エラー対策）
        if hasattr(data, 'index') and hasattr(data.index, 'tz_localize'):
            try:
                data.index = data.index.tz_localize(None)
            except:
                pass

        if isinstance(data, dict):
            pd.Series(data).to_excel(writer, sheet_name=sheet_name)
        elif isinstance(data, list):
            # ニュースなどのリスト形式
            pd.DataFrame(data).to_excel(writer, sheet_name=sheet_name)
        elif isinstance(data, str):
            pd.Series([data]).to_excel(writer, sheet_name=sheet_name)
        elif hasattr(data, 'to_excel'):
            data.to_excel(writer, sheet_name=sheet_name)
        else:
            # それ以外（独自オブジェクトなど）
            pd.Series(vars(data) if hasattr(data, '__dict__') else str(data)).to_excel(writer, sheet_name=sheet_name)

        print(f"  [SUCCESS] {sheet_name}")

    except Exception as e:
        print(f"  [ERROR] {sheet_name} -> {str(e)[:100]}")


print(f"\n{'='*50}")
print(f"  START: {TICKER_CODE}")
print(f"  OUTPUT: {OUTPUT_FILE}")
print(f"{'='*50}\n")

# ExcelWriterの開始
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    
    # 最初の一枚（IndexError回避用）
    summary = pd.DataFrame({"Ticker": [TICKER_CODE], "DateTime": [pd.Timestamp.now()]})
    summary.to_excel(writer, sheet_name="EXECUTION_LOG")

    # === 企業基本情報 ===
    print("--- Basic Info ---")
    safe_save(writer, lambda: ticker.info,       "INFO")
    safe_save(writer, lambda: ticker.fast_info,  "FAST_INFO")
    safe_save(writer, lambda: ticker.basic_info, "BASIC_INFO")
    safe_save(writer, lambda: ticker.isin,       "ISIN")
    safe_save(writer, lambda: ticker.calendar,   "CALENDAR")
    safe_save(writer, lambda: ticker.sustainability, "ESG")
    time.sleep(0.5)

    # === 株価・取引情報 ===
    print("\n--- Market Data ---")
    safe_save(writer, lambda: ticker.history(period="max"), "PRICE_HISTORY")
    safe_save(writer, lambda: ticker.actions,    "ACTIONS")
    safe_save(writer, lambda: ticker.dividends,  "DIVIDENDS")
    safe_save(writer, lambda: ticker.splits,     "SPLITS")
    safe_save(writer, lambda: ticker.capital_gains, "CAPITAL_GAINS")
    # shares_full はメソッドとして呼び出しが必要な場合がある
    safe_save(writer, lambda: ticker.get_shares_full(start="2000-01-01") if hasattr(ticker, 'get_shares_full') else ticker.shares_full, "SHARES_HISTORY")
    time.sleep(0.5)

    # === 財務諸表 ===
    print("\n--- Financial Statements ---")
    safe_save(writer, lambda: ticker.income_stmt,            "INCOME_STMT_ANNUAL")
    safe_save(writer, lambda: ticker.quarterly_income_stmt,  "INCOME_STMT_QUARTER")
    safe_save(writer, lambda: ticker.balance_sheet,          "BALANCE_SHEET_ANNUAL")
    safe_save(writer, lambda: ticker.quarterly_balance_sheet,"BALANCE_SHEET_QUARTER")
    safe_save(writer, lambda: ticker.cash_flow,              "CASHFLOW_ANNUAL")
    safe_save(writer, lambda: ticker.quarterly_cashflow,     "CASHFLOW_QUARTER")
    time.sleep(0.5)

    # === アナリスト・予想情報 ===
    print("\n--- Analysis & Estimates ---")
    safe_save(writer, lambda: ticker.analyst_price_targets, "PRICE_TARGETS")
    safe_save(writer, lambda: ticker.recommendations,       "RECOMMENDATIONS")
    safe_save(writer, lambda: ticker.upgrades_downgrades,   "UPGRADES_DOWNGRADES")
    safe_save(writer, lambda: ticker.earnings_estimate,     "EARNINGS_EST")
    safe_save(writer, lambda: ticker.earnings_history,      "EARNINGS_HISTORY")
    safe_save(writer, lambda: ticker.eps_revisions,         "EPS_REVISIONS")
    safe_save(writer, lambda: ticker.eps_trend,             "EPS_TREND")
    safe_save(writer, lambda: ticker.revenue_estimate,      "REVENUE_EST")
    safe_save(writer, lambda: ticker.earnings_forecasts,    "EARNINGS_FORECASTS")
    safe_save(writer, lambda: ticker.revenue_forecasts,     "REVENUE_FORECASTS")
    safe_save(writer, lambda: ticker.earnings_dates,        "EARNINGS_DATES")
    time.sleep(0.5)

    # === 株主情報 ===
    print("\n--- Holders ---")
    safe_save(writer, lambda: ticker.major_holders,          "MAJOR_HOLDERS")
    safe_save(writer, lambda: ticker.institutional_holders,  "INST_HOLDERS")
    safe_save(writer, lambda: ticker.mutualfund_holders,     "MUTUALFUND_HOLDERS")
    safe_save(writer, lambda: ticker.insider_transactions,   "INSIDER_TRANS")
    safe_save(writer, lambda: ticker.insider_purchases,      "INSIDER_PURCHASES")
    safe_save(writer, lambda: ticker.insider_roster_holders, "INSIDER_ROSTER")
    time.sleep(0.5)

    # === ニュース ===
    print("\n--- News ---")
    safe_save(writer, lambda: ticker.news, "NEWS")

    # === オプション ===
    # print("\n--- Options ---")
    # safe_save(writer, lambda: ticker.options, "OPTIONS")

print(f"\n{'='*50}")
print(f"  FINISH! -> {OUTPUT_FILE}")
print(f"{'='*50}\n")
