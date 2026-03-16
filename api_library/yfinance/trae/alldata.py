import os
from pathlib import Path
import shutil
import sys
import tempfile
import time

import certifi
import pandas as pd
import yfinance as yf

# ============================================================
# 設定：ここを変更してください
# ============================================================
TICKER_CODE = "7203.T"   # 取得したい銘柄コード（例: 7203.T = トヨタ）
if len(sys.argv) >= 2 and sys.argv[1].strip():
    TICKER_CODE = sys.argv[1].strip()

OUTPUT_FILE = str((Path(__file__).resolve().parent / f"{TICKER_CODE.replace('.', '_')}_全データ.xlsx"))
# ============================================================

_cacert_src = certifi.where()
_cacert_dst = str(Path(tempfile.gettempdir()) / "yfinance_cacert.pem")
try:
    if (not os.path.exists(_cacert_dst)) or (os.path.getmtime(_cacert_dst) < os.path.getmtime(_cacert_src)):
        shutil.copyfile(_cacert_src, _cacert_dst)
except Exception:
    _cacert_dst = _cacert_src

os.environ.setdefault("CURL_CA_BUNDLE", _cacert_dst)
os.environ.setdefault("SSL_CERT_FILE", _cacert_dst)
os.environ.setdefault("REQUESTS_CA_BUNDLE", _cacert_dst)

ticker = yf.Ticker(TICKER_CODE)

def normalize_sheet_name(name: str) -> str:
    invalid = ['\\', '/', '*', '?', ':', '[', ']']
    for ch in invalid:
        name = name.replace(ch, "_")
    name = name.strip()
    if not name:
        name = "Sheet"
    return name[:31]


def is_empty_data(data) -> bool:
    if data is None:
        return True
    if isinstance(data, pd.DataFrame):
        return data.empty
    if isinstance(data, pd.Series):
        return data.empty
    if isinstance(data, (list, tuple, dict)):
        return len(data) == 0
    if isinstance(data, str):
        return len(data.strip()) == 0
    return False


def to_excel_compatible(data):
    if isinstance(data, dict):
        return pd.Series(data)
    if isinstance(data, (list, tuple)):
        if len(data) == 0:
            return pd.DataFrame()
        if all(isinstance(x, dict) for x in data):
            return pd.DataFrame(data)
        return pd.DataFrame({"value": list(data)})
    if isinstance(data, str):
        return pd.Series([data], name="value")
    return data


def make_excel_safe(data):
    if isinstance(data, pd.DataFrame):
        df = data.copy()
        if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
            df.index = df.index.tz_convert(None)
        for col in df.columns:
            if isinstance(df[col].dtype, pd.DatetimeTZDtype):
                df[col] = df[col].dt.tz_convert(None)
        return df
    if isinstance(data, pd.Series):
        s = data.copy()
        if isinstance(s.index, pd.DatetimeIndex) and s.index.tz is not None:
            s.index = s.index.tz_convert(None)
        if isinstance(s.dtype, pd.DatetimeTZDtype):
            s = s.dt.tz_convert(None)
        return s
    return data


def safe_fetch_save(writer, fetch_fn, sheet_name, used_sheet_names):
    sheet_name = normalize_sheet_name(sheet_name)
    original = sheet_name
    i = 2
    while sheet_name in used_sheet_names:
        suffix = f"_{i}"
        sheet_name = normalize_sheet_name(original[: (31 - len(suffix))] + suffix)
        i += 1
    used_sheet_names.add(sheet_name)

    try:
        data = fetch_fn()
    except Exception as e:
        print(f"  ❌ エラー: {sheet_name} → {e}")
        return

    if is_empty_data(data):
        print(f"  スキップ（空）: {sheet_name}")
        return

    data = make_excel_safe(to_excel_compatible(data))
    try:
        if hasattr(data, "to_excel"):
            data.to_excel(writer, sheet_name=sheet_name)
        else:
            pd.Series([str(data)], name="value").to_excel(writer, sheet_name=sheet_name)
        print(f"  ✅ 取得成功: {sheet_name}")
    except Exception as e:
        print(f"  ❌ エラー: {sheet_name} → {e}")


print(f"\n{'='*50}")
print(f"  取得開始: {TICKER_CODE}")
print(f"  出力ファイル: {OUTPUT_FILE}")
print(f"{'='*50}\n")

with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    used_sheet_names = set()

    # === 企業基本情報 ===
    print("【企業基本情報】")
    safe_fetch_save(writer, lambda: ticker.info, "企業情報_info", used_sheet_names)
    safe_fetch_save(writer, lambda: ticker.fast_info, "企業情報_fast_info", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "basic_info", None), "企業情報_basic_info", used_sheet_names)
    safe_fetch_save(writer, lambda: ticker.isin, "ISIN番号", used_sheet_names)
    safe_fetch_save(writer, lambda: ticker.calendar, "決算配当予定日", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "sustainability", None), "ESGスコア", used_sheet_names)
    time.sleep(0.5)

    # === 株価・取引情報 ===
    print("\n【株価・取引情報】")
    safe_fetch_save(writer, lambda: ticker.history(period="max"), "株価履歴", used_sheet_names)
    safe_fetch_save(writer, lambda: ticker.actions, "配当＋分割まとめ", used_sheet_names)
    safe_fetch_save(writer, lambda: ticker.dividends, "配当履歴", used_sheet_names)
    safe_fetch_save(writer, lambda: ticker.splits, "株式分割履歴", used_sheet_names)
    safe_fetch_save(writer, lambda: ticker.capital_gains, "キャピタルゲイン", used_sheet_names)
    safe_fetch_save(
        writer,
        lambda: ticker.get_shares_full(start="2000-01-01") if hasattr(ticker, "get_shares_full") else None,
        "発行済株式数履歴",
        used_sheet_names,
    )
    time.sleep(0.5)

    # === 財務諸表 ===
    print("\n【財務諸表】")
    safe_fetch_save(writer, lambda: ticker.income_stmt, "PL_年次", used_sheet_names)
    safe_fetch_save(writer, lambda: ticker.quarterly_income_stmt, "PL_四半期", used_sheet_names)
    safe_fetch_save(writer, lambda: ticker.balance_sheet, "BS_年次", used_sheet_names)
    safe_fetch_save(writer, lambda: ticker.quarterly_balance_sheet, "BS_四半期", used_sheet_names)
    safe_fetch_save(writer, lambda: ticker.cash_flow, "CF_年次", used_sheet_names)
    safe_fetch_save(
        writer,
        lambda: getattr(ticker, "quarterly_cash_flow") if hasattr(ticker, "quarterly_cash_flow") else getattr(ticker, "quarterly_cashflow"),
        "CF_四半期",
        used_sheet_names,
    )
    time.sleep(0.5)

    # === アナリスト・予想情報 ===
    print("\n【アナリスト・予想情報】")
    safe_fetch_save(writer, lambda: getattr(ticker, "analyst_price_targets", None), "目標株価", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "recommendations", None), "アナリスト推奨", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "recommendations_summary", None), "推奨サマリ", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "upgrades_downgrades", None), "格付け変更", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "earnings_estimate", None), "EPS予想", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "earnings_history", None), "EPS実績履歴", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "eps_revisions", None), "EPS修正履歴", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "eps_trend", None), "EPSトレンド", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "revenue_estimate", None), "売上予想", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "earnings_dates", None), "決算発表日", used_sheet_names)
    time.sleep(0.5)

    # === 株主情報 ===
    print("\n【株主情報】")
    safe_fetch_save(writer, lambda: getattr(ticker, "major_holders", None), "主要株主", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "institutional_holders", None), "機関投資家", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "mutualfund_holders", None), "投資信託保有", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "insider_transactions", None), "インサイダー取引", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "insider_purchases", None), "インサイダー購入", used_sheet_names)
    safe_fetch_save(writer, lambda: getattr(ticker, "insider_roster_holders", None), "インサイダー保有者", used_sheet_names)
    time.sleep(0.5)

    # === ニュース ===
    print("\n【ニュース】")
    safe_fetch_save(writer, lambda: getattr(ticker, "news", None), "ニュース", used_sheet_names)

    # === オプション ===
    print("\n【オプション】")
    safe_fetch_save(writer, lambda: getattr(ticker, "options", None), "オプション満期日", used_sheet_names)

print(f"\n{'='*50}")
print(f"  完了！ → {OUTPUT_FILE}")
print(f"{'='*50}\n")
