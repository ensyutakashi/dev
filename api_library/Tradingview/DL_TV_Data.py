import sys
import os

lib_path = r'C:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス\venv312\Lib\site-packages'
if lib_path not in sys.path:
    sys.path.insert(0, lib_path)

try:
    from tvDatafeed import TvDatafeed, Interval
    print("成功：tvDatafeed を認識しました。")
except ImportError:
    print("エラー：まだ読み込めません。フォルダ名を確認してください。")
    sys.exit(1)

import pandas as pd

tv = TvDatafeed()

print("TradingViewからトヨタ(7203)のデータを取得中...")

df = tv.get_hist(
    symbol='7203',
    exchange='TSE',
    interval=Interval.in_daily,
    n_bars=20000
)

if df is not None:
    df = df.reset_index()
    df['trading_value'] = df['close'] * df['volume']

    df['return_1d'] = df['close'].pct_change()
    df['return_5d'] = df['close'].pct_change(5)

    df['sma_5'] = df['close'].rolling(5).mean()
    df['sma_25'] = df['close'].rolling(25).mean()
    df['sma_75'] = df['close'].rolling(75).mean()

    df['ema_12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ema_26'] = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = df['ema_12'] - df['ema_26']
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    df['macd_hist'] = df['macd'] - df['macd_signal']

    delta = df['close'].diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    roll_up = up.rolling(14).mean()
    roll_down = down.rolling(14).mean()
    rs = roll_up / roll_down
    df['rsi_14'] = 100 - (100 / (1 + rs))

    df['bb_ma_20'] = df['close'].rolling(20).mean()
    df['bb_std_20'] = df['close'].rolling(20).std()
    df['bb_upper_20_2'] = df['bb_ma_20'] + 2 * df['bb_std_20']
    df['bb_lower_20_2'] = df['bb_ma_20'] - 2 * df['bb_std_20']

    high_low = df['high'] - df['low']
    high_close_prev = (df['high'] - df['close'].shift()).abs()
    low_close_prev = (df['low'] - df['close'].shift()).abs()
    true_range = pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1)
    df['atr_14'] = true_range.rolling(14).mean()

    df['vol_ma_20'] = df['volume'].rolling(20).mean()
    df['vol_ratio_20'] = df['volume'] / df['vol_ma_20']
    df['trading_value_ma_20'] = df['trading_value'].rolling(20).mean()
    df['trading_value_ratio_20'] = df['trading_value'] / df['trading_value_ma_20']

    output_file = "toyota_tv_history.csv"
    df.to_csv(output_file, index=False)

    print("-" * 30)
    print(f"取得件数: {len(df)} 行")
    print(f"保存完了: {output_file}")
    print("-" * 30)
else:
    print("データ取得に失敗しました。")
