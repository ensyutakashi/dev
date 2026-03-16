from tradingview_ta import TA_Handler, Interval

# トヨタ(7203)の設定
handler = TA_Handler(
    symbol="7203",
    screener="japan",
    exchange="TSE",
    interval=Interval.INTERVAL_1_DAY
)

# データの取得
analysis = handler.get_analysis()

# インジケーターの中から「価格」と「出来高」を取り出す
close_price = analysis.indicators["close"]
volume = analysis.indicators["volume"]
trading_value = close_price * volume # 売買代金の計算

print(f"銘柄: トヨタ自動車 (7203)")
print(f"現在値: {close_price} 円")
print(f"出来高: {volume} 株")
print(f"概算売買代金: {trading_value:,.0f} 円")