import pandas as pd
import sys

file_path = r'c:\Users\ensyu\Documents\Speculation\TDnet\TDnet適時情報開示サービス\Quants\rakuten\楽天証券RSS_個別銘柄.xlsx'
try:
    # Read first 10 rows
    df = pd.read_excel(file_path, nrows=10)
    print("Columns:", df.columns.tolist())
    print("\nData:")
    print(df.to_string())
except Exception as e:
    print(f"Error: {e}")
