import yfinance as yf
import pandas as pd
from pathlib import Path

tickers = {
    "Aramco": "2222.SR",
    "STC": "7010.SR",
    "Al_Rajhi": "1120.SR"
}

output_dir = Path("data/raw")
output_dir.mkdir(parents=True, exist_ok=True)

for name, symbol in tickers.items():
    print(f"Downloading data for {name} ({symbol})...")
    df = yf.download(symbol, start="2024-01-01", end="2025-01-01")
    csv_path = output_dir / f"{name.lower()}_saudi.csv"
    df.to_csv(csv_path)
    print(f"Saved to {csv_path}\n")
