import pandas as pd
import yfinance as yf

def fetch_history(ticker: str, period: str = "10y", interval: str = "1d") -> pd.DataFrame:
    df = yf.download(ticker, period=period, interval=interval, auto_adjust=False, progress=False)
    if df is None or df.empty:
        raise ValueError(f"Sem dados para {ticker}")

    # Padroniza colunas
    df = df.rename(columns=str.title)

    needed = ["Open", "High", "Low", "Close", "Volume"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas ausentes para {ticker}: {missing}")

    df = df[needed].dropna()
    return df
