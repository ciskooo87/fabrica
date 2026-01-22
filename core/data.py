# DATA_PROVIDER_VERSION = "STOOQ_FIRST_IN_ACTIONS_v1"
import os
import time
import pandas as pd

def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(columns=str.title)

    try:
        df = df.sort_index()
    except Exception:
        pass

    needed = ["Open", "High", "Low", "Close"]
    if not all(c in df.columns for c in needed):
        return pd.DataFrame()

    if "Volume" not in df.columns:
        df["Volume"] = 0

    df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
    return df


def _to_stooq_symbol(ticker: str) -> str:
    t = ticker.strip()
    if "." in t:
        return t.lower()
    return f"{t}.US".lower()


def fetch_history_stooq(ticker: str, years: int = 10) -> pd.DataFrame:
    from pandas_datareader import data as pdr

    end = pd.Timestamp.utcnow().normalize()
    start = end - pd.DateOffset(years=years)

    symbol = _to_stooq_symbol(ticker)
    df = pdr.DataReader(symbol, "stooq", start, end)
    df = _normalize_ohlcv(df)

    if df.empty:
        raise ValueError(f"Stooq sem dados para {ticker} (symbol={symbol})")

    return df


def fetch_history_yfinance(ticker: str, period: str = "10y", interval: str = "1d", retries: int = 3) -> pd.DataFrame:
    import yfinance as yf

    last_err = None
    for attempt in range(retries):
        try:
            df = yf.download(
                ticker,
                period=period,
                interval=interval,
                auto_adjust=False,
                progress=False,
                threads=False,
            )
            df = df.rename(columns=str.title)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]
            df = _normalize_ohlcv(df)
            if not df.empty:
                return df
        except Exception as e:
            last_err = e
        time.sleep(1.0 * (attempt + 1))

    raise ValueError(f"yfinance sem dados para {ticker}. Erro: {repr(last_err)}")


def fetch_history(ticker: str, period: str = "10y", interval: str = "1d") -> pd.DataFrame:
    """
    No GitHub Actions: usa Stooq (mais estável).
    Fora do Actions: tenta Yahoo, depois Stooq.
    """
    in_actions = os.getenv("GITHUB_ACTIONS", "").lower() == "true"

    if in_actions:
        if interval != "1d":
            raise ValueError("No Actions, apenas interval=1d (MVP).")
        return fetch_history_stooq(ticker, years=10)

    try:
        return fetch_history_yfinance(ticker, period=period, interval=interval)
    except Exception:
        if interval != "1d":
            raise
        return fetch_history_stooq(ticker, years=10)
