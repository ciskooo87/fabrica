import os
import time
import io
import pandas as pd
import requests

DATA_PROVIDER_VERSION = "STOOQ_CSV_IN_ACTIONS_v3"


def _to_stooq_symbol(ticker: str) -> str:
    t = ticker.strip()
    if "." in t:
        return t.lower()
    return f"{t}.us".lower()


def fetch_history_stooq_csv(ticker: str, retries: int = 4, timeout: int = 25) -> pd.DataFrame:
    symbol = _to_stooq_symbol(ticker)
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"

    last_err = None
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()

            text = r.text.strip()
            if not text or "Date,Open,High,Low,Close" not in text:
                raise ValueError(f"Resposta inválida do Stooq para {ticker} ({symbol})")

            df = pd.read_csv(io.StringIO(text))
            if df.empty or "Close" not in df.columns:
                raise ValueError(f"CSV vazio/ inválido do Stooq para {ticker} ({symbol})")

            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            df = df.dropna(subset=["Date"]).set_index("Date").sort_index()

            df = df.rename(columns=str.title)
            needed = ["Open", "High", "Low", "Close"]
            if not all(c in df.columns for c in needed):
                raise ValueError(f"CSV sem OHLC completo para {ticker} ({symbol})")

            if "Volume" not in df.columns:
                df["Volume"] = 0

            df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
            return df

        except Exception as e:
            last_err = e
            time.sleep(1.5 * (attempt + 1))

    raise ValueError(f"Stooq CSV falhou para {ticker}. Erro: {repr(last_err)}")


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
            if df is None or df.empty:
                raise ValueError("yfinance retornou vazio")

            df = df.rename(columns=str.title)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [c[0] for c in df.columns]

            needed = ["Open", "High", "Low", "Close"]
            if not all(c in df.columns for c in needed):
                raise ValueError("yfinance sem OHLC completo")

            if "Volume" not in df.columns:
                df["Volume"] = 0

            df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
            return df

        except Exception as e:
            last_err = e
            time.sleep(1.0 * (attempt + 1))

    raise ValueError(f"yfinance falhou para {ticker}. Erro: {repr(last_err)}")


def fetch_history(ticker: str, period: str = "10y", interval: str = "1d") -> pd.DataFrame:
    """
    Política implacável:
    - GitHub Actions => Stooq CSV (não usa Yahoo)
    - Local => Yahoo primeiro, fallback Stooq CSV
    """
    in_actions = os.getenv("GITHUB_ACTIONS", "").lower() == "true"

    if in_actions:
        if interval != "1d":
            raise ValueError("No Actions, apenas interval=1d (MVP).")
        return fetch_history_stooq_csv(ticker)

    try:
        return fetch_history_yfinance(ticker, period=period, interval=interval)
    except Exception:
        if interval != "1d":
            raise
        return fetch_history_stooq_csv(ticker)
