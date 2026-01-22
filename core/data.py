# DATA_PROVIDER_VERSION = "STOOQ_CSV_IN_ACTIONS_v2"

import os
import time
import io
import pandas as pd
import requests


def _to_stooq_symbol(ticker: str) -> str:
    """
    Stooq para ativos US costuma usar 'spy.us'
    """
    t = ticker.strip()
    if "." in t:
        return t.lower()
    return f"{t}.us".lower()


def fetch_history_stooq_csv(ticker: str, retries: int = 4, timeout: int = 20) -> pd.DataFrame:
    """
    Fonte estável pra CI: baixa CSV direto do Stooq.
    Ex: https://stooq.com/q/d/l/?s=spy.us&i=d
    """
    symbol = _to_stooq_symbol(ticker)
    url = f"https://stooq.com/q/d/l/?s={symbol}&i=d"

    last_err = None
    for attempt in range(retries):
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            text = r.text.strip()
            if not text or "404" in text.lower():
                raise ValueError(f"Resposta vazia/ruim do Stooq para {ticker} ({symbol})")

            df = pd.read_csv(io.StringIO(text))
            # Esperado: Date,Open,High,Low,Close,Volume
            if df.empty or "Close" not in df.columns:
                raise ValueError(f"CSV inválido do Stooq para {ticker} ({symbol})")

            df["Date"] = pd.to_datetime(df["Date"])
            df = df.set_index("Date").sort_index()

            # Padroniza para OHLCV
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
    """
    Uso local (fora do Actions). No Actions a gente NÃO usa Yahoo.
    """
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
    Política definitiva:
    - GitHub Actions => Stooq CSV (100% sem Yahoo)
    - Local => tenta Yahoo, fallback Stooq CSV
    """
    in_actions = os.getenv("GITHUB_ACTIONS", "").lower() == "true"

    if in_actions:
        if interval != "1d":
            raise ValueError("No Actions, suportamos apenas interval=1d (MVP).")
        return fetch_history_stooq_csv(ticker)

    # local/dev: Yahoo primeiro, fallback stooq
    try:
        return fetch_history_yfinance(ticker, period=period, interval=interval)
    except Exception:
        if interval != "1d":
            raise
        return fetch_history_stooq_csv(ticker)
