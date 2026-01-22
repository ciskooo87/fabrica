import time
import pandas as pd
import yfinance as yf
import requests

def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.rename(columns=str.title)

    # yfinance às vezes traz MultiIndex colunas (quando baixa vários ativos)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] for c in df.columns]

    needed = ["Open", "High", "Low", "Close", "Volume"]
    if not all(c in df.columns for c in needed):
        # tenta auto_adjust/formatos diferentes
        cols = set(df.columns)
        if "Adj Close" in cols and "Close" not in cols:
            df["Close"] = df["Adj Close"]
        if not all(c in df.columns for c in needed):
            return pd.DataFrame()

    df = df[needed].dropna()
    return df


def fetch_history_yfinance(ticker: str, period: str = "10y", interval: str = "1d", retries: int = 4) -> pd.DataFrame:
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
            df = _normalize_ohlcv(df)
            if not df.empty:
                return df
        except Exception as e:
            last_err = e

        # backoff simples
        time.sleep(1.5 * (attempt + 1))

    # Falhou
    raise ValueError(f"yfinance sem dados para {ticker}. Erro: {repr(last_err)}")


def fetch_history_stooq(ticker: str, years: int = 10) -> pd.DataFrame:
    """
    Fallback confiável para muitos tickers de ETFs/ações US via Stooq.
    Usa pandas-datareader, que acessa Stooq.
    """
    try:
        from pandas_datareader import data as pdr
    except Exception as e:
        raise ValueError(f"pandas-datareader indisponível: {repr(e)}")

    # Stooq geralmente usa sufixos; para ETFs/ações US costuma aceitar "spy", "tlt" etc.
    symbol = ticker.lower()

    end = pd.Timestamp.utcnow().normalize()
    start = end - pd.DateOffset(years=years)

    df = pdr.DataReader(symbol, "stooq", start, end)

    # Stooq vem em ordem desc, vamos ordenar asc
    df = df.sort_index()

    # Padroniza para OHLCV
    df = df.rename(columns=str.title)
    # Algumas vezes vem "Close" e "Volume", e OHLC completos
    needed = ["Open", "High", "Low", "Close"]
    if not all(c in df.columns for c in needed):
        raise ValueError(f"Stooq sem colunas OHLC para {ticker}")

    if "Volume" not in df.columns:
        df["Volume"] = 0

    df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
    return df


def fetch_history(ticker: str, period: str = "10y", interval: str = "1d") -> pd.DataFrame:
    """
    Fonte primária: yfinance (rápida)
    Fallback: Stooq (mais estável em runners)
    """
    # 1) tenta yfinance
    try:
        return fetch_history_yfinance(ticker, period=period, interval=interval)
    except Exception:
        pass

    # 2) fallback stooq (intervalo diário)
    if interval != "1d":
        # fallback só para diário (MVP)
        raise ValueError(f"Fallback Stooq só suportado para interval=1d. Ticker={ticker}")

    return fetch_history_stooq(ticker, years=10)
