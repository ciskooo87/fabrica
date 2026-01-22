# core/strategy.py
from __future__ import annotations

import pandas as pd


def _ensure_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Garante que o DataFrame tenha DatetimeIndex válido.
    Prioridade:
      1) DatetimeIndex já existente
      2) Coluna 'Datetime'
      3) Coluna 'Date'
    """
    out = df.copy()

    # 1) Já é DatetimeIndex
    if isinstance(out.index, pd.DatetimeIndex):
        out = out[~out.index.isna()]
        return out.sort_index()

    # 2) Coluna Datetime
    if "Datetime" in out.columns:
        dt = pd.to_datetime(out["Datetime"], errors="coerce")
        mask = ~dt.isna()
        out = out.loc[mask].copy()
        out.index = dt.loc[mask]
        out = out.drop(columns=["Datetime"], errors="ignore")
        return out.sort_index()

    # 3) Coluna Date
    if "Date" in out.columns:
        dt = pd.to_datetime(out["Date"], errors="coerce")
        mask = ~dt.isna()
        out = out.loc[mask].copy()
        out.index = dt.loc[mask]
        out = out.drop(columns=["Date"], errors="ignore")
        return out.sort_index()

    # 4) Último recurso: tentar converter índice
    out.index = pd.to_datetime(out.index, errors="coerce")
    out = out[~out.index.isna()]
    return out.sort_index()


def _extract_close_series(df: pd.DataFrame) -> pd.Series:
    """
    Extrai Close como Series única e numérica.
    Lida com:
      - 'Close' duplicado (DataFrame)
      - 'close' minúsculo
    """
    if "Close" in df.columns:
        close = df["Close"]
    elif "close" in df.columns:
        close = df["close"]
    else:
        raise ValueError("DataFrame sem coluna Close/close")

    # Se vier como DataFrame (colunas duplicadas), pega a primeira
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]

    close = pd.to_numeric(close, errors="coerce")
    return close


def signal_on_off(df: pd.DataFrame, sma_window: int = 126) -> pd.DataFrame:
    """
    Trend filter On/Off via SMA.

    Retorna DataFrame com:
      - Close (Series)
      - SMA
      - Signal (0/1)

    Robusto para múltiplos providers e formatos.
    """
    if df is None or len(df) == 0:
        return pd.DataFrame()

    # 1) Índice temporal sólido
    out = _ensure_datetime_index(df)

    if out.empty:
        return pd.DataFrame()

    # 2) Close como Series única
    close = _extract_close_series(out)

    # 3) SMA alinhada
    w = int(sma_window)
    sma = close.rolling(window=w, min_periods=w).mean()

    # 4) Montagem final
    result = out.copy()
    result["Close"] = close
    result["SMA"] = sma

    # Series vs Series => zero erro de alignment
    signal = (close > sma).astype("Int64").fillna(0).astype(int)
    result["Signal"] = signal

    return result
