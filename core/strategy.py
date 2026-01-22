# core/strategy.py
from __future__ import annotations

import pandas as pd


def signal_on_off(df: pd.DataFrame, sma_window: int = 126) -> pd.DataFrame:
    """
    Trend filter simples (On/Off) usando SMA.

    Saída contém:
      - Close: série única e numérica
      - SMA: média móvel simples do Close
      - Signal: 1 quando Close > SMA, senão 0

    Robustez:
      - Aceita Datetime/Date como coluna ou índice
      - Lida com 'Close' duplicado (quando df["Close"] vira DataFrame)
      - Aceita 'close' minúsculo
      - Converte Close para numérico e ignora NaNs corretamente
    """
    if df is None or len(df) == 0:
        return pd.DataFrame()

    out = df.copy()

    # 1) Garantir índice temporal
    if "Datetime" in out.columns:
        out["Datetime"] = pd.to_datetime(out["Datetime"], errors="coerce")
        out = out.dropna(subset=["Datetime"]).set_index("Datetime")
    elif "Date" in out.columns:
        out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
        out = out.dropna(subset=["Date"]).set_index("Date")
    else:
        # Se já for índice temporal, mantém; senão tenta converter
        if not isinstance(out.index, pd.DatetimeIndex):
            out.index = pd.to_datetime(out.index, errors="coerce")
            out = out[~out.index.isna()]

    out = out.sort_index()

    # 2) Extrair Close como Series única
    close = None
    if "Close" in out.columns:
        close = out["Close"]
    elif "close" in out.columns:
        close = out["close"]

    if close is None:
        raise ValueError("DataFrame sem coluna Close/close")

    # Se Close vier como DataFrame (colunas duplicadas), pega a 1ª coluna
    if isinstance(close, pd.DataFrame):
        close = close.iloc[:, 0]

    # Converte para numérico
    close = pd.to_numeric(close, errors="coerce")

    # 3) SMA alinhada no mesmo índice
    w = int(sma_window)
    sma = close.rolling(window=w, min_periods=w).mean()

    # 4) Monta colunas padronizadas
    out["Close"] = close
    out["SMA"] = sma

    # 5) Sinal (Series vs Series => sem erro de alignment)
    sig = (close > sma).astype("Int64").fillna(0).astype(int)
    out["Signal"] = sig

    return out
