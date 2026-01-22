import pandas as pd

def sma(series: pd.Series, window: int) -> pd.Series:
    return series.rolling(window=window, min_periods=window).mean()

def signal_on_off(df: pd.DataFrame, sma_window: int = 200) -> pd.DataFrame:
    """
    Regra binária:
      Close > SMA -> ON (1)
      Close <= SMA -> OFF (0)
    Avaliado no fechamento do período.
    """
    out = df.copy()
    out["SMA"] = sma(out["Close"], sma_window)
    out = out.dropna()
    out["Signal"] = (out["Close"] > out["SMA"]).astype(int)
    return out
