# DATA_PROVIDER_VERSION = "STOOQ_CSV_IN_ACTIONS_v3"

import pandas as pd
import urllib.request
import datetime as dt


DATA_PROVIDER_VERSION = "STOOQ_CSV_IN_ACTIONS_v3"


def _to_stooq_symbol(ticker: str) -> str:
    """
    Converte ticker canônico (SPY, TLT etc) para símbolo Stooq:
    - ETFs USA: spy.us
    """
    t = ticker.strip().upper()
    if "." in t:
        # se alguém passar já com sufixo, normaliza
        return t.lower()
    return f"{t.lower()}.us"


def fetch_history(ticker: str, period="10y", interval="1d") -> pd.DataFrame:
    """
    Stooq CSV:
    https://stooq.com/q/d/l/?s=spy.us&i=d

    period/interval ficam "decorativos" aqui (mantemos assinatura).
    """
    sym = _to_stooq_symbol(ticker)
    url = f"https://stooq.com/q/d/l/?s={sym}&i=d"

    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            raw = resp.read().decode("utf-8", errors="ignore")
    except Exception:
        return pd.DataFrame()

    if not raw or "Date" not in raw:
        return pd.DataFrame()

    from io import StringIO
    df = pd.read_csv(StringIO(raw))

    # Normalização
    # Stooq retorna: Date,Open,High,Low,Close,Volume
    if "Date" not in df.columns:
        return pd.DataFrame()

    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).set_index("Date").sort_index()

    # Padroniza colunas em Title Case
    df = df.rename(columns={c: c.title() for c in df.columns})

    needed = ["Open", "High", "Low", "Close"]
    if not all(c in df.columns for c in needed):
        return pd.DataFrame()

    if "Volume" not in df.columns:
        df["Volume"] = 0

    # Remove linhas inválidas
    df = df[["Open", "High", "Low", "Close", "Volume"]].dropna()

    return df
