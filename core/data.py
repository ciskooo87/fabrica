# DATA_PROVIDER_VERSION = "BRAPI_QUOTE_HISTORY_v1"

import os
import time
import requests
import pandas as pd


DATA_PROVIDER_VERSION = "BRAPI_QUOTE_HISTORY_v1"
BRAPI_BASE_URL = os.getenv("BRAPI_BASE_URL", "https://brapi.dev/api")
BRAPI_TOKEN = os.getenv("BRAPI_TOKEN", "").strip()


def _headers():
    # docs recomendam Authorization: Bearer <token>
    # (se não tiver token, tenta mesmo assim; alguns ativos exigem token)
    h = {"User-Agent": "trend-system/1.0"}
    if BRAPI_TOKEN:
        h["Authorization"] = f"Bearer {BRAPI_TOKEN}"
    return h


def _parse_hist(hlist):
    """
    historicalDataPrice costuma vir como lista de candles com campos tipo:
    date (epoch), open, high, low, close, volume
    """
    if not isinstance(hlist, list) or not hlist:
        return pd.DataFrame()

    rows = []
    for x in hlist:
        if not isinstance(x, dict):
            continue

        d = x.get("date") or x.get("datetime") or x.get("timestamp")
        o = x.get("open")
        h = x.get("high")
        l = x.get("low")
        c = x.get("close")
        v = x.get("volume", 0)

        if d is None or c is None:
            continue

        # date pode vir em epoch (segundos) ou ms
        try:
            d = int(d)
            if d > 10_000_000_000:  # ms
                d = d / 1000
            dt_index = pd.to_datetime(d, unit="s", utc=True).tz_convert(None)
        except Exception:
            # fallback: tenta parse como string
            dt_index = pd.to_datetime(d, errors="coerce")
            if pd.isna(dt_index):
                continue

        rows.append({
            "Date": dt_index,
            "Open": float(o) if o is not None else None,
            "High": float(h) if h is not None else None,
            "Low": float(l) if l is not None else None,
            "Close": float(c) if c is not None else None,
            "Volume": float(v) if v is not None else 0.0,
        })

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).dropna(subset=["Date", "Close"])
    df = df.set_index("Date").sort_index()

    # garante colunas
    for col in ["Open", "High", "Low", "Close", "Volume"]:
        if col not in df.columns:
            df[col] = None

    df = df[["Open", "High", "Low", "Close", "Volume"]].dropna(subset=["Close"])
    return df


def fetch_history(ticker: str, period="10y", interval="1d") -> pd.DataFrame:
    """
    BRAPI:
      GET /api/quote/{tickers}?range=10y&interval=1d

    range suportados incluem 10y/max e interval 1d etc. :contentReference[oaicite:2]{index=2}
    """
    t = ticker.strip().upper()
    if not t:
        return pd.DataFrame()

    rng = os.getenv("BRAPI_RANGE", "10y")
    itv = os.getenv("BRAPI_INTERVAL", "1d")

    # permite override pelo caller
    if period and isinstance(period, str):
        rng = period
    if interval and isinstance(interval, str):
        itv = interval

    url = f"{BRAPI_BASE_URL}/quote/{t}"
    params = {"range": rng, "interval": itv}

    # token por query param também é aceito, mas header é mais seguro
    if not BRAPI_TOKEN:
        # se quiser, dá pra passar token por query (menos recomendado)
        pass

    # retry simples (robustez em CI)
    last_err = None
    for _ in range(3):
        try:
            r = requests.get(url, headers=_headers(), params=params, timeout=30)
            if r.status_code == 429:
                time.sleep(2)
                continue
            r.raise_for_status()
            data = r.json()
            results = data.get("results") or []
            if not results:
                return pd.DataFrame()
            hist = results[0].get("historicalDataPrice") or []
            return _parse_hist(hist)
        except Exception as e:
            last_err = e
            time.sleep(1)

    # falhou de vez
    return pd.DataFrame()
