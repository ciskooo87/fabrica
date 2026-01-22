"""
Provider implementations for fetching historical data.

This module defines separate functions for different data providers based on the
asset type and market. The router in ``core.router`` will choose the
appropriate provider for each asset when ``run_daily.py`` requests data.

Providers currently implemented:

* Yahoo Finance for Brazilian ETFs (B3) via yfinance
* BRAPI wrapper for equities (stocks) on B3 (delegates to existing core.data)
* A simple FX provider using exchangerate.host for currency pairs

Note: Additional providers can be added here and wired into ``core.router``.
"""
from __future__ import annotations

import datetime as dt
from typing import Tuple, Optional, Dict, Any

import pandas as pd


def _normalize_yahoo_symbol(ticker: str, market: str) -> str:
    """Normalize a B3 ticker for Yahoo Finance.

    Yahoo Finance uses the ``.SA`` suffix for Brazilian securities. If the
    provided symbol already ends with ``.SA`` we leave it untouched, otherwise
    we append the suffix. For other markets we leave the symbol unchanged.

    Args:
        ticker: Raw ticker symbol (e.g. ``BOVA11``).
        market: Market code (e.g. ``B3``).

    Returns:
        Symbol normalized for Yahoo Finance.
    """
    t = (ticker or "").strip().upper()
    if market.upper() == "B3" and not t.endswith(".SA"):
        return f"{t}.SA"
    return t


def fetch_history_yahoo(ticker: str, market: str, period: str = "10y", interval: str = "1d") -> pd.DataFrame:
    """Fetch historical OHLC data from Yahoo Finance.

    This uses the ``yfinance`` package to download data. If no data is
    available it returns an empty DataFrame. Columns are renamed to follow a
    consistent convention (``Datetime``, ``Open``, ``High``, ``Low``, ``Close``,
    ``Adj Close``, ``Volume``) and sorted in ascending order by date.

    Args:
        ticker: Raw ticker symbol (e.g. ``BOVA11``).
        market: Market code (e.g. ``B3``).
        period: Duration to fetch (e.g. ``"10y"``).
        interval: Sampling interval (e.g. ``"1d"``).

    Returns:
        DataFrame with price history. May be empty.
    """
    import yfinance as yf  # type: ignore  # Local import to avoid hard dependency when unused

    sym = _normalize_yahoo_symbol(ticker, market)
    df = yf.download(sym, period=period, interval=interval, auto_adjust=False, progress=False)

    if df is None or df.empty:
        return pd.DataFrame()

    df = df.reset_index()
    # Standardize column names
    if "Date" in df.columns:
        df.rename(columns={"Date": "Datetime"}, inplace=True)
    elif "Datetime" not in df.columns and "Date" not in df.columns:
        df.insert(0, "Datetime", pd.to_datetime(df.index))

    df.rename(
        columns={
            "Open": "Open",
            "High": "High",
            "Low": "Low",
            "Close": "Close",
            "Adj Close": "Adj Close",
            "Volume": "Volume",
        },
        inplace=True,
    )
    # Sort ascending by datetime
    df = df.sort_values("Datetime").reset_index(drop=True)
    # Keep only relevant columns if others exist
    cols = [c for c in ["Datetime", "Open", "High", "Low", "Close", "Adj Close", "Volume"] if c in df.columns]
    return df[cols]


def fetch_history_brapi(ticker: str, period: str = "10y", interval: str = "1d") -> pd.DataFrame:
    """Proxy to the existing BRAPI provider for equities.

    Delegates to ``core.data.fetch_history`` so that any existing behaviour
    remains unchanged. Returns an empty DataFrame if the provider returns
    ``None`` or empty data.

    Args:
        ticker: Ticker symbol for the security.
        period: Duration to fetch (default ``"10y"``).
        interval: Sampling interval (default ``"1d"``).

    Returns:
        DataFrame with OHLC data.
    """
    from core.data import fetch_history as brapi_fetch  # type: ignore

    df = brapi_fetch(ticker, period=period, interval=interval)
    if df is None:
        return pd.DataFrame()
    return df


def fetch_history_fx(ticker: str, period_days: int = 3650) -> pd.DataFrame:
    """Fetch FX time series from exchangerate.host.

    This provider is a lightweight example using the public exchangerate.host API.
    It produces daily closing rates for the specified currency pair. The OHLC
    values are all set equal to the closing rate; volume is zero. Real-world
    implementations should replace this with a more robust FX data source.

    Args:
        ticker: Currency pair like ``"USD/BRL"`` or ``"USDBRL"``.
        period_days: Number of days of history (default ten years ~ 3650 days).

    Returns:
        DataFrame with columns ``Datetime``, ``Open``, ``High``, ``Low``, ``Close``, ``Volume``.
    """
    import requests

    t = (ticker or "").upper().replace("-", "").replace("_", "")
    if "/" in t:
        base, quote = t.split("/")
    else:
        base, quote = t[:3], t[3:]

    end = dt.date.today()
    start = end - dt.timedelta(days=period_days)
    url = f"https://api.frankfurter.app/{start.isoformat()}..{end.isoformat()}?from={base}&to={quote}"
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        payload = r.json()
    except Exception:
        return pd.DataFrame()

    rates: Dict[str, Any] = payload.get("rates", {})  # type: ignore
    if not rates:
        return pd.DataFrame()

    rows = []
    for date_str, obj in rates.items():
        px = obj.get(quote)
        if px is None:
            continue
        rows.append({"Datetime": pd.to_datetime(date_str), "Close": float(px)})

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values("Datetime").reset_index(drop=True)
    # Duplicate Close into OHLC fields and zero volume
    df["Open"] = df["Close"]
    df["High"] = df["Close"]
    df["Low"] = df["Close"]
    df["Volume"] = 0.0
    return df[["Datetime", "Open", "High", "Low", "Close", "Volume"]]