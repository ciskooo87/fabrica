"""
Routing logic for selecting the appropriate history provider.

This module defines ``route_fetch_history`` which inspects asset metadata and
chooses the best provider function for fetching historical price data. It
relies on provider implementations in ``core.providers``. Additional routing
rules can be added here as the system evolves.
"""
from __future__ import annotations

from typing import Optional
import pandas as pd

from core.providers import fetch_history_yahoo, fetch_history_brapi, fetch_history_fx


def route_fetch_history(
    ticker: str,
    asset_type: str,
    market: str,
    *,
    period: str = "10y",
    interval: str = "1d",
) -> pd.DataFrame:
    """Route a history request to the correct provider.

    Args:
        ticker: Raw ticker symbol.
        asset_type: Asset class, e.g. ``"ETF"``, ``"STOCK"``, ``"FX"``.
        market: Market code like ``"B3"`` or ``"OTC"``.
        period: Duration of historical data requested (default 10 years).
        interval: Sampling interval (default daily).

    Returns:
        DataFrame with OHLC data. May be empty if no provider yields data.
    """
    at = (asset_type or "").strip().upper()
    mk = (market or "").strip().upper()

    # ETFs traded on B3 → Yahoo Finance
    if at == "ETF" and mk == "B3":
        return fetch_history_yahoo(ticker, market=mk, period=period, interval=interval)

    # Stocks traded on B3 → BRAPI (delegate to existing core.data)
    if at in {"STOCK", "ACAO", "AÇÃO", "EQUITY"} and mk == "B3":
        return fetch_history_brapi(ticker, period=period, interval=interval)

    # Foreign exchange → dedicated FX provider
    if at == "FX":
        # interval and period don't map directly; convert period to days
        if period.lower() in {"max", "10y"}:
            days = 3650
        elif period.lower().endswith("y"):
            try:
                years = int(period[:-1])
                days = years * 365
            except ValueError:
                days = 3650
        else:
            # Default to 10 years
            days = 3650
        return fetch_history_fx(ticker, period_days=days)

    # Fallback: delegate to BRAPI for unknown asset types
    return fetch_history_brapi(ticker, period=period, interval=interval)