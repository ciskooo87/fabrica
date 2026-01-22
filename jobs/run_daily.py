"""
Job for running the daily strategy refresh.

This script loads configuration and state, fetches historical data for each
asset via the router (see ``core.router``), computes trend-following signals,
weights, applies a kill switch, logs an event, and updates state. It is
designed to be resilient to missing data by skipping tickers that return
empty histories and logging a ``NO_DATA`` event when no ticker yields data.
"""

import os
import datetime as dt
from typing import Dict, List

from core.data import DATA_PROVIDER_VERSION
from core.storage import load_state, save_state, log_event
from core.strategy import signal_on_off
from core.portfolio import compute_weights, update_kill_switch, diff_states
from core.router import route_fetch_history

try:
    import tomllib  # Python 3.11+
except Exception:
    tomllib = None

CONFIG_FILE = "config.toml"


def load_config() -> Dict:
    """Load configuration from ``config.toml`` if it exists.

    Returns:
        Configuration dictionary (empty if file missing or tomllib unavailable).
    """
    if tomllib is None or not os.path.exists(CONFIG_FILE):
        return {}
    with open(CONFIG_FILE, "rb") as f:
        return tomllib.load(f)


def universe_from_config(cfg: Dict) -> List[Dict[str, str]]:
    """Extract universe assets from config.

    Supports the legacy simple list of tickers (as strings) and a new
    structured format where each asset is a dict containing ``ticker``,
    ``type``, and ``market``. If the structured format is detected under
    ``universe.assets`` it is returned directly; otherwise the legacy format
    is converted assuming all instruments are ETFs on B3.

    Args:
        cfg: Parsed configuration dictionary.

    Returns:
        List of asset dictionaries with keys ``ticker``, ``type``, ``market``.
    """
    u = cfg.get("universe") or {}
    assets_cfg = u.get("assets")
    if isinstance(assets_cfg, list) and assets_cfg:
        normalized: List[Dict[str, str]] = []
        for a in assets_cfg:
            if not isinstance(a, dict):
                continue
            ticker = str(a.get("ticker", "")).strip().upper()
            if not ticker:
                continue
            asset_type = str(a.get("type", "ETF")).strip().upper()
            market = str(a.get("market", "B3")).strip().upper()
            normalized.append({"ticker": ticker, "type": asset_type, "market": market})
        if normalized:
            return normalized

    # Legacy behaviour: treat values in universe keys as tickers for ETFs on B3
    tickers: List[str] = []
    for key in ["risk_directional", "strong_currency", "rates_real", "real_asset"]:
        val = u.get(key)
        if val:
            tickers.append(str(val).strip().upper())
    if not tickers:
        tickers = ["BOVA11", "IVVB11", "IMAB11", "GOLD11"]
    return [{"ticker": t, "type": "ETF", "market": "B3"} for t in tickers]


def state_identity(cfg: Dict, tickers: List[str]) -> str:
    """Generate a state identity string based on configuration and universe.

    Args:
        cfg: Configuration dictionary.
        tickers: List of ticker symbols.

    Returns:
        A string uniquely identifying the state shape.
    """
    trend_cfg = cfg.get("trend") or {}
    window = int(trend_cfg.get("window", 126))
    ref = str(trend_cfg.get("reference", "SMA")).upper()
    return f"UNIV={','.join(tickers)}|TREND={ref}:{window}|PROVIDER={DATA_PROVIDER_VERSION}"


def reset_state(cfg: Dict, sid: str) -> Dict:
    """Initialize a new state dictionary when state shape changes.

    Args:
        cfg: Configuration dictionary.
        sid: New state identifier.

    Returns:
        State dictionary with default values.
    """
    init_eq = float((cfg.get("system") or {}).get("initial_equity", 100000.0))
    return {
        "state_id": sid,
        "equity": init_eq,
        "peak_equity": init_eq,
        "last_drawdown": 0.0,
        "kill_switch": False,
        "positions": {},
        "last_prices": {},
        "last_run": None,
    }


def run() -> None:
    """Main entrypoint for the daily job.

    Loads configuration and state, fetches data via the router, computes
    signals and weights, applies kill switch logic, logs events, and saves
    state. Handles missing data gracefully by skipping tickers and logging
    a ``NO_DATA`` event when no valid histories are found.
    """
    cfg = load_config()
    assets = universe_from_config(cfg)
    trend_cfg = cfg.get("trend") or {}
    window = int(trend_cfg.get("window", 126))
    ref = str(trend_cfg.get("reference", "SMA")).upper()

    kill_cfg = cfg.get("kill_switch") or {}
    max_dd = float(kill_cfg.get("max_drawdown", 0.20))
    kill_enabled = bool(kill_cfg.get("enabled", True))

    state = load_state()
    sid = state_identity(cfg, [a["ticker"] for a in assets])

    # Ensure state has expected shape and id
    if state.get("state_id") and state.get("state_id") != sid:
        state = reset_state(cfg, sid)
    else:
        state.setdefault("state_id", sid)
        state.setdefault("equity", float((cfg.get("system") or {}).get("initial_equity", 100000.0)))
        state.setdefault("peak_equity", state["equity"])
        state.setdefault("last_drawdown", 0.0)
        state.setdefault("kill_switch", False)
        state.setdefault("positions", {})
        state.setdefault("last_prices", {})
        state.setdefault("last_run", None)

    signals: Dict[str, int] = {}
    prices: Dict[str, float] = {}
    skipped: List[str] = []

    # Fetch historical data for each asset via the router
    for asset in assets:
        t = asset["ticker"]
        atype = asset.get("type", "ETF")
        market = asset.get("market", "B3")
        df = route_fetch_history(t, asset_type=atype, market=market, period="10y", interval="1d")
        if df is None or df.empty:
            print(f"[WARN] Sem dados para {t} ({atype}/{market}). Pulando.")
            skipped.append(t)
            continue
        sig_df = signal_on_off(df, sma_window=window)
        if sig_df is None or sig_df.empty:
            print(f"[WARN] signal_on_off vazio para {t}. Pulando.")
            skipped.append(t)
            continue
        last_row = sig_df.iloc[-1]
        signals[t] = int(last_row["Signal"])
        prices[t] = float(last_row["Close"])

    now = dt.datetime.utcnow().isoformat() + "Z"

    # If no valid signals, log and exit gracefully
    if not signals:
        log_event(
            {
                "type": "NO_DATA",
                "ts": now,
                "provider": DATA_PROVIDER_VERSION,
                "state_id": sid,
                "message": "Nenhum ticker retornou dados válidos.",
                "skipped": skipped,
                "universe": [a["ticker"] for a in assets],
            }
        )
        state["last_run"] = now
        save_state(state)
        return

    # Compute weights from signals
    weights = compute_weights(signals)

    # Apply kill switch if enabled
    if kill_enabled:
        equity = float(state.get("equity", 100000.0))
        peak_eq = float(state.get("peak_equity", equity))
        kill, new_peak = update_kill_switch(equity, peak_eq, max_dd)
        state["peak_equity"] = float(new_peak)
        if kill:
            state["kill_switch"] = True
            # Zero out weights and signals for the effective universe
            weights = {t: 0.0 for t in signals.keys()}
            signals = {t: 0 for t in signals.keys()}

    # Compute orders based on difference between current and target positions
    orders = diff_states(state.get("positions", {}), weights)

    # Persist positions and last prices for the effective universe
    state["positions"] = {
        t: {
            "state": 1 if weights.get(t, 0.0) > 0 else 0,
            "weight": float(weights.get(t, 0.0)),
        }
        for t in signals.keys()
    }
    state["last_prices"] = prices
    state["last_run"] = now

    # Update drawdown
    equity = float(state.get("equity", 100000.0))
    peak_eq = float(state.get("peak_equity", equity))
    if peak_eq > 0:
        state["last_drawdown"] = float((peak_eq - equity) / peak_eq)

    log_event(
        {
            "type": "RUN",
            "ts": now,
            "provider": DATA_PROVIDER_VERSION,
            "state_id": sid,
            "kill_switch": state.get("kill_switch", False),
            "trend": {"reference": ref, "window": window},
            "universe": [a["ticker"] for a in assets],
            "effective_universe": list(signals.keys()),
            "skipped": skipped,
            "signals": signals,
            "weights": weights,
            "prices": prices,
            "orders": orders,
        }
    )

    save_state(state)


if __name__ == "__main__":
    run()