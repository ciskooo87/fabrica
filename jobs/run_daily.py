import os
import datetime as dt

from core.data import fetch_history, DATA_PROVIDER_VERSION
from core.storage import load_state, save_state, log_event
from core.strategy import trend_signal
from core.portfolio import build_weights, apply_kill_switch, build_orders

try:
    import tomllib  # Python 3.11+
except Exception:
    tomllib = None

CONFIG_FILE = "config.toml"

def load_config():
    if tomllib is None or not os.path.exists(CONFIG_FILE):
        return {}
    with open(CONFIG_FILE, "rb") as f:
        return tomllib.load(f)

def universe_from_config(cfg: dict):
    u = cfg.get("universe") or {}
    keys = ["risk_directional", "strong_currency", "rates_real", "real_asset"]
    tickers = [str(u[k]).strip().upper() for k in keys if u.get(k)]
    return tickers or ["BOVA11", "IVVB11", "IMAB11", "GOLD11"]

def state_identity(cfg: dict, tickers: list):
    trend = cfg.get("trend") or {}
    window = int(trend.get("window", 126))
    ref = str(trend.get("reference", "SMA")).upper()
    return f"UNIV={','.join(tickers)}|TREND={ref}:{window}|PROVIDER={DATA_PROVIDER_VERSION}"

def reset_state(cfg: dict, state_id: str):
    init_eq = float((cfg.get("system") or {}).get("initial_equity", 100000.0))
    return {
        "state_id": state_id,
        "equity": init_eq,
        "peak_equity": init_eq,
        "last_drawdown": 0.0,
        "kill_switch": False,
        "positions": {},
        "last_prices": {},
        "last_run": None,
    }

def run():
    cfg = load_config()
    tickers = universe_from_config(cfg)

    trend_cfg = cfg.get("trend") or {}
    window = int(trend_cfg.get("window", 126))
    reference = str(trend_cfg.get("reference", "SMA")).upper()

    kill_cfg = cfg.get("kill_switch") or {}
    max_dd = float(kill_cfg.get("max_drawdown", 0.20))
    kill_enabled = bool(kill_cfg.get("enabled", True))

    state = load_state()
    sid = state_identity(cfg, tickers)

    # Reseta automaticamente se universo ou regra mudar
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

    prices = {}
    signals = {}

    # 1) Dados + sinal
    for t in tickers:
        df = fetch_history(t, period="10y", interval="1d")
        if df is None or df.empty:
            raise ValueError(f"Sem dados para {t}")
        prices[t] = float(df["Close"].iloc[-1])
        sig = trend_signal(df["Close"], window=window, reference=reference)
        signals[t] = int(sig)

    # 2) Pesos (igual para ON)
    weights = build_weights(signals)

    # 3) Kill switch
    kill_switch = False
    if kill_enabled:
        kill_switch = apply_kill_switch(state=state, weights=weights, prices=prices, max_drawdown=max_dd)

    # 4) Ordens (mudanças de estado)
    orders = build_orders(state.get("positions", {}), weights)

    # 5) Atualiza state
    now = dt.datetime.utcnow().isoformat() + "Z"
    state["kill_switch"] = bool(kill_switch)
    state["last_run"] = now
    state["last_prices"] = prices

    state["positions"] = {
        t: {"state": 1 if float(weights.get(t, 0)) > 0 else 0, "weight": float(weights.get(t, 0))}
        for t in tickers
    }

    # Peak equity / drawdown
    state["peak_equity"] = max(float(state.get("peak_equity", state["equity"])), float(state["equity"]))
    if state["peak_equity"] > 0:
        state["last_drawdown"] = float((state["peak_equity"] - state["equity"]) / state["peak_equity"])

    # 6) Log evento
    log_event({
        "type": "RUN",
        "ts": now,
        "provider": DATA_PROVIDER_VERSION,
        "state_id": sid,
        "kill_switch": bool(kill_switch),
        "signals": signals,
        "weights": weights,
        "prices": prices,
        "orders": orders,
    })

    save_state(state)

if __name__ == "__main__":
    run()
