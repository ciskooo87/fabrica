import os
import datetime as dt

from core.data import fetch_history, DATA_PROVIDER_VERSION
from core.storage import load_state, save_state, log_event
from core.strategy import signal_on_off
from core.portfolio import compute_weights, update_kill_switch, diff_states

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


def universe_from_config(cfg):
    u = cfg.get("universe") or {}
    tickers = []
    for key in ["risk_directional", "strong_currency", "rates_real", "real_asset"]:
        val = u.get(key)
        if val:
            tickers.append(str(val).strip().upper())
    # fallback
    if not tickers:
        tickers = ["BOVA11", "IVVB11", "IMAB11", "GOLD11"]
    return tickers


def state_identity(cfg, tickers):
    trend_cfg = cfg.get("trend") or {}
    window = int(trend_cfg.get("window", 126))
    ref = str(trend_cfg.get("reference", "SMA")).upper()
    return f"UNIV={','.join(tickers)}|TREND={ref}:{window}|PROVIDER={DATA_PROVIDER_VERSION}"


def reset_state(cfg, sid):
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


def run():
    cfg = load_config()
    tickers = universe_from_config(cfg)

    trend_cfg = cfg.get("trend") or {}
    window = int(trend_cfg.get("window", 126))
    ref = str(trend_cfg.get("reference", "SMA")).upper()  # reservado p/ futuro (hoje só usamos SMA)

    kill_cfg = cfg.get("kill_switch") or {}
    max_dd = float(kill_cfg.get("max_drawdown", 0.20))
    kill_enabled = bool(kill_cfg.get("enabled", True))

    state = load_state()
    sid = state_identity(cfg, tickers)

    # Estado compatível com o "shape" atual
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

    signals = {}
    prices = {}
    skipped = []

    # Busca + sinal por ticker (robusto)
    for t in tickers:
        df = fetch_history(t, period="10y", interval="1d")
        if df is None or df.empty:
            print(f"[WARN] Sem dados para {t}. Pulando ticker.")
            skipped.append(t)
            continue

        sig_df = signal_on_off(df, sma_window=window)

        if sig_df is None or sig_df.empty:
            print(f"[WARN] signal_on_off retornou vazio para {t}. Pulando ticker.")
            skipped.append(t)
            continue

        # valida colunas mínimas
        if "Signal" not in sig_df.columns or "Close" not in sig_df.columns:
            print(f"[WARN] Colunas esperadas ausentes para {t} (precisa de Signal e Close). Pulando ticker.")
            skipped.append(t)
            continue

        last_row = sig_df.iloc[-1]

        # valida NaN / None
        try:
            sig_val = last_row["Signal"]
            close_val = last_row["Close"]
            if sig_val is None or close_val is None:
                raise ValueError("Signal/Close None")
            signals[t] = int(sig_val)
            prices[t] = float(close_val)
        except Exception:
            print(f"[WARN] Última linha inválida para {t}. Pulando ticker.")
            skipped.append(t)
            continue

    valid_tickers = list(signals.keys())

    # Se nada veio, aí sim falha (não existe operação sem insumo)
    if not valid_tickers:
        raise RuntimeError(f"Nenhum ticker retornou dados válidos. Pulados: {skipped}")

    weights = compute_weights(signals)

    # Kill switch (se acionou, zera apenas o universo válido)
    if kill_enabled:
        equity = float(state.get("equity", 100000.0))
        peak_eq = float(state.get("peak_equity", equity))
        kill, new_peak = update_kill_switch(equity, peak_eq, max_dd)
        state["peak_equity"] = float(new_peak)

        if kill:
            state["kill_switch"] = True
            weights = {t: 0.0 for t in valid_tickers}
            signals = {t: 0 for t in valid_tickers}

    # Diferença de posições só para tickers válidos
    current_positions = state.get("positions", {})
    current_positions_valid = {t: current_positions.get(t) for t in valid_tickers if t in current_positions}
    orders = diff_states(current_positions_valid, weights)

    # Persistência do estado só para tickers válidos (sem fantasma)
    state["positions"] = {
        t: {"state": 1 if weights.get(t, 0.0) > 0 else 0, "weight": float(weights.get(t, 0.0))}
        for t in valid_tickers
    }
    state["last_prices"] = prices

    now = dt.datetime.utcnow().isoformat() + "Z"
    state["last_run"] = now

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
            "universe": tickers,
            "valid_universe": valid_tickers,
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
