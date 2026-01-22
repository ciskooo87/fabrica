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
        "last_run": None
    }


def _ticker_candidates(t: str):
    """
    Gera candidatos de ticker para maximizar chance do provider retornar dados.
    Regra prática: ETFs B3 geralmente precisam de .SA (Yahoo).
    """
    t = (t or "").strip().upper()
    if not t:
        return []

    cands = [t]

    # Se já veio com sufixo, tenta também sem (e vice-versa)
    if t.endswith(".SA"):
        cands.append(t.replace(".SA", ""))
    else:
        cands.append(f"{t}.SA")

    # Alguns providers funcionam com hífen/igual (raros, mas barato tentar)
    # Mantém aqui só como hedge.
    cands.append(t.replace("-", ""))
    cands.append(t.replace(".", ""))

    # Remove duplicados preservando ordem
    seen = set()
    out = []
    for x in cands:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out


def _fetch_history_with_fallback(t: str, period="10y", interval="1d"):
    """
    Tenta obter histórico com fallback de ticker.
    Retorna: (df, used_symbol) ou (None, None)
    """
    last_err = None
    for sym in _ticker_candidates(t):
        try:
            df = fetch_history(sym, period=period, interval=interval)
            if df is not None and not df.empty:
                return df, sym
        except Exception as e:
            last_err = e
            continue

    # Se quiser visibilidade do erro raiz no log, imprime aqui (sem quebrar pipeline)
    if last_err:
        print(f"[WARN] fetch_history falhou para {t} (último erro: {type(last_err).__name__}: {last_err})")
    return None, None


def run():
    cfg = load_config()
    tickers = universe_from_config(cfg)

    trend_cfg = cfg.get("trend") or {}
    window = int(trend_cfg.get("window", 126))
    ref = str(trend_cfg.get("reference", "SMA")).upper()

    kill_cfg = cfg.get("kill_switch") or {}
    max_dd = float(kill_cfg.get("max_drawdown", 0.20))
    kill_enabled = bool(kill_cfg.get("enabled", True))

    state = load_state()
    sid = state_identity(cfg, tickers)

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

    now = dt.datetime.utcnow().isoformat() + "Z"

    signals = {}
    prices = {}
    used_symbols = {}
    skipped = []

    # Coleta dados com fallback
    for t in tickers:
        df, used = _fetch_history_with_fallback(t, period="10y", interval="1d")
        if df is None or df.empty:
            print(f"[WARN] Sem dados para {t}. Pulando ticker.")
            skipped.append(t)
            continue

        used_symbols[t] = used

        # Estratégia (trend)
        sig_df = signal_on_off(df, sma_window=window)
        last_row = sig_df.iloc[-1]
        signals[t] = int(last_row["Signal"])
        prices[t] = float(last_row["Close"])

    # Se nada veio, não derruba o pipeline: registra evento e mantém estado.
    if not signals:
        log_event({
            "type": "NO_DATA",
            "ts": now,
            "provider": DATA_PROVIDER_VERSION,
            "state_id": sid,
            "message": "Nenhum ticker retornou dados válidos. Mantendo estado anterior e encerrando execução.",
            "skipped": skipped,
            "universe": tickers
        })
        state["last_run"] = now
        save_state(state)
        return  # exit 0

    # Se alguns tickers vieram e outros não, recalcula universo efetivo
    effective_tickers = list(signals.keys())

    weights = compute_weights(signals)

    # Kill switch
    if kill_enabled:
        equity = float(state.get("equity", 100000.0))
        peak_eq = float(state.get("peak_equity", equity))
        kill, new_peak = update_kill_switch(equity, peak_eq, max_dd)
        state["peak_equity"] = float(new_peak)
        if kill:
            state["kill_switch"] = True
            weights = {t: 0.0 for t in effective_tickers}
            signals = {t: 0 for t in effective_tickers}

    orders = diff_states(state.get("positions", {}), weights)

    # Atualiza posições apenas do universo efetivo
    state["positions"] = {
        t: {"state": 1 if weights.get(t, 0) > 0 else 0, "weight": float(weights.get(t, 0))}
        for t in effective_tickers
    }
    state["last_prices"] = prices
    state["last_run"] = now

    equity = float(state.get("equity", 100000.0))
    peak_eq = float(state.get("peak_equity", equity))
    if peak_eq > 0:
        state["last_drawdown"] = float((peak_eq - equity) / peak_eq)

    log_event({
        "type": "RUN",
        "ts": now,
        "provider": DATA_PROVIDER_VERSION,
        "state_id": sid,
        "kill_switch": state.get("kill_switch", False),
        "trend": {"reference": ref, "window": window},
        "universe": tickers,
        "effective_universe": effective_tickers,
        "used_symbols": used_symbols,
        "skipped": skipped,
        "signals": signals,
        "weights": weights,
        "prices": prices,
        "orders": orders
    })

    save_state(state)


if __name__ == "__main__":
    run()
