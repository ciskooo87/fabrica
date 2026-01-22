import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from datetime import datetime
from core.storage import load_state, save_state, log_event
from core.data import fetch_history
from core.strategy import signal_on_off
from core.portfolio import compute_weights, update_kill_switch, diff_states

UNIVERSE = {
    "Risco direcional": "SPY",
    "Proteção": "TLT",
    "Real asset": "DBC",
    "Taxas": "IEF",
    "Moeda forte": "UUP",
}

SMA_WINDOW = 200
MAX_DD = 0.20          # 20% drawdown => kill switch
MIN_VALID_ASSETS = 3   # governança: mínimo de ativos com dados válidos


def _extract_prev_weights(state: dict) -> dict:
    """
    Pega pesos do estado anterior.
    Estrutura esperada: state["positions"][ticker]["weight"]
    """
    prev_positions = state.get("positions", {}) or {}
    prev_w = {}
    for t, info in prev_positions.items():
        try:
            prev_w[t] = float(info.get("weight", 0.0))
        except Exception:
            prev_w[t] = 0.0
    return prev_w


def _mark_to_market_equity(state: dict, todays_prices: dict) -> dict:
    """
    Atualiza equity usando retorno close->close baseado nos pesos do estado anterior.
    - Usa state["last_prices"] como preços de ontem
    - Usa state["positions"] anterior como pesos de ontem
    """
    equity = float(state.get("equity", 100000.0))
    peak_equity = float(state.get("peak_equity", equity))

    prev_prices = state.get("last_prices", {}) or {}
    prev_weights = _extract_prev_weights(state)

    # Primeiro run (ou sem base): não calcula retorno
    if not prev_prices or not prev_weights:
        state["last_portfolio_return"] = 0.0
        state["last_m2m_note"] = "NO_PREV_PRICES_OR_WEIGHTS"
        return state

    # Calcula retorno ponderado só onde existe preço ontem e hoje
    port_ret = 0.0
    used = {}
    missing = {"no_prev_price": [], "no_today_price": []}

    for t, w in prev_weights.items():
        if w <= 0:
            continue

        if t not in prev_prices:
            missing["no_prev_price"].append(t)
            continue
        if t not in todays_prices:
            missing["no_today_price"].append(t)
            continue

        p0 = float(prev_prices[t])
        p1 = float(todays_prices[t])

        if p0 <= 0:
            continue

        r = (p1 / p0) - 1.0
        contrib = w * r
        port_ret += contrib
        used[t] = {"w": w, "p0": p0, "p1": p1, "r": r, "contrib": contrib}

    # Cash: resto do peso não investido rende 0, então não entra
    equity_new = equity * (1.0 + port_ret)

    state["equity"] = float(equity_new)
    state["last_portfolio_return"] = float(port_ret)
    state["last_m2m_details"] = {
        "used": used,
        "missing": missing,
        "prev_equity": equity,
        "new_equity": equity_new,
    }

    # Peak/Drawdown atualizados aqui (antes do kill switch)
    peak_equity = max(peak_equity, equity_new)
    dd = (peak_equity - equity_new) / peak_equity if peak_equity > 0 else 0.0

    state["peak_equity"] = float(peak_equity)
    state["last_drawdown"] = float(dd)

    return state


def run():
    os.makedirs(os.path.join(ROOT_DIR, "state"), exist_ok=True)

    state = load_state()

    # Se kill switch está ativo, nada roda. Governança máxima.
    if state.get("kill_switch", False):
        log_event({"type": "RUN_SKIPPED", "reason": "KILL_SWITCH_ACTIVE"})
        return

    # 1) Busca preço e sinal do dia (robusto)
    signals = {}
    todays_prices = {}
    data_errors = {}

    for _, ticker in UNIVERSE.items():
        try:
            df = fetch_history(ticker, period="10y", interval="1d")
            sigdf = signal_on_off(df, sma_window=SMA_WINDOW)

            if sigdf.empty:
                raise RuntimeError(f"Sem dados suficientes para SMA{SMA_WINDOW} em {ticker}")

            last = sigdf.iloc[-1]
            signals[ticker] = int(last["Signal"])
            todays_prices[ticker] = float(last["Close"])
        except Exception as e:
            data_errors[ticker] = repr(e)

    if len(signals) < MIN_VALID_ASSETS:
        log_event({
            "type": "RUN_ERROR",
            "error": "INSUFFICIENT_VALID_ASSETS",
            "details": {"valid_assets": list(signals.keys()), "errors": data_errors},
        })
        raise RuntimeError(f"Poucos ativos válidos ({len(signals)}) — abortando. Erros: {data_errors}")

    # 2) Mark-to-market do equity com base no estado anterior
    state = _mark_to_market_equity(state, todays_prices)

    # 3) Kill switch (agora com equity real)
    equity = float(state.get("equity", 100000.0))
    peak_equity = float(state.get("peak_equity", equity))
    kill, peak, dd = update_kill_switch(equity, peak_equity, MAX_DD)

    state["peak_equity"] = float(peak)
    state["last_drawdown"] = float(dd)

    # 4) Decide pesos para o próximo estado (equal weight entre ON)
    weights = compute_weights(signals)

    # 5) Gera ordens (mudança de estado)
    orders = diff_states(state.get("positions", {}), weights)

    # 6) Se kill acionou: zera tudo e força saída
    if kill:
        state["kill_switch"] = True
        weights = {t: 0.0 for t in weights.keys()}
        orders = [{"ticker": t, "action": "FORCE_EXIT"} for t in weights.keys()]

    # 7) Persistência do novo estado
    new_positions = {t: {"state": 1 if w > 0 else 0, "weight": float(w)} for t, w in weights.items()}

    state["positions"] = new_positions
    state["last_prices"] = todays_prices  # <- chave pra M2M do próximo run
    state["last_run"] = datetime.utcnow().isoformat() + "Z"

    log_event({
        "type": "RUN",
        "signals": signals,
        "weights": weights,
        "orders": orders,
        "prices": todays_prices,
        "data_errors": data_errors,
        "equity": state.get("equity"),
        "peak_equity": state.get("peak_equity"),
        "drawdown": state.get("last_drawdown"),
        "portfolio_return": state.get("last_portfolio_return"),
        "kill_switch": state.get("kill_switch", False),
        "params": {
            "SMA_WINDOW": SMA_WINDOW,
            "MAX_DD": MAX_DD,
            "MIN_VALID_ASSETS": MIN_VALID_ASSETS,
            "UNIVERSE": UNIVERSE,
        },
    })

    save_state(state)


if __name__ == "__main__":
    run()
