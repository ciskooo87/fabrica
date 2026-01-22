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
MAX_DD = 0.20

# ✅ Governança operacional: mínimo de ativos com sinal válido para rodar
MIN_VALID_ASSETS = 3


def run():
    os.makedirs(os.path.join(ROOT_DIR, "state"), exist_ok=True)
    state = load_state()

    if state.get("kill_switch", False):
        log_event({"type": "RUN_SKIPPED", "reason": "KILL_SWITCH_ACTIVE"})
        return

    signals = {}
    last_prices = {}
    errors = {}

    # Busca e sinal por ativo (robusto)
    for _, ticker in UNIVERSE.items():
        try:
            df = fetch_history(ticker, period="10y", interval="1d")
            sigdf = signal_on_off(df, sma_window=SMA_WINDOW)
            if sigdf.empty:
                raise RuntimeError(f"Sem dados suficientes para SMA{SMA_WINDOW} em {ticker}")

            last = sigdf.iloc[-1]
            signals[ticker] = int(last["Signal"])
            last_prices[ticker] = float(last["Close"])
        except Exception as e:
            errors[ticker] = repr(e)

    # Se muitos ativos falharam, aborta (proteção contra dados ruins)
    if len(signals) < MIN_VALID_ASSETS:
        log_event({
            "type": "RUN_ERROR",
            "error": "INSUFFICIENT_VALID_ASSETS",
            "details": {"valid_assets": list(signals.keys()), "errors": errors},
        })
        raise RuntimeError(f"Poucos ativos válidos ({len(signals)}) — abortando. Erros: {errors}")

    # Pesos equal weight somente nos ativos válidos (ON)
    weights = compute_weights(signals)

    equity = float(state.get("equity", 100000.0))
    peak_equity = float(state.get("peak_equity", equity))
    kill, peak, dd = update_kill_switch(equity, peak_equity, MAX_DD)

    # Ordens baseadas nas mudanças de estado
    orders = diff_states(state.get("positions", {}), weights)

    if kill:
        weights = {t: 0.0 for t in weights.keys()}
        orders = [{"ticker": t, "action": "FORCE_EXIT"} for t in weights.keys()]
        state["kill_switch"] = True

    new_positions = {t: {"state": 1 if w > 0 else 0, "weight": float(w)} for t, w in weights.items()}

    state["positions"] = new_positions
    state["peak_equity"] = float(peak)
    state["last_drawdown"] = float(dd)
    state["last_run"] = datetime.utcnow().isoformat() + "Z"

    log_event({
        "type": "RUN",
        "signals": signals,
        "weights": weights,
        "orders": orders,
        "prices": last_prices,
        "data_errors": errors,  # 👈 transparência total
        "kill_switch": state.get("kill_switch", False),
        "params": {"SMA_WINDOW": SMA_WINDOW, "MAX_DD": MAX_DD, "UNIVERSE": UNIVERSE},
    })

    save_state(state)


if __name__ == "__main__":
    run()
