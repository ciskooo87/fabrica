import sys
import os

# ✅ Garante que a raiz do projeto (onde fica /core) está no PYTHONPATH
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from datetime import datetime
from core.storage import load_state, save_state, log_event
from core.data import fetch_history
from core.strategy import signal_on_off
from core.portfolio import compute_weights, update_kill_switch, diff_states

# Universo MVP (5 funções -> 5 proxies)
UNIVERSE = {
    "Risco direcional": "SPY",
    "Proteção": "TLT",
    "Real asset": "DBC",
    "Taxas": "IEF",
    "Moeda forte": "UUP",
}

SMA_WINDOW = 200
MAX_DD = 0.20  # 20% drawdown => OFF global e suspende


def run():
    # ✅ Garante que o diretório state existe (em qualquer ambiente)
    os.makedirs(os.path.join(ROOT_DIR, "state"), exist_ok=True)

    state = load_state()

    # Governança: se kill switch ativo, não roda (não negocia)
    if state.get("kill_switch", False):
        log_event({"type": "RUN_SKIPPED", "reason": "KILL_SWITCH_ACTIVE"})
        return

    signals = {}
    last_prices = {}

    # Sinais por ativo
    for _, ticker in UNIVERSE.items():
        df = fetch_history(ticker, period="10y", interval="1d")
        sigdf = signal_on_off(df, sma_window=SMA_WINDOW)

        if sigdf.empty:
            raise RuntimeError(f"Sem dados suficientes para sinal (SMA{SMA_WINDOW}) em {ticker}")

        last = sigdf.iloc[-1]
        signals[ticker] = int(last["Signal"])
