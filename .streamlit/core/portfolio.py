from typing import Dict, Tuple, List

def compute_weights(signals: Dict[str, int]) -> Dict[str, float]:
    on = [t for t, s in signals.items() if s == 1]
    if len(on) == 0:
        return {t: 0.0 for t in signals.keys()}
    w = 1.0 / len(on)
    return {t: (w if signals[t] == 1 else 0.0) for t in signals.keys()}

def update_kill_switch(equity: float, peak_equity: float, max_dd: float) -> Tuple[bool, float, float]:
    """
    Drawdown = (peak - equity) / peak
    Se drawdown >= max_dd => kill switch
    """
    peak = max(peak_equity, equity)
    dd = 0.0 if peak == 0 else (peak - equity) / peak
    kill = dd >= max_dd
    return kill, peak, dd

def diff_states(prev_positions: Dict[str, dict], new_weights: Dict[str, float]) -> List[dict]:
    """
    Gera “ordens” apenas quando há mudança de estado:
      OFF->ON = ENTER
      ON->OFF = EXIT
    """
    orders = []
    for ticker, w in new_weights.items():
        prev_state = int(prev_positions.get(ticker, {}).get("state", 0))
        new_state = 1 if w > 0 else 0

        if prev_state == 0 and new_state == 1:
            orders.append({"ticker": ticker, "action": "ENTER"})
        elif prev_state == 1 and new_state == 0:
            orders.append({"ticker": ticker, "action": "EXIT"})
    return orders
