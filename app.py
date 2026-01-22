import json
import os
import pandas as pd
import streamlit as st

from core.storage import load_state

STATE_DIR = "state"
LOG_FILE = os.path.join(STATE_DIR, "events.log")


# ----------------------------
# Helpers
# ----------------------------
def load_events(log_file: str):
    rows = []
    if not os.path.exists(log_file):
        return rows
    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                # pula linha quebrada
                pass
    return rows


def last_run_event(events):
    for ev in reversed(events):
        if ev.get("type") == "RUN":
            return ev
    return None


def state_from_last_run(state: dict, run_ev: dict):
    """Se state estiver incompleto, completa com info do último RUN."""
    if not run_ev:
        return state

    # positions
    if not state.get("positions"):
        w = run_ev.get("weights", {}) or {}
        state["positions"] = {t: {"state": 1 if float(p) > 0 else 0, "weight": float(p)} for t, p in w.items()}

    # equity / peak / drawdown / return
    if "equity" in run_ev and (state.get("equity") in (None, 0, 100000.0)):
        try:
            state["equity"] = float(run_ev["equity"])
        except Exception:
            pass

    if "peak_equity" in run_ev:
        try:
            state["peak_equity"] = float(run_ev["peak_equity"])
        except Exception:
            pass

    if "drawdown" in run_ev:
        try:
            state["last_drawdown"] = float(run_ev["drawdown"])
        except Exception:
            pass

    if "portfolio_return" in run_ev and state.get("last_portfolio_return") is None:
        try:
            state["last_portfolio_return"] = float(run_ev["portfolio_return"])
        except Exception:
            pass

    # kill switch
    if "kill_switch" in run_ev:
        state["kill_switch"] = bool(run_ev["kill_switch"])

    # last run timestamp
    if state.get("last_run") is None and "ts" in run_ev:
        state["last_run"] = run_ev.get("ts")

    return state


def health_label(state: dict, run_ev: dict, events: list):
    """
    Health simples e objetivo:
    - STOPPED: kill_switch
    - DEGRADED: sem RUN recente / sem positions / sem prices no último run
    - OK: tem RUN + positions + weights
    """
    if state.get("kill_switch"):
        return "STOPPED"

    if not events or not run_ev:
        return "DEGRADED"

    if not state.get("positions"):
        return "DEGRADED"

    if not run_ev.get("weights"):
        return "DEGRADED"

    return "OK"


# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title="Sistema Autônomo de Tendência", layout="wide")

st.title("Sistema Autônomo Baseado em Tendência (Price-Based)")
st.caption("Preço decide. Regra executa. Stop manda. Humano observa.")

with st.expander("Debug (remover depois)", expanded=False):
    st.write("LOG_FILE:", LOG_FILE)
    st.write("STATE_DIR exists:", os.path.exists(STATE_DIR))

state = load_state()
events = load_events(LOG_FILE)
run_ev = last_run_event(events)
state = state_from_last_run(state, run_ev)

status = health_label(state, run_ev, events)

# KPIs
c1, c2, c3, c4, c5 = st.columns(5)

equity = float(state.get("equity", 100000.0))
peak = float(state.get("peak_equity", equity))
dd = float(state.get("last_drawdown", 0.0))
ret = float(state.get("last_portfolio_return", 0.0) or 0.0)

c1.metric("Equity", f"{equity:,.2f}")
c2.metric("Peak Equity", f"{peak:,.2f}")
c3.metric("Drawdown", f"{dd*100:.2f}%")
c4.metric("Retorno do dia", f"{ret*100:.2f}%")
c5.metric("Health", status)

st.divider()

# Last run
st.subheader("Último Run")
if run_ev:
    st.write(f"**Timestamp (log):** {run_ev.get('ts')}")
    st.write(f"**Kill Switch:** {'ON' if bool(run_ev.get('kill_switch')) else 'OFF'}")
else:
    st.warning("Ainda não há evento RUN no log.")

st.divider()

# Positions
st.subheader("Posições (Estado / Peso)")
pos = state.get("positions", {}) or {}
if pos:
    df = pd.DataFrame.from_dict(pos, orient="index")
    df.index.name = "Ticker"
    df = df.sort_values(["state", "weight"], ascending=[False, False])
    st.dataframe(df, use_container_width=True)
else:
    st.info("Sem posições registradas ainda. Execute o job diário.")

st.divider()

# Signals/Weights of last run (clean view)
st.subheader("Sinais e Pesos (Último Run)")
if run_ev:
    sig = run_ev.get("signals", {}) or {}
    w = run_ev.get("weights", {}) or {}
    px = run_ev.get("prices", {}) or {}

    view = []
    tickers = sorted(set(list(sig.keys()) + list(w.keys()) + list(px.keys())))
    for t in tickers:
        view.append({
            "ticker": t,
            "signal": sig.get(t),
            "weight": w.get(t),
            "price": px.get(t),
        })

    vdf = pd.DataFrame(view)
    st.dataframe(vdf, use_container_width=True)
else:
    st.info("Sem RUN ainda.")

st.divider()

# Events table
st.subheader("Eventos (Audit Trail)")
if events:
    logdf = pd.json_normalize(events)
    # ordena desc por ts se existir
    if "ts" in logdf.columns:
        logdf = logdf.sort_values("ts", ascending=False)
    st.dataframe(logdf, use_container_width=True)
else:
    st.warning("Ainda não há log. Execute o job diário para começar a trilha.")
