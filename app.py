import sys
import os

HERE = os.path.dirname(os.path.abspath(__file__))

CANDIDATES = [
    HERE,
    os.path.dirname(HERE),
    os.path.dirname(os.path.dirname(HERE)),
]

CORE_FOUND = False
for p in CANDIDATES:
    if os.path.isdir(os.path.join(p, "core")):
        sys.path.insert(0, p)
        CORE_FOUND = True
        break

if not CORE_FOUND:
    raise RuntimeError(
        f"Não achei a pasta 'core' nos caminhos candidatos: {CANDIDATES}. "
        "Garanta que 'core/' está no repo e contém __init__.py."
    )

import json
import pandas as pd
import streamlit as st
from core.storage import load_state

STATE_DIR = "state"
LOG_FILE = os.path.join(STATE_DIR, "events.log")

st.set_page_config(page_title="Sistema Autônomo de Tendência", layout="wide")

st.title("Sistema Autônomo Baseado em Tendência (Price-Based)")
st.caption("Preço decide. Regra executa. Stop manda. Humano observa.")

# Diagnóstico (apague depois)
with st.expander("Debug (remover depois)"):
    st.write("HERE:", HERE)
    st.write("CANDIDATES:", CANDIDATES)
    st.write("sys.path[0:5]:", sys.path[:5])
    st.write("core found:", CORE_FOUND)

state = load_state()
def load_last_run_from_log(log_file: str):
    if not os.path.exists(log_file):
        return None
    last = None
    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except Exception:
                continue
            if ev.get("type") == "RUN":
                last = ev
    return last

last_run = load_last_run_from_log(LOG_FILE)

# Se state veio "default" / vazio, reconstrói a partir do último RUN do log
if last_run:
    if not state.get("positions"):
        w = last_run.get("weights", {}) or {}
        state["positions"] = {t: {"state": 1 if float(p) > 0 else 0, "weight": float(p)} for t, p in w.items()}
    if state.get("equity", 0) in (0, 100000.0) and "equity" in last_run:
        try:
            state["equity"] = float(last_run["equity"])
        except Exception:
            pass
    if "peak_equity" in last_run:
        try:
            state["peak_equity"] = float(last_run["peak_equity"])
        except Exception:
            pass
    if "drawdown" in last_run:
        try:
            state["last_drawdown"] = float(last_run["drawdown"])
        except Exception:
            pass
    if "kill_switch" in last_run:
        state["kill_switch"] = bool(last_run["kill_switch"])


c1, c2, c3, c4 = st.columns(4)
c1.metric("Equity (MVP)", f"{state.get('equity', 0):,.2f}")
c2.metric("Peak Equity", f"{state.get('peak_equity', 0):,.2f}")
c3.metric("Drawdown", f"{state.get('last_drawdown', 0) * 100:.2f}%")
c4.metric("Kill Switch", "ATIVO" if state.get("kill_switch") else "OFF")

st.divider()

st.subheader("Posições (Estado / Peso)")
pos = state.get("positions", {})
if pos:
    df = pd.DataFrame.from_dict(pos, orient="index")
    df.index.name = "Ticker"
    st.dataframe(df, use_container_width=True)
else:
    st.info("Sem posições registradas ainda. Rode `python jobs/run_daily.py` para gerar o primeiro snapshot.")

st.divider()

st.subheader("Eventos (Audit Trail)")
if os.path.exists(LOG_FILE):
    rows = []
    with open(LOG_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                pass

    if rows:
        logdf = pd.json_normalize(rows).sort_values("ts", ascending=False)
        st.dataframe(logdf, use_container_width=True)
    else:
        st.warning("Log existe, mas está vazio/ilegível.")
else:
    st.warning("Ainda não há log. Execute o job diário para começar a trilha.")
