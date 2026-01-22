import sys
import os

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import json
import pandas as pd
import streamlit as st
from core.storage import load_state


STATE_DIR = "state"
LOG_FILE = os.path.join(STATE_DIR, "events.log")

st.set_page_config(page_title="Sistema Autônomo de Tendência", layout="wide")

st.title("Sistema Autônomo Baseado em Tendência (Price-Based)")
st.caption("Preço decide. Regra executa. Stop manda. Humano observa.")

state = load_state()

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
