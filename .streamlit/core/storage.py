import json
import os
from datetime import datetime
from typing import Any, Dict

STATE_DIR = "state"
STATE_FILE = os.path.join(STATE_DIR, "state.json")
LOG_FILE = os.path.join(STATE_DIR, "events.log")

def _ensure():
    os.makedirs(STATE_DIR, exist_ok=True)

def load_state() -> Dict[str, Any]:
    _ensure()
    if not os.path.exists(STATE_FILE):
        # Estado inicial padrão (MVP)
        return {
            "equity": 100000.0,
            "peak_equity": 100000.0,
            "last_drawdown": 0.0,
            "kill_switch": False,
            "positions": {},  # ticker -> {"state":0/1, "weight":float}
            "last_run": None,
            "version": "mvp-1"
        }
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state: Dict[str, Any]) -> None:
    _ensure()
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def log_event(event: Dict[str, Any]) -> None:
    _ensure()
    event["ts"] = datetime.utcnow().isoformat() + "Z"
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")
