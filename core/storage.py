import json
import os
from datetime import datetime

STATE_DIR = "state"
STATE_FILE = os.path.join(STATE_DIR, "state.json")
LOG_FILE = os.path.join(STATE_DIR, "events.log")


DEFAULT_STATE = {
    "equity": 100000.0,
    "peak_equity": 100000.0,
    "last_drawdown": 0.0,
    "kill_switch": False,
    "positions": {},
    "last_prices": {},
    "last_run": None,
}


def _ensure_state_dir():
    os.makedirs(STATE_DIR, exist_ok=True)
    # garante que o git mantém a pasta
    keep = os.path.join(STATE_DIR, ".gitkeep")
    if not os.path.exists(keep):
        try:
            open(keep, "a").close()
        except Exception:
            pass


def load_state():
    """
    Leitura resiliente:
    - Se state.json não existir => DEFAULT_STATE
    - Se estiver corrompido/truncado => tenta backup; senão DEFAULT_STATE
    """
    _ensure_state_dir()

    if not os.path.exists(STATE_FILE):
        return DEFAULT_STATE.copy()

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # merge defensivo: garante chaves mínimas
        merged = DEFAULT_STATE.copy()
        if isinstance(data, dict):
            merged.update(data)
        return merged
    except Exception:
        # fallback: tenta backup
        backup = STATE_FILE + ".bak"
        if os.path.exists(backup):
            try:
                with open(backup, "r", encoding="utf-8") as f:
                    data = json.load(f)
                merged = DEFAULT_STATE.copy()
                if isinstance(data, dict):
                    merged.update(data)
                return merged
            except Exception:
                pass

        # último recurso: volta pro default
        return DEFAULT_STATE.copy()


def save_state(state: dict):
    """
    Escrita atômica:
    - escreve em tmp
    - faz backup do atual
    - replace atomic (os.replace)
    Isso evita state.json truncado.
    """
    _ensure_state_dir()

    tmp = STATE_FILE + ".tmp"
    bak = STATE_FILE + ".bak"

    payload = DEFAULT_STATE.copy()
    if isinstance(state, dict):
        payload.update(state)

    # escreve tmp
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())

    # backup do antigo (se existir)
    if os.path.exists(STATE_FILE):
        try:
            os.replace(STATE_FILE, bak)
        except Exception:
            pass

    # promote tmp -> final (atômico)
    os.replace(tmp, STATE_FILE)


def log_event(event: dict):
    _ensure_state_dir()

    record = {"ts": datetime.utcnow().isoformat() + "Z"}
    if isinstance(event, dict):
        record.update(event)

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
