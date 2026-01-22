# ============================
# SISTEMA AUTÔNOMO DE TENDÊNCIA — CONFIG (B3 READY)
# Preço decide. Regra executa. Stop manda. Humano observa.
# ============================

[system]
name = "trend_system_b3"
base_currency = "BRL"
initial_equity = 100000.0

# "daily" = avalia no fechamento do dia
# (se você usa dados diários, esse é o correto)
frequency = "daily"

# Quando não há ativos ON, fica em caixa (0% risco)
cash_when_all_off = true

# Se um ativo não tiver dados no dia, ele é ignorado naquele run (não quebra o job)
skip_missing_data = true


# ============================
# UNIVERSO (FUNÇÕES -> TICKERS)
# ============================
[universe]
# Risco direcional Brasil (Ibovespa)
risk_directional = "BOVA11"

# Moeda forte / Risco global em BRL (S&P 500 em reais)
strong_currency = "IVVB11"

# Juros reais / proteção doméstica (NTN-B via IMA-B)
rates_real = "IMAB11"

# Real asset / inflação (ouro em BRL)
real_asset = "GOLD11"


# ============================
# REGRA DE TENDÊNCIA (IGUAL P/ TODOS)
# ============================
[trend]
# Referência intermediária: média móvel simples (SMA)
reference = "SMA"

# Janela intermediária (opinião forte):
# 126 dias ~ ~6 meses úteis. É um bom "meio do caminho" pra B3.
window = 126

# Usa preço de fechamento (coerente com seu mantra)
price_field = "Close"

# Avalia sempre no último fechamento disponível
evaluate_on = "close"


# ============================
# EXECUÇÃO / ESTADOS
# ============================
[execution]
# Sem escala, sem parcial: só entra/saí quando muda estado
mode = "state_only"
allow_partial = false
allow_scale_in = false
allow_scale_out = false

# Custos estimados (paper):
# Ajuste depois com sua realidade de corretagem + emolumentos
commission_bps = 2.0      # 0.02% por trade (bem conservador)
slippage_bps = 3.0        # 0.03% (B3 pode variar; melhor começar conservador)


# ============================
# ALOCAÇÃO
# ============================
[allocation]
# Peso igual entre todos ON
method = "equal_weight_on"

# Peso mínimo para considerar ON (protege de float lixo)
min_weight = 0.0001

# Alavancagem (não use agora)
leverage = 1.0


# ============================
# KILL SWITCH (PORTFÓLIO)
# ============================
[kill_switch]
enabled = true

# Se drawdown máximo for atingido: desliga tudo (autoridade máxima)
max_drawdown = 0.20

# Envelope de vol anualizada (proxy): se sair do range, pausa
# (opcional, útil pra evitar "regime quebrado")
volatility_check_enabled = true
vol_window = 20
vol_max_annualized = 0.35

# Se algo operacional falhar (sem dados geral, exceções): corta execução
operational_fail_stop = true


# ============================
# DADOS
# ============================
[data]
provider = "brapi"
base_url = "https://brapi.dev/api"
range = "10y"
interval = "1d"

# token via variável de ambiente (NUNCA hardcode)
token_env = "BRAPI_TOKEN"

# BRAPI usa tickers B3 canônicos (ex.: BOVA11, IVVB11...)
ticker_style = "canonical"



# Últimos N anos de histórico desejado (não obrigatório, mas bom pra tendência)
lookback_years = 10


# ============================
# OUTPUT / LOG
# ============================
[output]
state_file = "state/state.json"
events_log = "state/events.log"
snapshots_dir = "state/snapshots"

# Mantém log enxuto (se quiser)
max_events = 5000
