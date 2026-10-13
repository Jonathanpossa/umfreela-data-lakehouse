"""Configurações centrais do pipeline UmFreela Data Lakehouse.

Todos os caminhos partem da pasta do projeto. Para guardar o data lake em outro
lugar (por exemplo, no Google Drive dentro do Colab), defina a variável de
ambiente UMF_LAKE_DIR antes de executar o pipeline.
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
LAKE_DIR = Path(os.environ.get("UMF_LAKE_DIR", BASE_DIR / "lake"))

# Pastas do data lake
LANDING_DIR = BASE_DIR / "data" / "generated"          # onde o gerador "entrega" os lotes
LANDING_EVENTOS = LANDING_DIR / "eventos"
LANDING_CADASTROS = LANDING_DIR / "cadastros"
BRONZE_DIR = LAKE_DIR / "bronze"
SILVER_DIR = LAKE_DIR / "silver"
GOLD_DIR = LAKE_DIR / "gold"
MONITOR_DIR = LAKE_DIR / "monitoramento"
LOG_DIR = BASE_DIR / "logs"
CONTROLE_INGESTAO = MONITOR_DIR / "controle_ingestao.json"

# Saídas de consulta e dashboard
RESULTADOS_CONSULTAS = BASE_DIR / "queries" / "resultados"
DASHBOARD_DADOS = BASE_DIR / "dashboard" / "dados"

# Regras de negócio da UmFreela
TAXA_POR_VAGA = 3.99          # valor cobrado do restaurante por vaga publicada (R$)
DIAS_GRATIS = 15              # primeiros dias após o cadastro sem cobrança

CIDADES = ["Santa Maria", "Porto Alegre", "Santiago", "Uruguaiana", "Itaqui", "Quarta Colônia"]
FUNCOES = ["Garçom", "Cozinheiro", "Auxiliar de Cozinha", "Bartender", "Recepcionista",
           "Chapeiro", "Copeiro", "Auxiliar de Limpeza"]
TIPOS_EVENTO = ["vaga_publicada", "candidatura", "contratacao", "turno_concluido",
                "no_show", "cancelamento", "avaliacao", "pagamento"]

# Parâmetros da simulação
DATA_INICIO = "2025-10-01"
DIAS_SIMULADOS = 365
SEMENTE = 42


def garantir_pastas():
    for p in [LANDING_EVENTOS, LANDING_CADASTROS, BRONZE_DIR, SILVER_DIR, GOLD_DIR,
              MONITOR_DIR, LOG_DIR, RESULTADOS_CONSULTAS, DASHBOARD_DADOS]:
        p.mkdir(parents=True, exist_ok=True)
