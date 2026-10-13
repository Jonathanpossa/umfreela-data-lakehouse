"""Validações de qualidade das camadas Silver e Gold.

Podem ser executadas de duas formas:
- pelo orquestrador (etapa "validações"), que interrompe o pipeline se algo falhar;
- com pytest:  python -m pytest tests/
"""
import sys
from pathlib import Path

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402


def _um(sql):
    sql = sql.replace("{SILVER}", str(config.SILVER_DIR)).replace("{GOLD}", str(config.GOLD_DIR))
    return duckdb.connect().execute(sql).fetchone()[0]


EV = "read_parquet('{SILVER}/eventos/**/*.parquet', hive_partitioning=true)"
CIDADES = ", ".join(f"'{c}'" for c in config.CIDADES)
FUNCOES = ", ".join(f"'{f}'" for f in config.FUNCOES)

VALIDACOES = {
    "Silver sem event_id duplicado":
        f"SELECT COUNT(*) - COUNT(DISTINCT event_id) = 0 FROM {EV}",
    "Silver sem campos obrigatórios nulos":
        f"SELECT COUNT(*) = 0 FROM {EV} WHERE event_id IS NULL OR event_ts IS NULL "
        f"OR id_vaga IS NULL OR tipo_evento IS NULL",
    "Silver só com cidades padronizadas":
        f"SELECT COUNT(*) = 0 FROM {EV} WHERE cidade NOT IN ({CIDADES})",
    "Silver só com funções padronizadas":
        f"SELECT COUNT(*) = 0 FROM {EV} WHERE funcao NOT IN ({FUNCOES})",
    "Silver com notas entre 1 e 5":
        f"SELECT COUNT(*) = 0 FROM {EV} WHERE tipo_evento = 'avaliacao' AND nota NOT BETWEEN 1 AND 5",
    "Silver com diárias positivas":
        f"SELECT COUNT(*) = 0 FROM {EV} WHERE tipo_evento IN ('vaga_publicada','contratacao','pagamento') "
        f"AND (valor_diaria IS NULL OR valor_diaria <= 0)",
    "Cobrança só de R$ 0 ou R$ 3,99 por vaga":
        f"SELECT COUNT(*) = 0 FROM {EV} WHERE tipo_evento = 'vaga_publicada' "
        f"AND valor_cobrado NOT IN (0, {config.TAXA_POR_VAGA})",
    "Gold com uma linha por vaga":
        "SELECT COUNT(*) = COUNT(DISTINCT id_vaga) FROM read_parquet('{GOLD}/fato_vagas/*.parquet')",
    "Gold com taxas entre 0 e 1":
        "SELECT COUNT(*) = 0 FROM read_parquet('{GOLD}/kpi_mensal/*.parquet') "
        "WHERE taxa_preenchimento NOT BETWEEN 0 AND 1 OR taxa_cancelamento NOT BETWEEN 0 AND 1",
    "Gold com receita igual à soma das cobranças da Silver":
        "SELECT ABS((SELECT SUM(receita_plataforma) FROM read_parquet('{GOLD}/kpi_mensal/*.parquet')) - "
        f"(SELECT SUM(valor_cobrado) FROM {EV} WHERE tipo_evento = 'vaga_publicada')) < 0.01",
}


def executar_validacoes(log=print):
    resultados = {}
    for nome, sql in VALIDACOES.items():
        ok = bool(_um(sql))
        resultados[nome] = ok
        log(f"[{'OK' if ok else 'FALHOU'}] {nome}")
    falhas = [n for n, ok in resultados.items() if not ok]
    if falhas:
        raise AssertionError(f"Validações com falha: {falhas}")
    return resultados


# Versão pytest: cada validação vira um teste
def _criar_teste(nome, sql):
    def teste():
        assert _um(sql), nome
    return teste


for _i, (_n, _s) in enumerate(VALIDACOES.items(), 1):
    globals()[f"test_{_i:02d}"] = _criar_teste(_n, _s)
