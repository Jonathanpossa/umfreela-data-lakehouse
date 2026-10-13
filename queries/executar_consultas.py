"""Executa as consultas de queries/consultas.sql com DuckDB e salva os resultados em CSV."""
import re
import sys
from pathlib import Path

import duckdb

sys.path.append(str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402

ARQUIVO_SQL = Path(__file__).with_name("consultas.sql")


def carregar_consultas():
    texto = ARQUIVO_SQL.read_text(encoding="utf-8")
    blocos = re.split(r"^-- name:\s*", texto, flags=re.M)[1:]
    consultas = {}
    for b in blocos:
        nome, sql = b.split("\n", 1)
        consultas[nome.strip()] = sql.replace("{GOLD}", str(config.GOLD_DIR)).replace(
            "{SILVER}", str(config.SILVER_DIR))
    return consultas


def executar(log=print, mostrar=True):
    config.garantir_pastas()
    con = duckdb.connect()
    resultados = {}
    for nome, sql in carregar_consultas().items():
        df = con.execute(sql).df()
        df.to_csv(config.RESULTADOS_CONSULTAS / f"{nome}.csv", index=False)
        resultados[nome] = df
        if mostrar:
            log(f"\n=== {nome} ===\n{df.to_string(index=False)}")
    log(f"{len(resultados)} consultas executadas; resultados em {config.RESULTADOS_CONSULTAS}")
    return resultados


if __name__ == "__main__":
    executar()
