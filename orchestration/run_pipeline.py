"""Orquestrador do pipeline UmFreela Data Lakehouse.

Executa as etapas em sequência. Cada etapa só começa se a anterior terminou
com sucesso; se uma etapa falhar, o pipeline para, registra o erro no log e
grava a execução como FALHOU no monitoramento.

Etapas:
  1. gerar_dados       -> gerador sintético grava novos lotes na landing
  2. ingestao_bronze   -> ingestão micro-batch dos lotes novos para a Bronze
  3. camada_silver     -> limpeza, padronização, validação e deduplicação
  4. camada_gold       -> fato de vagas e indicadores
  5. validacoes        -> testes de qualidade nas camadas Silver e Gold
  6. consultas         -> consultas analíticas (DuckDB) salvas em CSV
  7. dashboard         -> painel PNG e CSVs para o Looker Studio

Uso:
  python orchestration/run_pipeline.py                  # processa os 365 dias
  python orchestration/run_pipeline.py --dias 30        # só os primeiros 30 dias
  python orchestration/run_pipeline.py --dias 45        # rodando de novo: ingere só os 15 dias novos
  python orchestration/run_pipeline.py --a-cada 60 --dias-por-ciclo 1 --ciclos 5
      # modo contínuo: a cada 60 s chega um novo dia e o pipeline roda de novo
"""
import argparse
import csv
import json
import logging
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from dashboard.gerar_dashboard import gerar as gerar_dashboard  # noqa: E402
from ingestion.gerador_eventos import gerar as gerar_dados  # noqa: E402
from ingestion.ingestao_bronze import ingerir  # noqa: E402
from processing.gold import construir_gold  # noqa: E402
from processing.silver import construir_silver  # noqa: E402
from queries.executar_consultas import executar as executar_consultas  # noqa: E402
from tests.test_qualidade import executar_validacoes  # noqa: E402


def _logger(run_id):
    config.garantir_pastas()
    log = logging.getLogger("umfreela")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fmt = logging.Formatter(f"%(asctime)s | {run_id} | %(levelname)s | %(message)s", "%Y-%m-%d %H:%M:%S")
    for h in [logging.FileHandler(config.LOG_DIR / "pipeline.log", encoding="utf-8"),
              logging.StreamHandler(sys.stdout)]:
        h.setFormatter(fmt)
        log.addHandler(h)
    return log


def _registrar(run_id, etapa, inicio, status, detalhes):
    arq = config.MONITOR_DIR / "execucoes.csv"
    novo = not arq.exists()
    with open(arq, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if novo:
            w.writerow(["run_id", "etapa", "inicio", "duracao_s", "status", "detalhes"])
        w.writerow([run_id, etapa, inicio.strftime("%Y-%m-%d %H:%M:%S"),
                    round((datetime.now() - inicio).total_seconds(), 1), status,
                    json.dumps(detalhes, ensure_ascii=False, default=str)])


def executar(dias=None):
    run_id = datetime.now().strftime("%Y%m%d-%H%M%S-") + uuid.uuid4().hex[:4]
    log = _logger(run_id)
    info = log.info
    etapas = [
        ("1_gerar_dados", lambda: {"lotes_novos": len(gerar_dados(dias))}),
        ("2_ingestao_bronze", lambda: ingerir(log=info)),
        ("3_camada_silver", lambda: construir_silver(log=info)),
        ("4_camada_gold", lambda: construir_gold(log=info)),
        ("5_validacoes", lambda: executar_validacoes(log=info)),
        ("6_consultas", lambda: {"consultas": len(executar_consultas(log=info, mostrar=False))}),
        ("7_dashboard", lambda: {"arquivo": str(gerar_dashboard(log=info))}),
    ]
    info(f"===== Início da execução {run_id} =====")
    inicio_total = time.time()
    for nome, func in etapas:
        inicio = datetime.now()
        info(f">> Etapa {nome} iniciada")
        try:
            detalhes = func()
        except Exception as e:  # interrompe o pipeline: etapas dependentes não rodam
            log.error(f"Etapa {nome} FALHOU: {e}")
            _registrar(run_id, nome, inicio, "FALHOU", {"erro": str(e)})
            info(f"===== Execução {run_id} interrompida =====")
            raise
        _registrar(run_id, nome, inicio, "SUCESSO", detalhes)
        info(f"<< Etapa {nome} concluída em {(datetime.now() - inicio).total_seconds():.1f}s")
    info(f"===== Execução {run_id} concluída em {time.time() - inicio_total:.1f}s =====")
    return run_id


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Executa o pipeline UmFreela Data Lakehouse")
    ap.add_argument("--dias", type=int, default=None, help="dias simulados disponíveis na landing")
    ap.add_argument("--a-cada", type=int, default=0, help="modo contínuo: segundos entre execuções")
    ap.add_argument("--dias-por-ciclo", type=int, default=1)
    ap.add_argument("--ciclos", type=int, default=3)
    a = ap.parse_args()
    if a.a_cada:
        dias = a.dias or len(list(config.LANDING_EVENTOS.glob("eventos_*.jsonl"))) or 1
        for c in range(a.ciclos):
            executar(dias)
            dias += a.dias_por_ciclo
            if c < a.ciclos - 1:
                time.sleep(a.a_cada)
    else:
        executar(a.dias)
