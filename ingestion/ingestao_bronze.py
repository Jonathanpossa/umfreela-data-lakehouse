"""Ingestão automática em micro-batch: landing zone -> camada Bronze.

A cada execução, o script:
1. lista os lotes que chegaram na pasta data/generated/eventos;
2. consulta o arquivo de controle para saber quais já foram ingeridos;
3. lê apenas os lotes novos com Spark e grava na Bronze em Parquet, mantendo
   cada evento exatamente como chegou (texto JSON original) e acrescentando
   metadados de ingestão (arquivo de origem, data do lote e horário da ingestão);
4. atualiza o arquivo de controle.

O cadastro de restaurantes (segunda fonte, CSV) é carregado inteiro a cada
execução, também sem nenhuma transformação.
"""
import json
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from processing.spark_session import obter_spark  # noqa: E402


def _ler_controle():
    if config.CONTROLE_INGESTAO.exists():
        return json.loads(config.CONTROLE_INGESTAO.read_text(encoding="utf-8"))
    return {"arquivos_ingeridos": {}}


def _salvar_controle(ctrl):
    config.CONTROLE_INGESTAO.write_text(json.dumps(ctrl, indent=1, ensure_ascii=False),
                                        encoding="utf-8")


def ingerir(log=print):
    config.garantir_pastas()
    spark = obter_spark()
    ctrl = _ler_controle()
    ja = ctrl["arquivos_ingeridos"]
    todos = sorted(config.LANDING_EVENTOS.glob("eventos_*.jsonl"))
    novos = [p for p in todos if p.name not in ja]
    log(f"Lotes na landing: {len(todos)} | já ingeridos: {len(ja)} | novos: {len(novos)}")

    resumo = {"lotes_novos": len(novos), "eventos_ingeridos": 0}
    if novos:
        agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df = (spark.read.text([str(p) for p in novos])
              .withColumnRenamed("value", "json_original")
              .withColumn("arquivo_origem", F.element_at(F.split(F.input_file_name(), "/"), -1))
              .withColumn("data_lote", F.regexp_extract("arquivo_origem", r"(\d{4}-\d{2}-\d{2})", 1))
              .withColumn("ingestao_ts", F.lit(agora).cast("timestamp")))
        (df.write.mode("append").partitionBy("data_lote")
         .parquet(str(config.BRONZE_DIR / "eventos")))
        contagem = {r["arquivo_origem"]: r["count"] for r in
                    df.groupBy("arquivo_origem").count().collect()}
        for p in novos:
            ja[p.name] = {"linhas": int(contagem.get(p.name, 0)), "ingerido_em": agora}
        resumo["eventos_ingeridos"] = int(sum(contagem.values()))
        _salvar_controle(ctrl)

    cad = (spark.read.option("header", True).csv(str(config.LANDING_CADASTROS / "restaurantes.csv"))
           .withColumn("ingestao_ts", F.current_timestamp()))
    cad.write.mode("overwrite").parquet(str(config.BRONZE_DIR / "restaurantes"))
    resumo["restaurantes"] = cad.count()
    log(f"Bronze atualizada: {resumo}")
    return resumo


if __name__ == "__main__":
    ingerir()
