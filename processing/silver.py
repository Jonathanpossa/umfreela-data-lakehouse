"""Camada Silver: dados limpos, padronizados e validados.

Transformações aplicadas sobre a Bronze:
- leitura do JSON original com um esquema definido;
- padronização de datas (ISO, formato brasileiro e epoch em milissegundos);
- padronização de nomes de cidade e função (maiúsculas, espaços e acentos);
- conversão de tipos (valores em reais, notas, datas);
- validação de regras de negócio; registros inválidos vão para uma tabela de
  rejeitados com o motivo, em vez de serem apagados;
- remoção de eventos duplicados (mesmo event_id reenviado pelo aplicativo);
- cruzamento com o cadastro de restaurantes para calcular a cobrança da vaga
  (R$ 3,99 por vaga, com os primeiros 15 dias após o cadastro gratuitos);
- criação de colunas de apoio (data, hora, mês, dia da semana).
"""
import sys
import unicodedata
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402
from processing.spark_session import obter_spark  # noqa: E402

ESQUEMA_EVENTO = T.StructType([T.StructField(c, T.StringType()) for c in [
    "event_id", "tipo_evento", "event_ts", "id_vaga", "id_restaurante", "id_freela",
    "cidade", "funcao", "canal", "valor_diaria", "data_turno", "nota", "cancelado_por"]])

COM_ACENTO = "áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ"
SEM_ACENTO = "aaaaaeeeeiiiiooooouuuucAAAAAEEEEIIIIOOOOOUUUUC"


def _chave(texto):
    """Mesma normalização do Spark, em Python, para montar o dicionário de nomes."""
    t = "".join(c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn")
    return t.strip().lower()


def _normalizar(col):
    return F.lower(F.trim(F.translate(col, COM_ACENTO, SEM_ACENTO)))


def _mapa(valores):
    pares = []
    for v in valores:
        pares += [F.lit(_chave(v)), F.lit(v)]
    return F.create_map(*pares)


def _parse_ts(col):
    return F.coalesce(
        F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss"),
        F.to_timestamp(col, "dd/MM/yyyy HH:mm:ss"),
        F.when(col.rlike(r"^\d{12,13}$"), F.timestamp_millis(col.cast("long"))),
    )


def construir_silver(log=print):
    spark = obter_spark()
    mapa_cid, mapa_fun = _mapa(config.CIDADES), _mapa(config.FUNCOES)

    # ---------- Restaurantes (segunda fonte) ----------
    rest = (spark.read.parquet(str(config.BRONZE_DIR / "restaurantes"))
            .withColumn("cidade", mapa_cid[_normalizar(F.col("cidade"))])
            .withColumn("data_cadastro", F.to_date("data_cadastro"))
            .dropDuplicates(["id_restaurante"])
            .drop("ingestao_ts"))
    rest.write.mode("overwrite").parquet(str(config.SILVER_DIR / "restaurantes"))

    # ---------- Eventos ----------
    bronze = spark.read.parquet(str(config.BRONZE_DIR / "eventos"))
    total_bronze = bronze.count()
    ev = (bronze
          .withColumn("j", F.from_json("json_original", ESQUEMA_EVENTO))
          .select("j.*", "arquivo_origem", "ingestao_ts")
          .withColumn("event_ts", _parse_ts(F.col("event_ts")))
          .withColumn("data_turno", F.to_timestamp("data_turno", "yyyy-MM-dd'T'HH:mm:ss"))
          .withColumn("cidade_original", F.col("cidade"))
          .withColumn("cidade", mapa_cid[_normalizar(F.col("cidade"))])
          .withColumn("funcao", mapa_fun[_normalizar(F.col("funcao"))])
          .withColumn("valor_diaria", F.col("valor_diaria").cast("double"))
          .withColumn("nota", F.col("nota").cast("int")))

    tipos = F.array(*[F.lit(t) for t in config.TIPOS_EVENTO])
    usa_valor = F.col("tipo_evento").isin("vaga_publicada", "contratacao", "pagamento")
    motivo = (F.when(F.col("event_id").isNull(), "sem_event_id")
              .when(F.col("tipo_evento").isNull() | ~F.array_contains(tipos, F.col("tipo_evento")),
                    "tipo_evento_invalido")
              .when(F.col("event_ts").isNull(), "data_invalida_ou_ausente")
              .when(F.col("id_vaga").isNull(), "sem_id_vaga")
              .when(F.col("cidade").isNull(), "cidade_desconhecida")
              .when(F.col("funcao").isNull(), "funcao_desconhecida")
              .when(usa_valor & ((F.col("valor_diaria").isNull()) | (F.col("valor_diaria") <= 0)),
                    "valor_diaria_invalido")
              .when((F.col("tipo_evento") == "avaliacao") &
                    (F.col("nota").isNull() | ~F.col("nota").between(1, 5)), "nota_fora_da_escala"))
    ev = ev.withColumn("motivo_rejeicao", motivo)

    # Deduplicação: mantém a primeira ocorrência de cada event_id
    w = Window.partitionBy("event_id").orderBy("ingestao_ts", "arquivo_origem")
    ev = ev.withColumn("ocorrencia", F.row_number().over(w))
    ev = ev.withColumn("motivo_rejeicao", F.when(
        F.col("motivo_rejeicao").isNull() & (F.col("ocorrencia") > 1) & F.col("event_id").isNotNull(),
        "duplicado").otherwise(F.col("motivo_rejeicao"))).cache()

    rejeitados = ev.filter(F.col("motivo_rejeicao").isNotNull())
    (rejeitados.select("event_id", "tipo_evento", "id_vaga", "cidade_original", "arquivo_origem",
                       "motivo_rejeicao", "ingestao_ts")
     .write.mode("overwrite").parquet(str(config.SILVER_DIR / "eventos_rejeitados")))

    validos = (ev.filter(F.col("motivo_rejeicao").isNull())
               .drop("motivo_rejeicao", "ocorrencia", "cidade_original"))

    # Cobrança da vaga: R$ 3,99, exceto nos primeiros 15 dias após o cadastro do restaurante
    validos = (validos.join(rest.select("id_restaurante", "data_cadastro", "segmento"),
                            "id_restaurante", "left")
               .withColumn("em_periodo_gratuito", F.when(
                   F.col("tipo_evento") == "vaga_publicada",
                   F.datediff(F.to_date("event_ts"), F.col("data_cadastro")) < config.DIAS_GRATIS))
               .withColumn("valor_cobrado", F.when(F.col("tipo_evento") == "vaga_publicada",
                                                   F.when(F.col("em_periodo_gratuito"), F.lit(0.0))
                                                   .otherwise(F.lit(config.TAXA_POR_VAGA))))
               .withColumn("data_evento", F.to_date("event_ts"))
               .withColumn("hora", F.hour("event_ts"))
               .withColumn("mes", F.date_format("event_ts", "yyyy-MM"))
               .withColumn("dia_semana", F.dayofweek("event_ts")))

    validos.write.mode("overwrite").partitionBy("mes").parquet(str(config.SILVER_DIR / "eventos"))

    por_motivo = {r["motivo_rejeicao"]: r["count"]
                  for r in rejeitados.groupBy("motivo_rejeicao").count().collect()}
    n_validos = total_bronze - sum(por_motivo.values())
    qualidade = {
        "eventos_bronze": total_bronze,
        "eventos_silver": n_validos,
        "rejeitados_total": sum(por_motivo.values()),
        "percentual_rejeitado": round(100 * sum(por_motivo.values()) / max(total_bronze, 1), 2),
        "rejeitados_por_motivo": por_motivo,
        "restaurantes": rest.count(),
    }
    ev.unpersist()
    log(f"Silver construída: {qualidade}")
    return qualidade


if __name__ == "__main__":
    construir_silver()
