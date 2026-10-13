"""Cria (ou reaproveita) a sessão Spark usada por todas as etapas."""
from pyspark.sql import SparkSession

_spark = None


def obter_spark():
    global _spark
    if _spark is None:
        _spark = (SparkSession.builder
                  .appName("UmFreela Data Lakehouse")
                  .master("local[*]")
                  .config("spark.sql.session.timeZone", "UTC")
                  .config("spark.sql.shuffle.partitions", "8")
                  .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
                  .config("spark.ui.showConsoleProgress", "false")
                  .getOrCreate())
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark
