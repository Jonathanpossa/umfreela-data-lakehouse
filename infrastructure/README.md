# Infraestrutura

Este projeto foi construído para rodar **sem nenhum recurso em nuvem pago e sem Docker**, seguindo a Opção B do enunciado (ambiente local com processamento batch):

- **Execução:** Google Colab ou qualquer computador com Python 3.10+ e Java 8/11/17.
- **Armazenamento:** diretórios locais organizados em camadas (`lake/bronze`, `lake/silver`, `lake/gold`) com arquivos Parquet. A variável `UMF_LAKE_DIR` permite apontar o data lake para outra pasta, como o Google Drive.
- **Processamento:** Spark em modo local (`local[*]`), configurado em `processing/spark_session.py`.

Por isso não há `docker-compose.yml`, CloudFormation ou Terraform.

## Evolução para a nuvem (referência)

O mesmo desenho pode ser levado para a AWS (Opção C do enunciado) sem mudar a lógica:

| Hoje | Na AWS |
|---|---|
| `data/generated` (landing) | bucket S3 `umfreela-landing` |
| `lake/bronze`, `silver`, `gold` | buckets ou prefixos S3 por camada |
| `silver.py` e `gold.py` (PySpark local) | jobs do AWS Glue (PySpark) |
| DuckDB | Amazon Athena |
| Dashboard PNG / Looker Studio | Amazon QuickSight |
| `run_pipeline.py` | AWS Step Functions ou Amazon EventBridge |
