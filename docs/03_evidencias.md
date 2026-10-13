# Entrega 3: Evidências de Execução

Execução realizada no **Google Colab** em 13/10/2026 (horários em UTC), usando o notebook [`notebooks/UmFreela_Pipeline_Colab.ipynb`](../notebooks/UmFreela_Pipeline_Colab.ipynb).

Notebook executado, com todas as saídas: https://colab.research.google.com/drive/1VEodySVxSd9Jh3xadrzMghwGJysA0ycd?usp=sharing

Ambiente: Python 3.13, PySpark 3.5.9 (modo local), DuckDB 1.3.2.

O pipeline foi executado **duas vezes** para demonstrar a ingestão incremental:

1. **Execução 1** (`--dias 335`): chegam os lotes de 01/10/2025 a 31/08/2026.
2. **Execução 2** (`--dias 365`): chegam mais 30 dias (setembro de 2026). A ingestão lê **somente os 30 lotes novos**.

## 1. Orquestração e monitoramento (`lake/monitoramento/execucoes.csv`)

| Execução | Etapa | Duração (s) | Status | Detalhes |
|---|---|---|---|---|
| 1 | 1_gerar_dados | 16,3 | SUCESSO | 335 lotes novos |
| 1 | 2_ingestao_bronze | 40,9 | SUCESSO | 335 lotes, 197.616 eventos, 420 restaurantes |
| 1 | 3_camada_silver | 40,9 | SUCESSO | 191.737 válidos, 5.879 rejeitados (2,97%) |
| 1 | 4_camada_gold | 20,5 | SUCESSO | 26.818 vagas, 21.675 preenchidas, 11 meses |
| 1 | 5_validacoes | 1,2 | SUCESSO | 10 de 10 verificações OK |
| 1 | 6_consultas | 0,6 | SUCESSO | 6 consultas |
| 1 | 7_dashboard | 2,7 | SUCESSO | dashboard_umfreela.png |
| 2 | 1_gerar_dados | 5,0 | SUCESSO | 30 lotes novos |
| 2 | 2_ingestao_bronze | 22,9 | SUCESSO | **30 lotes, 32.884 eventos** (só os novos) |
| 2 | 3_camada_silver | 48,7 | SUCESSO | 223.687 válidos, 6.813 rejeitados (2,96%) |
| 2 | 4_camada_gold | 22,8 | SUCESSO | 31.256 vagas, 25.260 preenchidas, 12 meses |
| 2 | 5_validacoes | 0,7 | SUCESSO | 10 de 10 verificações OK |
| 2 | 6_consultas | 0,4 | SUCESSO | 6 consultas |
| 2 | 7_dashboard | 1,7 | SUCESSO | dashboard_umfreela.png |

Tempo total: 123 s na execução 1 e 102 s na execução 2.

Trecho do log (`logs/pipeline.log`) da execução 2:

```text
2026-10-13 02:49:03 | 20261013-024746-b477 | INFO | Silver construída: {'eventos_bronze': 230500, 'eventos_silver': 223687, 'rejeitados_total': 6813, 'percentual_rejeitado': 2.96, ...}
2026-10-13 02:49:26 | 20261013-024746-b477 | INFO | Gold construída: {'vagas': 31256, 'preenchidas': 25260, 'meses': 12}
2026-10-13 02:49:26 | 20261013-024746-b477 | INFO | [OK] Silver sem event_id duplicado
2026-10-13 02:49:26 | 20261013-024746-b477 | INFO | [OK] Gold com receita igual à soma das cobranças da Silver
2026-10-13 02:49:29 | 20261013-024746-b477 | INFO | ===== Execução 20261013-024746-b477 concluída em 102.2s =====
```

## 2. Dados em cada camada

| Camada | Conteúdo | Volume |
|---|---|---|
| Landing | arquivos JSON Lines, um por dia | 365 arquivos |
| Bronze | JSON original + `arquivo_origem`, `data_lote`, `ingestao_ts` | 230.500 eventos |
| Silver | eventos limpos e padronizados | 223.687 eventos |
| Silver (rejeitados) | registros inválidos com o motivo | 6.813 registros |
| Gold | `fato_vagas` (uma linha por vaga) | 31.256 vagas |

Exemplo de evento bruto na landing, com problemas de qualidade (data em formato brasileiro, função com espaços):

```json
{"event_id": "E619453023189951", "tipo_evento": "candidatura", "event_ts": "01/10/2025 19:08:37", "cidade": "Santa Maria", "funcao": " Auxiliar de Cozinha ", ...}
```

Exemplo na Silver (vaga publicada, já com a cobrança calculada):

| event_id | event_ts | cidade | funcao | valor_diaria | em_periodo_gratuito | valor_cobrado |
|---|---|---|---|---|---|---|
| E019948373888825 | 2025-10-17 21:07:58 | Itaqui | Garçom | 155,00 | False | 3,99 |
| E029911072923667 | 2025-10-30 10:01:45 | Porto Alegre | Chapeiro | 165,00 | False | 3,99 |

## 3. Qualidade dos dados (registros rejeitados na Silver)

| Motivo | Registros |
|---|---|
| duplicado | 4.432 |
| tipo_evento_invalido | 649 |
| sem_id_vaga | 612 |
| data_invalida_ou_ausente | 576 |
| nota_fora_da_escala | 358 |
| valor_diaria_invalido | 186 |
| **Total** | **6.813 (2,96%)** |

As 10 validações de qualidade passaram no pytest (`10 passed in 1.06s`).

## 4. Resultados das consultas

**Indicadores mensais** (consulta 01)

| Mês | Vagas | Preench. (%) | Tempo até contratar (h) | Receita (R$) | Vagas grátis | Cancel. (%) | No-show (%) | Nota |
|---|---|---|---|---|---|---|---|---|
| 2025-10 | 1.212 | 79,5 | 8,96 | 4.724,16 | 28 | 6,7 | 3,6 | 4,30 |
| 2025-11 | 1.252 | 81,4 | 8,93 | 4.744,11 | 63 | 5,7 | 3,0 | 4,28 |
| 2025-12 | 2.067 | 79,7 | 8,75 | 7.588,98 | 165 | 6,9 | 4,3 | 4,24 |
| 2026-01 | 1.538 | 81,9 | 9,38 | 5.669,79 | 117 | 7,3 | 3,9 | 4,26 |
| 2026-02 | 1.535 | 79,1 | 9,08 | 5.777,52 | 87 | 7,0 | 3,4 | 4,25 |
| 2026-03 | 2.347 | 82,2 | 8,88 | 8.718,15 | 162 | 5,8 | 3,4 | 4,26 |
| 2026-04 | 2.494 | 79,7 | 8,79 | 9.560,04 | 98 | 6,3 | 4,1 | 4,27 |
| 2026-05 | 3.166 | 81,8 | 8,85 | 11.634,84 | 250 | 6,5 | 3,1 | 4,25 |
| 2026-06 | 3.302 | 81,4 | 8,95 | 12.269,25 | 227 | 5,6 | 3,9 | 4,32 |
| 2026-07 | 3.701 | 81,3 | 8,82 | 14.172,48 | 149 | 6,2 | 3,8 | 4,28 |
| 2026-08 | 4.204 | 81,2 | 8,93 | 16.039,80 | 184 | 6,0 | 4,0 | 4,26 |
| 2026-09 | 4.438 | 79,7 | 8,73 | 16.873,71 | 209 | 5,9 | 3,5 | 4,24 |

**Funções com falta de freelancer** (consulta 02): Cozinheiro tem a menor taxa de preenchimento (73,1%) e a maior diária média (R$ 200,03), seguido de Bartender (76,8%). Auxiliar de Limpeza é a mais fácil de preencher (84,2%).

**Ranking de cidades** (consulta 03)

| Cidade | Vagas | Preench. (%) | Receita (R$) | Nota |
|---|---|---|---|---|
| Santa Maria | 10.443 | 85,5 | 39.401,25 | 4,28 |
| Porto Alegre | 8.894 | 86,7 | 33.440,19 | 4,28 |
| Quarta Colônia | 3.713 | 69,1 | 14.144,55 | 4,24 |
| Uruguaiana | 3.177 | 78,8 | 11.950,05 | 4,24 |
| Santiago | 2.974 | 73,1 | 10.952,55 | 4,24 |
| Itaqui | 2.055 | 66,8 | 7.884,24 | 4,23 |

**Horários de pico** (consulta 04): os 10 horários com mais vagas publicadas são todos no sábado, com pico às 14h (502 vagas).

**Status final das vagas** (consulta 05): 71,5% concluídas, 18,5% não preenchidas, 5,0% canceladas, 3,0% com falta do freelancer e 2,0% contratadas aguardando o turno.

## 5. Dashboard

O painel é gerado a cada execução em `dashboard/dashboard_umfreela.png` e aparece na última célula do notebook. Indicadores gerais no fim do período:

| Vagas publicadas | Taxa de preenchimento | Tempo médio até contratar | Receita da plataforma |
|---|---|---|---|
| 31.256 | 80,8% | 8,9 h | R$ 117.772,83 |

## 6. Principais conclusões para a UmFreela

- **Onde faltam freelancers:** Itaqui (66,8%) e Quarta Colônia (69,1%) preenchem bem menos vagas que Santa Maria e Porto Alegre. São as cidades prioritárias para campanhas de captação de freelancers.
- **Funções críticas:** Cozinheiro e Bartender são as mais difíceis de preencher, mesmo pagando as maiores diárias.
- **Momento de maior demanda:** sábado à tarde concentra o maior volume de vagas publicadas.
- **Receita:** cresce mês a mês acompanhando o número de vagas; os 15 dias grátis representam entre 2% e 8% das vagas publicadas no mês.
