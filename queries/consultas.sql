-- Consultas analíticas sobre a camada Gold (executadas com DuckDB direto nos arquivos Parquet).
-- Cada consulta começa com uma linha "-- name: <nome>" usada pelo script executar_consultas.py.
-- {GOLD} e {SILVER} são substituídos pelo caminho das camadas.

-- name: 01_indicadores_mensais
-- Pergunta: como evoluem vagas, preenchimento, tempo até contratar e receita mês a mês?
SELECT mes,
       vagas_publicadas,
       vagas_preenchidas,
       ROUND(100 * taxa_preenchimento, 1)  AS taxa_preenchimento_pct,
       tempo_medio_contratacao_h,
       receita_plataforma,
       vagas_gratuitas,
       ticket_medio_diaria,
       ROUND(100 * taxa_cancelamento, 1)   AS taxa_cancelamento_pct,
       ROUND(100 * taxa_no_show, 1)        AS taxa_no_show_pct,
       nota_media
FROM read_parquet('{GOLD}/kpi_mensal/*.parquet')
ORDER BY mes;

-- name: 02_funcoes_com_falta_de_freelancer
-- Pergunta: em quais funções os restaurantes mais ficam sem profissional?
SELECT funcao,
       SUM(vagas_publicadas)                                         AS vagas,
       SUM(vagas_preenchidas)                                        AS preenchidas,
       ROUND(100.0 * SUM(vagas_preenchidas) / SUM(vagas_publicadas), 1) AS taxa_preenchimento_pct,
       ROUND(AVG(tempo_medio_contratacao_h), 1)                      AS tempo_medio_contratacao_h,
       ROUND(AVG(ticket_medio_diaria), 2)                            AS diaria_media
FROM read_parquet('{GOLD}/kpi_cidade_funcao/*.parquet')
GROUP BY funcao
ORDER BY taxa_preenchimento_pct ASC;

-- name: 03_ranking_cidades
-- Pergunta: quais cidades movimentam mais a plataforma e como está a qualidade do serviço?
SELECT cidade,
       SUM(vagas_publicadas)                                         AS vagas,
       ROUND(100.0 * SUM(vagas_preenchidas) / SUM(vagas_publicadas), 1) AS taxa_preenchimento_pct,
       ROUND(SUM(receita_plataforma), 2)                             AS receita_plataforma,
       ROUND(SUM(valor_diarias_pagas), 2)                            AS valor_diarias_pagas,
       ROUND(AVG(nota_media), 2)                                     AS nota_media
FROM read_parquet('{GOLD}/kpi_cidade_funcao/*.parquet')
GROUP BY cidade
ORDER BY vagas DESC;

-- name: 04_horarios_de_pico
-- Pergunta: em que dias e horários os restaurantes mais publicam vagas?
SELECT nome_dia, hora, SUM(eventos) AS vagas_publicadas
FROM read_parquet('{GOLD}/eventos_por_hora/*.parquet')
WHERE tipo_evento = 'vaga_publicada'
GROUP BY nome_dia, dia_semana, hora
ORDER BY vagas_publicadas DESC
LIMIT 10;

-- name: 05_status_final_das_vagas
-- Pergunta: como termina cada vaga publicada?
SELECT status_final,
       COUNT(*)                                          AS vagas,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS percentual
FROM read_parquet('{GOLD}/fato_vagas/*.parquet')
GROUP BY status_final
ORDER BY vagas DESC;

-- name: 06_qualidade_dos_dados
-- Pergunta: quantos registros foram descartados na limpeza e por quê?
SELECT motivo_rejeicao, COUNT(*) AS registros
FROM read_parquet('{SILVER}/eventos_rejeitados/*.parquet')
GROUP BY motivo_rejeicao
ORDER BY registros DESC;
