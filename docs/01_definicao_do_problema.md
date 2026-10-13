# Entrega 1: Definição do Problema

**Projeto:** UmFreela Data Lakehouse
**Aluno:** Jonathan Gonçalves Possa
**Disciplina:** Introdução ao Big Data, Armazenamento e Processamento de Dados em Escala (Prof. Felipe Becker Nunes)

## 1. Tema escolhido

Pipeline de dados para a **UmFreela**, plataforma que faz a mediação entre restaurantes e profissionais freelancers de todas as funções do setor (garçom, cozinheiro, auxiliar de cozinha, bartender, recepcionista, entre outras). Na plataforma, o restaurante publica uma vaga para um turno, os freelancers se candidatam, o restaurante contrata, o turno acontece e, ao final, há avaliação e pagamento.

## 2. Problema que será resolvido

Cada ação na plataforma gera um evento (vaga publicada, candidatura, contratação, turno concluído, avaliação, pagamento, cancelamento). Esses eventos chegam de forma contínua, em grande volume e com problemas de qualidade, e hoje não existe um processo que os organize para responder perguntas básicas do negócio, como:

- Qual percentual das vagas publicadas é efetivamente preenchido?
- Quanto tempo um restaurante espera, em média, até conseguir contratar um freelancer?
- Quais funções e cidades têm mais demanda e onde faltam profissionais?
- Quanto a plataforma movimenta e fatura por mês?
- Qual a taxa de cancelamentos e de faltas (no-show)?
- Como está a satisfação, medida pelas avaliações?

O projeto constrói um pipeline que coleta esses eventos, guarda os dados brutos, limpa e padroniza, e entrega indicadores prontos em um dashboard.

## 3. Público interessado nos dados

- **Sócios e diretoria da UmFreela:** acompanhamento de faturamento e crescimento.
- **Comercial e marketing:** identificar cidades e funções com demanda alta para direcionar campanhas de captação de restaurantes e freelancers.
- **Operação e suporte:** acompanhar cancelamentos, faltas e avaliações baixas.

## 4. Fonte dos dados

- **Origem:** gerador sintético desenvolvido em Python, que simula o funcionamento da plataforma. Os dados são sintéticos para não expor dados pessoais de usuários reais (LGPD), mas seguem a lógica do negócio da UmFreela.
- **Formato:** eventos em **JSON Lines** (um evento por linha), gravados em lotes.
- **Frequência:** cada execução do gerador produz um lote que representa **um dia de operação** da plataforma. A ingestão processa automaticamente todos os lotes novos (micro-batch).
- **Volume:** 12 meses de operação simulada, com algumas centenas de milhares de eventos (o volume exato será registrado nas evidências de execução).
- **Problemas de qualidade inseridos de propósito**, para serem tratados no pipeline:
  - eventos duplicados (reenvio pelo aplicativo);
  - campos obrigatórios ausentes;
  - cidades e funções escritas de formas diferentes ("santa maria", "Santa Maria ", "SANTA MARIA");
  - datas em formatos diferentes;
  - valores inválidos (diária negativa ou zerada, nota fora da escala de 1 a 5).

## 5. Indicadores que serão produzidos

1. **Taxa de preenchimento de vagas:** vagas com contratação ÷ vagas publicadas.
2. **Tempo médio até a contratação:** horas entre a publicação da vaga e a contratação.
3. **Movimentação e receita da plataforma por mês:** valor total das diárias pagas e a parte que fica com a UmFreela.
4. **Ticket médio da diária**, por função.
5. **Taxa de cancelamento e de no-show.**
6. **Nota média das avaliações**, por função e por cidade.
7. **Ranking de funções e cidades com maior demanda.**
8. **Qualidade dos dados (monitoramento):** percentual de registros inválidos e duplicados descartados em cada execução.

## 6. Proposta inicial de arquitetura

Baseada na **Opção B do enunciado (ambiente local com processamento batch)**, adaptada para rodar no **Google Colab** sem instalar nada, e que também pode ser executada localmente com Python.

```text
Gerador Python (eventos JSON por dia)
        ↓
Pasta landing (chegada dos lotes)
        ↓
Ingestão automática em micro-batch (script Python com controle de lotes já processados)
        ↓
Bronze: JSON original + metadados de ingestão
        ↓
PySpark: deduplicação, validação, padronização e conversão de tipos
        ↓
Silver: Parquet particionado por data
        ↓
PySpark: agregações e cálculo dos indicadores
        ↓
Gold: tabelas Parquet prontas para análise
        ↓
Consultas SQL (DuckDB / Spark SQL)
        ↓
Dashboard (Looker Studio e gráficos no notebook)

Orquestração: script que executa as etapas em sequência e interrompe tudo se uma etapa falhar
Monitoramento: arquivo de log + tabela com contagens e qualidade dos dados por execução
```
