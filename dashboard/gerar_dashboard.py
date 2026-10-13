"""Gera o dashboard da UmFreela a partir da camada Gold.

Saídas:
- dashboard/dashboard_umfreela.png: painel com indicadores e gráficos;
- dashboard/dados/*.csv: tabelas da Gold em CSV, prontas para importar no
  Looker Studio (ou Power BI / Metabase), caso se queira um dashboard interativo.
"""
import sys
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

sys.path.append(str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402

AZUL = "#2a78d6"
TEXTO = "#0b0b0b"
TEXTO_2 = "#52514e"
GRADE = "#e4e3df"
FUNDO = "#fcfcfb"
SAIDA_PNG = Path(__file__).with_name("dashboard_umfreela.png")


def _q(con, sql):
    return con.execute(sql.replace("{GOLD}", str(config.GOLD_DIR))).df()


def _estilo(ax, titulo):
    ax.set_title(titulo, loc="left", fontsize=11, color=TEXTO, fontweight="bold", pad=10)
    ax.set_facecolor(FUNDO)
    for s in ["top", "right", "left"]:
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(GRADE)
    ax.tick_params(colors=TEXTO_2, labelsize=8, length=0)
    ax.grid(axis="x", color=GRADE, linewidth=0.8)
    ax.set_axisbelow(True)


def gerar(log=print):
    config.garantir_pastas()
    con = duckdb.connect()
    for tabela in ["kpi_mensal", "kpi_cidade_funcao", "fato_vagas", "eventos_por_hora"]:
        _q(con, f"SELECT * FROM read_parquet('{{GOLD}}/{tabela}/*.parquet')").to_csv(
            config.DASHBOARD_DADOS / f"{tabela}.csv", index=False)

    mensal = _q(con, "SELECT * FROM read_parquet('{GOLD}/kpi_mensal/*.parquet') ORDER BY mes")
    tot = _q(con, """SELECT COUNT(*) vagas, AVG(preenchida::INT) taxa,
                            AVG(horas_ate_contratacao) horas, SUM(valor_cobrado) receita
                     FROM read_parquet('{GOLD}/fato_vagas/*.parquet')""").iloc[0]
    funcao = _q(con, """SELECT funcao, SUM(vagas_preenchidas)/SUM(vagas_publicadas) taxa
                        FROM read_parquet('{GOLD}/kpi_cidade_funcao/*.parquet')
                        GROUP BY funcao ORDER BY taxa""")
    cidade = _q(con, """SELECT cidade, SUM(vagas_publicadas) vagas
                        FROM read_parquet('{GOLD}/kpi_cidade_funcao/*.parquet')
                        GROUP BY cidade ORDER BY vagas""")
    hora = _q(con, """SELECT dia_semana, hora, SUM(eventos) n
                      FROM read_parquet('{GOLD}/eventos_por_hora/*.parquet')
                      WHERE tipo_evento = 'vaga_publicada' GROUP BY 1, 2""")
    status = _q(con, """SELECT status_final, COUNT(*) n FROM read_parquet('{GOLD}/fato_vagas/*.parquet')
                        GROUP BY 1 ORDER BY n""")

    fig = plt.figure(figsize=(16, 12), facecolor=FUNDO)
    gs = fig.add_gridspec(4, 3, height_ratios=[0.45, 1, 1, 1], hspace=0.55, wspace=0.35)
    fig.suptitle("UmFreela · Painel de Vagas e Receita (dados sintéticos)", x=0.06, ha="left",
                 fontsize=17, fontweight="bold", color=TEXTO)

    tiles = [("Vagas publicadas", f"{int(tot.vagas):,}".replace(",", ".")),
             ("Taxa de preenchimento", f"{tot.taxa * 100:.1f}%".replace(".", ",")),
             ("Tempo médio até contratar", f"{tot.horas:.1f} h".replace(".", ",")),
             ("Receita da plataforma", f"R$ {tot.receita:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))]
    sub = gs[0, :].subgridspec(1, 4, wspace=0.15)
    for i, (rot, val) in enumerate(tiles):
        ax = fig.add_subplot(sub[0, i])
        ax.axis("off")
        ax.text(0, 0.75, rot, fontsize=10, color=TEXTO_2, transform=ax.transAxes)
        ax.text(0, 0.1, val, fontsize=22, fontweight="bold", color=TEXTO, transform=ax.transAxes)

    ax = fig.add_subplot(gs[1, :2])
    _estilo(ax, "Receita da plataforma por mês (R$ 3,99 por vaga, 15 dias grátis)")
    ax.bar(mensal["mes"], mensal["receita_plataforma"], color=AZUL, width=0.7)
    ax.grid(axis="x", visible=False)
    ax.grid(axis="y", color=GRADE, linewidth=0.8)
    ax.tick_params(axis="x", rotation=45)
    for x, y in zip(mensal["mes"].iloc[[-1]], mensal["receita_plataforma"].iloc[[-1]]):
        ax.annotate(f"R$ {y:,.0f}".replace(",", "."), (x, y), textcoords="offset points",
                    xytext=(0, 4), ha="center", fontsize=8, color=TEXTO)

    ax = fig.add_subplot(gs[1, 2])
    _estilo(ax, "Como terminam as vagas")
    rot = {"concluida": "Concluída", "nao_preenchida": "Não preenchida", "cancelada": "Cancelada",
           "no_show": "Freela faltou", "contratada_aguardando_turno": "Aguardando turno"}
    ax.barh([rot.get(s, s) for s in status["status_final"]], status["n"], color=AZUL, height=0.6)
    for y, v in enumerate(status["n"]):
        ax.text(v, y, f" {v / status['n'].sum() * 100:.0f}%", va="center", fontsize=8, color=TEXTO_2)

    ax = fig.add_subplot(gs[2, 0])
    _estilo(ax, "Taxa de preenchimento por função")
    ax.barh(funcao["funcao"], funcao["taxa"] * 100, color=AZUL, height=0.6)
    ax.set_xlim(0, 100)
    for y, v in enumerate(funcao["taxa"] * 100):
        ax.text(v, y, f" {v:.0f}%", va="center", fontsize=8, color=TEXTO_2)

    ax = fig.add_subplot(gs[2, 1])
    _estilo(ax, "Vagas publicadas por cidade")
    ax.barh(cidade["cidade"], cidade["vagas"], color=AZUL, height=0.6)
    for y, v in enumerate(cidade["vagas"]):
        ax.text(v, y, f" {int(v):,}".replace(",", "."), va="center", fontsize=8, color=TEXTO_2)

    ax = fig.add_subplot(gs[2, 2])
    _estilo(ax, "Tempo médio até contratar (horas)")
    ax.plot(mensal["mes"], mensal["tempo_medio_contratacao_h"], color=AZUL, linewidth=2,
            marker="o", markersize=5)
    ax.set_ylim(0, max(mensal["tempo_medio_contratacao_h"]) * 1.3)
    ax.grid(axis="y", color=GRADE, linewidth=0.8)
    ax.tick_params(axis="x", rotation=45)

    ax = fig.add_subplot(gs[3, :])
    _estilo(ax, "Quando os restaurantes publicam vagas (dia da semana x hora)")
    mat = np.zeros((7, 24))
    for _, r in hora.iterrows():
        mat[int(r.dia_semana) - 1, int(r.hora)] = r.n
    im = ax.imshow(mat, aspect="auto", cmap="Blues")
    ax.set_yticks(range(7), ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sáb"])
    ax.set_xticks(range(24), [f"{h}h" for h in range(24)])
    ax.grid(False)
    cb = fig.colorbar(im, ax=ax, fraction=0.02, pad=0.01)
    cb.ax.tick_params(labelsize=8, colors=TEXTO_2)
    cb.outline.set_visible(False)

    fig.savefig(SAIDA_PNG, dpi=120, facecolor=FUNDO, bbox_inches="tight")
    plt.close(fig)
    log(f"Dashboard salvo em {SAIDA_PNG} e CSVs em {config.DASHBOARD_DADOS}")
    return SAIDA_PNG


if __name__ == "__main__":
    gerar()
