"""Gerador sintético de eventos da plataforma UmFreela.

Simula o dia a dia da plataforma: restaurantes publicam vagas, freelancers se
candidatam, há contratações, turnos concluídos, faltas, cancelamentos,
avaliações e pagamentos.

Cada execução grava um arquivo JSON Lines por dia de operação na pasta
data/generated/eventos (a "landing zone"). Só são gerados os dias que ainda não
existem, o que permite rodar o gerador várias vezes, como se novos lotes
chegassem continuamente.

Problemas de qualidade são inseridos de propósito (duplicados, campos
ausentes, nomes de cidade e função escritos de formas diferentes, datas em
formatos diferentes e valores inválidos) para serem tratados na camada Silver.

Os dados são 100% sintéticos: não há dados pessoais de usuários reais.
"""
import argparse
import csv
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[1]))
import config  # noqa: E402

# Peso de cada cidade no volume de restaurantes e oferta de freelancers
PESO_CIDADE = {"Santa Maria": 0.34, "Porto Alegre": 0.26, "Santiago": 0.10,
               "Uruguaiana": 0.11, "Itaqui": 0.07, "Quarta Colônia": 0.12}
OFERTA_CIDADE = {"Santa Maria": 4.0, "Porto Alegre": 5.0, "Santiago": 2.2,
                 "Uruguaiana": 2.5, "Itaqui": 1.6, "Quarta Colônia": 1.8}
# Peso de cada função na demanda, diária base (R$) e facilidade de encontrar profissional
FUNCAO_INFO = {
    "Garçom":              (0.32, 140, 1.10),
    "Cozinheiro":          (0.14, 190, 0.60),
    "Auxiliar de Cozinha": (0.15, 130, 0.90),
    "Bartender":           (0.09, 170, 0.70),
    "Recepcionista":       (0.07, 130, 1.00),
    "Chapeiro":            (0.08, 150, 0.75),
    "Copeiro":             (0.07, 120, 1.00),
    "Auxiliar de Limpeza": (0.08, 115, 1.05),
}
SEGMENTOS = ["Restaurante", "Bar", "Buffet", "Hotel", "Lancheria", "Pizzaria", "Churrascaria"]
CANAIS = ["app_android", "app_ios", "web"]
N_RESTAURANTES = 420
N_FREELAS = 3000
JANELA_CICLO = 12  # dias máximos entre a publicação da vaga e o último evento


def _variante_texto(rng, texto):
    """Escreve o mesmo nome de formas diferentes (problema de padronização)."""
    sem_acento = (texto.replace("ç", "c").replace("ô", "o").replace("Ç", "C"))
    opcoes = [texto.upper(), texto.lower(), f" {texto} ", sem_acento, sem_acento.lower()]
    return opcoes[rng.integers(len(opcoes))]


def _cadastros():
    """Cadastro fixo de restaurantes e freelancers (determinístico pela semente)."""
    rng = np.random.default_rng(config.SEMENTE)
    inicio = date.fromisoformat(config.DATA_INICIO)
    cidades = list(PESO_CIDADE)
    p_cid = np.array(list(PESO_CIDADE.values()))
    restaurantes = []
    for i in range(1, N_RESTAURANTES + 1):
        # 25% já eram clientes antes do início; o resto entra ao longo do ano (crescimento)
        if rng.random() < 0.25:
            cad = inicio - timedelta(days=int(rng.integers(30, 300)))
        else:
            cad = inicio + timedelta(days=int(config.DIAS_SIMULADOS * rng.power(1.4)))
        seg = SEGMENTOS[rng.integers(len(SEGMENTOS))]
        restaurantes.append({
            "id_restaurante": f"R{i:04d}",
            "nome_fantasia": f"{seg} UMF {i:03d}",
            "segmento": seg,
            "cidade": cidades[rng.choice(len(cidades), p=p_cid)],
            "data_cadastro": cad.isoformat(),
            "atividade": float(rng.gamma(2.0, 0.17)),  # vagas/dia esperadas
        })
    freelas = {c: [] for c in cidades}
    for i in range(1, N_FREELAS + 1):
        c = cidades[rng.choice(len(cidades), p=p_cid)]
        freelas[c].append(f"F{i:05d}")
    return restaurantes, freelas


def _ciclo_vagas_do_dia(dia_idx, restaurantes, freelas):
    """Gera o ciclo de vida completo das vagas publicadas em um dia."""
    rng = np.random.default_rng(config.SEMENTE * 1000 + dia_idx)
    inicio = date.fromisoformat(config.DATA_INICIO)
    dia = inicio + timedelta(days=dia_idx)
    funcoes = list(FUNCAO_INFO)
    p_fun = np.array([v[0] for v in FUNCAO_INFO.values()])
    p_fun = p_fun / p_fun.sum()
    # Sazonalidade: mais vagas na sexta/sábado e no fim de ano
    fator = {4: 1.5, 5: 1.7, 6: 1.2}.get(dia.weekday(), 0.85)
    if dia.month == 12:
        fator *= 1.45
    elif dia.month in (1, 2):
        fator *= 0.85
    eventos, cont = [], 0
    for r in restaurantes:
        if date.fromisoformat(r["data_cadastro"]) > dia:
            continue
        for _ in range(rng.poisson(r["atividade"] * fator)):
            cont += 1
            id_vaga = f"V{dia:%Y%m%d}{cont:04d}"
            fun = funcoes[rng.choice(len(funcoes), p=p_fun)]
            _, base, facilidade = FUNCAO_INFO[fun]
            diaria = round(base * rng.uniform(0.85, 1.25) / 5) * 5
            pub = datetime.combine(dia, datetime.min.time()) + timedelta(
                hours=float(rng.uniform(8, 23)))
            turno = datetime.combine(dia + timedelta(days=int(rng.integers(1, 7))),
                                     datetime.min.time()) + timedelta(hours=int(rng.choice([11, 12, 18, 19, 20])))
            comum = {"id_vaga": id_vaga, "id_restaurante": r["id_restaurante"],
                     "cidade": r["cidade"], "funcao": fun}
            canal = CANAIS[rng.integers(3)]

            def ev(tipo, ts, **extra):
                eventos.append({"event_id": f"E{rng.integers(1e15):015d}", "tipo_evento": tipo,
                                "event_ts": ts, **comum, "canal": canal, **extra})

            ev("vaga_publicada", pub, valor_diaria=float(diaria), data_turno=turno)
            n_cand = rng.poisson(OFERTA_CIDADE[r["cidade"]] * facilidade)
            pool = freelas[r["cidade"]]
            cands = [pool[i] for i in rng.choice(len(pool), size=min(n_cand, len(pool)), replace=False)]
            t = pub
            for f in cands:
                t = t + timedelta(hours=float(rng.exponential(3)))
                if t >= turno:
                    break
                ev("candidatura", t, id_freela=f)
            if not cands or rng.random() > 0.9:
                continue  # vaga não preenchida
            contrato = pub + timedelta(hours=float(rng.exponential(5) + rng.exponential(4)))
            if contrato >= turno - timedelta(hours=2):
                continue
            freela = cands[0]
            ev("contratacao", contrato, id_freela=freela, valor_diaria=float(diaria))
            sorte = rng.random()
            if sorte < 0.06:
                quem = "restaurante" if rng.random() < 0.55 else "freelancer"
                ev("cancelamento", contrato + (turno - contrato) * float(rng.uniform(0.2, 0.95)),
                   id_freela=freela, cancelado_por=quem)
                continue
            if sorte < 0.10:
                ev("no_show", turno + timedelta(minutes=30), id_freela=freela)
                continue
            fim = turno + timedelta(hours=int(rng.integers(5, 9)))
            ev("turno_concluido", fim, id_freela=freela)
            ev("pagamento", fim + timedelta(hours=float(rng.uniform(1, 30))), id_freela=freela,
               valor_diaria=float(diaria))
            if rng.random() < 0.8:
                nota = int(rng.choice([5, 4, 3, 2, 1], p=[0.55, 0.27, 0.10, 0.05, 0.03]))
                ev("avaliacao", fim + timedelta(hours=float(rng.uniform(1, 48))),
                   id_freela=freela, nota=nota)
    return eventos


def _sujar(rng, eventos):
    """Insere problemas de qualidade de propósito."""
    saida = []
    for e in eventos:
        e = dict(e)
        ts = e["event_ts"]
        r = rng.random()
        if r < 0.10:
            e["event_ts"] = ts.strftime("%d/%m/%Y %H:%M:%S")          # formato brasileiro
        elif r < 0.13:
            e["event_ts"] = str(int(ts.replace(tzinfo=timezone.utc).timestamp() * 1000))  # epoch em ms (UTC)
        else:
            e["event_ts"] = ts.strftime("%Y-%m-%dT%H:%M:%S")
        if "data_turno" in e:
            e["data_turno"] = e["data_turno"].strftime("%Y-%m-%dT%H:%M:%S")
        if rng.random() < 0.15:
            e["cidade"] = _variante_texto(rng, e["cidade"])
        if rng.random() < 0.15:
            e["funcao"] = _variante_texto(rng, e["funcao"])
        if e["tipo_evento"] == "vaga_publicada" and rng.random() < 0.006:
            e["valor_diaria"] = float(rng.choice([0, -150, -1]))
        if e["tipo_evento"] == "avaliacao" and rng.random() < 0.02:
            e["nota"] = int(rng.choice([0, 6, 10]))
        if rng.random() < 0.008:
            e.pop(rng.choice(["id_vaga", "event_ts", "tipo_evento"]), None)
        saida.append(e)
        if rng.random() < 0.02:                                       # reenvio do app
            saida.append(dict(e))
    rng.shuffle(saida)
    return saida


def gravar_cadastro_restaurantes(restaurantes):
    rng = np.random.default_rng(config.SEMENTE + 7)
    caminho = config.LANDING_CADASTROS / "restaurantes.csv"
    with open(caminho, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id_restaurante", "nome_fantasia", "segmento",
                                          "cidade", "data_cadastro"])
        w.writeheader()
        for r in restaurantes:
            cidade = _variante_texto(rng, r["cidade"]) if rng.random() < 0.15 else r["cidade"]
            w.writerow({k: (cidade if k == "cidade" else r[k]) for k in w.fieldnames})
    return caminho


def gerar(dias=None):
    """Gera os lotes diários que ainda não existem. Retorna a lista de arquivos novos."""
    config.garantir_pastas()
    restaurantes, freelas = _cadastros()
    gravar_cadastro_restaurantes(restaurantes)
    total = config.DIAS_SIMULADOS if dias is None else min(dias, config.DIAS_SIMULADOS)
    inicio = date.fromisoformat(config.DATA_INICIO)
    novos, cache = [], {}
    for d in range(total):
        dia = inicio + timedelta(days=d)
        arq = config.LANDING_EVENTOS / f"eventos_{dia:%Y-%m-%d}.jsonl"
        if arq.exists():
            continue
        # eventos que acontecem neste dia vêm de vagas publicadas nos últimos dias
        do_dia = []
        for origem in range(max(0, d - JANELA_CICLO), d + 1):
            if origem not in cache:
                cache[origem] = _ciclo_vagas_do_dia(origem, restaurantes, freelas)
            do_dia += [e for e in cache[origem] if e["event_ts"].date() == dia]
        cache.pop(d - JANELA_CICLO, None)
        sujos = _sujar(np.random.default_rng(config.SEMENTE * 7 + d), do_dia)
        with open(arq, "w", encoding="utf-8") as f:
            for e in sujos:
                f.write(json.dumps(e, ensure_ascii=False) + "\n")
        novos.append(arq)
    return novos


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Gera lotes diários de eventos da UmFreela")
    ap.add_argument("--dias", type=int, default=None,
                    help="quantos dias (a partir do início) devem existir na landing; padrão: todos")
    a = ap.parse_args()
    arquivos = gerar(a.dias)
    print(f"{len(arquivos)} novos lotes gerados em {config.LANDING_EVENTOS}")
