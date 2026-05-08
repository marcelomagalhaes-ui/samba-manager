"""
scripts/ingest_samba_limpo.py
==============================
Ingere dados estruturados do projeto Samba_Limpo (Pricing Builder) na
CorporateKnowledge do SAMBA_AGENTS.

Fontes:
  1. historico_basis_portos_corn_soy.csv  → basis histórico soja/milho por porto
  2. historico_basis_portos_acucar.csv    → basis histórico açúcar por porto
  3. tb_portos_internacionais_master.csv  → portos internacionais (82 portos)
  4. portos_fluviais.csv                 → portos/terminais fluviais (24 terminais)
  5. tb_rotas_maritimas.csv              → 1.839 rotas marítimas por origem
  6. usinas_icumsa.csv                   → 126 usinas de açúcar
  7. services/pricing_builder_service.py → lógica de pricing FOB
  8. services/multimodal_router.py       → roteamento multimodal

Estratégia:
  - Dados estruturados → converter para texto legível e embeddável
  - Sem chamada Gemini para geração (só embeddings) → ultra-barato
  - Um chunk por "unidade lógica" (porto, grupo de rotas, grupo de usinas, etc.)

Uso:
    python scripts/ingest_samba_limpo.py
    python scripts/ingest_samba_limpo.py --dry-run   # mostra chunks sem salvar
    python scripts/ingest_samba_limpo.py --source basis     # só basis
    python scripts/ingest_samba_limpo.py --source ports     # só portos intl
    python scripts/ingest_samba_limpo.py --source fluvial   # só portos fluviais
    python scripts/ingest_samba_limpo.py --source routes    # só rotas marítimas
    python scripts/ingest_samba_limpo.py --source usinas    # só usinas açúcar
    python scripts/ingest_samba_limpo.py --source services  # só código serviços
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import struct
import sys
import time
import threading
from datetime import datetime
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(override=True)

from google import genai

GEMINI_KEY   = os.getenv("GEMINI_API_KEY", "")
MODEL_EMB    = "models/gemini-embedding-001"   # 3072 dims, float32
RPM_LIMIT    = 80
SAMBA_LIMPO  = Path(r"C:\Samba_Limpo")

client = genai.Client(api_key=GEMINI_KEY)

# ── Rate limiter ──────────────────────────────────────────────────────────────
_rpm_lock  = threading.Lock()
_rpm_calls: list[float] = []

def _rpm_wait():
    with _rpm_lock:
        now = time.monotonic()
        _rpm_calls[:] = [t for t in _rpm_calls if now - t < 60.0]
        if len(_rpm_calls) >= RPM_LIMIT:
            wait = 60.0 - (now - _rpm_calls[0]) + 1.0
            print(f"  [rpm] aguardando {wait:.0f}s...", flush=True)
            time.sleep(max(wait, 0))
            _rpm_calls[:] = [t for t in _rpm_calls if time.monotonic() - t < 60.0]
        _rpm_calls.append(time.monotonic())

# ── Embedding ─────────────────────────────────────────────────────────────────
def embed_text(text: str) -> bytes:
    _rpm_wait()
    result = client.models.embed_content(model=MODEL_EMB, contents=text[:8000])
    floats = result.embeddings[0].values
    return struct.pack(f"{len(floats)}f", *floats)

# ── DB save ───────────────────────────────────────────────────────────────────
def save_chunk(doc_name: str, content: str, dry_run: bool = False) -> bool:
    if dry_run:
        print(f"  [DRY] {doc_name[:70]}")
        preview = content[:120].replace(chr(10),' ').encode('ascii', errors='replace').decode('ascii')
        print(f"        {len(content)} chars | preview: {preview}")
        return True
    from models.database import get_session, CorporateKnowledge
    try:
        emb = embed_text(content)
    except Exception as e:
        print(f"  [EMB ERRO] {doc_name[:60]}: {e}")
        return False
    sess = get_session()
    try:
        existing = sess.query(CorporateKnowledge)\
            .filter(CorporateKnowledge.document_name == doc_name).first()
        if existing:
            existing.content   = content
            existing.embedding = emb
            action = "UPDATE"
        else:
            sess.add(CorporateKnowledge(
                document_name = doc_name,
                content       = content,
                embedding     = emb,
            ))
            action = "INSERT"
        sess.commit()
        print(f"  [{action}] {doc_name[:70]}")
        return True
    except Exception as e:
        sess.rollback()
        print(f"  [DB ERRO] {doc_name[:60]}: {e}")
        return False
    finally:
        sess.close()

# ─────────────────────────────────────────────────────────────────────────────
# 1. BASIS HISTÓRICO — SOJA / MILHO / AÇÚCAR
# ─────────────────────────────────────────────────────────────────────────────
def ingest_basis(dry_run=False):
    """
    Lê os CSVs de basis histórico e cria um chunk por (commodity, porto),
    resumindo min/max/média/último valor com datas.
    """
    print("\n=== [1/6] BASIS HISTÓRICO ===")
    files = {
        "SOY_CORN": SAMBA_LIMPO / "historico_basis_portos_corn_soy.csv",
        "SUGAR":    SAMBA_LIMPO / "historico_basis_portos_acucar.csv",
    }
    # Agrega: {(commodity, porto): {basis_values, contratos, safras, min_date, max_date}}
    data: dict[tuple, dict] = defaultdict(lambda: {
        "values": [], "contratos": set(), "safras": set(),
        "dates": [], "fonte": ""
    })

    for label, path in files.items():
        if not path.exists():
            print(f"  [SKIP] {path} não encontrado")
            continue
        with open(path, encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                commodity = row.get("commodity", "").strip()
                porto     = row.get("porto_code", "").strip()
                try:
                    val = float(row.get("basis_usd_mt", "0").strip())
                except ValueError:
                    continue
                key = (commodity, porto)
                data[key]["values"].append(val)
                data[key]["contratos"].add(row.get("contrato_ref", "").strip())
                data[key]["safras"].add(row.get("safra", "").strip())
                data[key]["dates"].append(row.get("timestamp", "").strip()[:10])
                data[key]["fonte"] = row.get("fonte", "").strip()

    ok = errors = 0
    for (commodity, porto), d in sorted(data.items()):
        vals  = d["values"]
        dates = sorted(d["dates"])
        min_v, max_v, avg_v = min(vals), max(vals), sum(vals)/len(vals)
        last_v = vals[-1]
        contratos = ", ".join(sorted(d["contratos"]))
        safras    = ", ".join(sorted(d["safras"]))
        date_range = f"{dates[0]} a {dates[-1]}" if dates else "N/D"
        n = len(vals)

        content = f"""BASIS HISTÓRICO — {commodity} — PORTO {porto}
Commodity: {commodity}
Porto de Referência: {porto} (FOB)
Período de Dados: {date_range} ({n} registros)
Safras Cobertas: {safras}
Contratos de Referência: {contratos}
Fonte: {d['fonte']}

ESTATÍSTICAS DE BASIS (USD/MT):
  Mínimo registrado: USD {min_v:.2f}/MT
  Máximo registrado: USD {max_v:.2f}/MT
  Média histórica:   USD {avg_v:.2f}/MT
  Último valor:      USD {last_v:.2f}/MT

INTERPRETAÇÃO:
O basis FOB {porto} para {commodity} representa o diferencial entre o preço
físico no porto e o contrato futuro de referência ({contratos}).
Um basis positivo indica prêmio do mercado físico brasileiro; negativo indica
desconto. A amplitude de USD {(max_v - min_v):.2f}/MT registrada reflete variações
sazonais, logística, demanda China/exterior e disponibilidade de navios.

Uso típico: ao negociar um contrato FOB {porto}, o trader referencia o futuro
CBOT/ICE no contrato {contratos} e adiciona/subtrai o basis histórico para
formar o preço físico.
"""
        doc_name = f"SAMBA_BASIS_{commodity}_{porto}"
        if save_chunk(doc_name, content, dry_run):
            ok += 1
        else:
            errors += 1

    print(f"  Basis: {ok} chunks gerados | {errors} erros")
    return ok

# ─────────────────────────────────────────────────────────────────────────────
# 2. PORTOS INTERNACIONAIS
# ─────────────────────────────────────────────────────────────────────────────
def ingest_ports_international(dry_run=False):
    print("\n=== [2/6] PORTOS INTERNACIONAIS ===")
    path = SAMBA_LIMPO / "tb_portos_internacionais_master.csv"
    if not path.exists():
        print(f"  [SKIP] {path}")
        return 0

    ok = errors = 0
    with open(path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code    = row.get("PORTO_CODE", "").strip()
            regiao  = row.get("REGIAO", "").strip()
            pais    = row.get("PAIS", "").strip()
            cidade  = row.get("CIDADE", "").strip()
            lat     = row.get("LATITUDE", "").strip()
            lon     = row.get("LONGITUDE", "").strip()
            calado  = row.get("CALADO_MAX_M", "").strip()
            navio   = row.get("CLASSE_NAVIO_IDEAL", "").strip()
            disch   = row.get("DISCHARGE_RATE_TPD", "").strip()
            disb    = row.get("EST_PORT_DISBURSEMENTS_USD", "").strip()
            dwell   = row.get("DWELL_TIME_DAYS", "").strip()
            risco   = row.get("RISCO_GEOPOLITICO", "").strip()
            choke   = row.get("CHOKEPOINT_PREFERENCIAL", "").strip()
            ftz     = row.get("IS_FREE_TRADE_ZONE", "").strip()
            ativo   = row.get("ATIVO", "TRUE").strip()
            if ativo.upper() != "TRUE":
                continue

            content = f"""PORTO INTERNACIONAL — {cidade}, {pais} ({code})
Código: {code}
Região: {regiao} | País: {pais} | Cidade: {cidade}
Coordenadas: {lat}°N, {lon}°E

ESPECIFICAÇÕES TÉCNICAS:
  Calado máximo: {calado} metros
  Classe de navio ideal: {navio}
  Taxa de descarga: {disch} toneladas/dia
  Port disbursements estimados: USD {disb}
  Tempo médio de dwell (permanência): {dwell} dias
  Free Trade Zone: {ftz}

RISCO E LOGÍSTICA:
  Risco geopolítico: {risco}
  Chokepoint preferencial: {choke}

CONTEXTO:
Porto {cidade} ({code}) é um destino relevante para exportações brasileiras
de commodities agrícolas. Com calado de {calado}m aceita navios tipo {navio},
processando até {disch} t/dia. Disbursements de USD {disb} são custos estimados
de escala. Tempo de dwell de {dwell} dias deve ser considerado no planejamento
de capital de giro do comprador.
Rota marítima do Brasil passa pelo {choke}.
"""
            doc_name = f"SAMBA_PORT_INTL_{code}"
            if save_chunk(doc_name, content, dry_run):
                ok += 1
            else:
                errors += 1

    print(f"  Portos internacionais: {ok} inseridos | {errors} erros")
    return ok

# ─────────────────────────────────────────────────────────────────────────────
# 3. PORTOS / TERMINAIS FLUVIAIS
# ─────────────────────────────────────────────────────────────────────────────
def ingest_ports_fluvial(dry_run=False):
    print("\n=== [3/6] PORTOS E TERMINAIS FLUVIAIS ===")
    path = SAMBA_LIMPO / "portos_fluviais.csv"
    if not path.exists():
        print(f"  [SKIP] {path}")
        return 0

    ok = errors = 0
    with open(path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code    = row.get("port_code", "").strip()
            name    = row.get("port_name", "").strip()
            state   = row.get("state_uf", "").strip()
            desc    = row.get("overview_desc", "").strip()
            lat     = row.get("latitude", "").strip()
            lon     = row.get("longitude", "").strip()
            draft   = row.get("max_draft_m", "").strip()
            loa     = row.get("max_loa_m", "").strip()
            bunker  = row.get("has_bunker", "").strip()
            water   = row.get("has_fresh_water", "").strip()
            tide    = row.get("tide_variation_m", "").strip()
            modal_a = row.get("modal_acesso", "").strip()
            modal_e = row.get("modal_escoamento", "").strip()
            taxa_tb = row.get("transbordo_taxa_brl_ton", "").strip()
            prancha = row.get("prancha_carregamento_tpd", "").strip()
            pilotage= row.get("towage_pilotage_rules", "").strip()
            weather = row.get("weather_restrictions", "").strip()
            vhf     = row.get("vhf_channels", "").strip()

            # Parse JSON berths se disponível
            berths_raw = row.get("terminals_berths_json", "[]").strip()
            berths_str = ""
            try:
                berths = json.loads(berths_raw)
                for b in berths:
                    berths_str += f"  Berço {b.get('berth','?')}: draft {b.get('draft_m','?')}m, "
                    berths_str += f"LOA {b.get('loa_m','?')}m, carga: {b.get('cargo','?')}\n"
                    if b.get("rate_tpd") or b.get("storage"):
                        berths_str += f"    Taxa: {b.get('rate_tpd','')}, Estocagem: {b.get('storage','')}\n"
            except Exception:
                berths_str = "  (dados de berços não disponíveis)\n"

            content = f"""PORTO/TERMINAL FLUVIAL — {name} ({code}) — {state}
Nome: {name}
Código: {code} | Estado: {state}
Coordenadas: {lat}°N, {lon}°E

DESCRIÇÃO:
{desc}

ESPECIFICAÇÕES TÉCNICAS:
  Calado máximo: {draft} metros
  LOA máximo: {loa} metros
  Variação de maré: {tide} metros
  Prancha de carregamento: {prancha} t/dia

BERÇOS/TERMINAIS:
{berths_str}
MODAL:
  Acesso: {modal_a}
  Escoamento oceânico: {modal_e}
  Taxa de transbordo: R$ {taxa_tb}/tonelada

SERVIÇOS:
  Bunker: {bunker} | Água doce: {water}
  Pilotagem/Rebocagem: {pilotage}
  Restrições climáticas: {weather}
  Canais VHF: {vhf}
"""
            doc_name = f"SAMBA_PORT_FLUV_{code}"
            if save_chunk(doc_name, content, dry_run):
                ok += 1
            else:
                errors += 1

    print(f"  Terminais fluviais: {ok} inseridos | {errors} erros")
    return ok

# ─────────────────────────────────────────────────────────────────────────────
# 4. ROTAS MARÍTIMAS (agrupadas por porto origem)
# ─────────────────────────────────────────────────────────────────────────────
def ingest_maritime_routes(dry_run=False):
    print("\n=== [4/6] ROTAS MARÍTIMAS ===")
    path = SAMBA_LIMPO / "tb_rotas_maritimas.csv"
    if not path.exists():
        # Tentar arquivo alternativo
        path = SAMBA_LIMPO / "novas_rotas_maritimas.csv"
        if not path.exists():
            print(f"  [SKIP] Nenhum CSV de rotas encontrado")
            return 0

    # Agrupa por porto de origem
    routes_by_origin: dict[str, list[dict]] = defaultdict(list)
    with open(path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ativo = row.get("ATIVO", "TRUE").strip()
            if ativo.upper() != "TRUE":
                continue
            origem = row.get("PORTO_ORIGEM_BR", "").strip()
            if not origem:
                continue
            routes_by_origin[origem].append(row)

    ok = errors = 0
    for origem, routes in sorted(routes_by_origin.items()):
        # Agrupa destinos por zona/região
        by_zone: dict[str, list] = defaultdict(list)
        for r in routes:
            zona = r.get("ZONA_MARITIMA", "").strip() or "OUTROS"
            by_zone[zona].append(r)

        lines = [f"ROTAS MARÍTIMAS — PORTO ORIGEM: {origem}",
                 f"Total de rotas ativas: {len(routes)}",
                 ""]

        for zona, zona_routes in sorted(by_zone.items()):
            lines.append(f"ZONA {zona} ({len(zona_routes)} rotas):")
            for r in sorted(zona_routes, key=lambda x: float(x.get("DISTANCIA_NM","0") or 0)):
                dest   = r.get("PORTO_DESTINO_CODE", "?")
                canal  = r.get("CANAL_ROTA", "?")
                dist   = r.get("DISTANCIA_NM", "?")
                dias   = r.get("TEMPO_DIAS_12NOS", "?")
                risco  = r.get("RISCO", "?")
                try:
                    dias_f = f"{float(dias):.1f}"
                except Exception:
                    dias_f = dias
                lines.append(f"  {origem} → {dest} | {canal} | {dist} NM | {dias_f} dias | risco {risco}")
            lines.append("")

        lines.append(f"""CONTEXTO:
Porto {origem} conecta-se a {len(routes)} destinos internacionais ativos.
Principais zonas de destino: {', '.join(sorted(by_zone.keys()))}.
A rota mais longa é de {max(float(r.get('DISTANCIA_NM','0') or 0) for r in routes):.0f} NM.
Tempo mínimo de trânsito: {min(float(r.get('TEMPO_DIAS_12NOS','99') or 99) for r in routes):.1f} dias (a 12 nós).
""")
        content = "\n".join(lines)
        doc_name = f"SAMBA_ROUTES_{origem}"
        if save_chunk(doc_name, content, dry_run):
            ok += 1
        else:
            errors += 1

    print(f"  Rotas marítimas: {ok} chunks (por porto origem) | {errors} erros")
    return ok

# ─────────────────────────────────────────────────────────────────────────────
# 5. USINAS DE AÇÚCAR
# ─────────────────────────────────────────────────────────────────────────────
def ingest_usinas(dry_run=False):
    print("\n=== [5/6] USINAS DE AÇÚCAR ===")
    path = SAMBA_LIMPO / "usinas_icumsa.csv"
    if not path.exists():
        path = SAMBA_LIMPO / "tb_usina_acucar.csv"
        if not path.exists():
            print(f"  [SKIP] Nenhum CSV de usinas encontrado")
            return 0

    by_group: dict[str, list[dict]] = defaultdict(list)
    total_cap = 0.0
    with open(path, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            group = row.get("GRUPO", row.get("grupo", "?")).strip()
            by_group[group].append(row)
            try:
                cap = float(row.get("CAPACIDADE_MMT", row.get("capacidade_mmt", "0")) or 0)
                total_cap += cap
            except ValueError:
                pass

    ok = errors = 0
    for group, usinas in sorted(by_group.items()):
        cap_group = sum(
            float(u.get("CAPACIDADE_MMT", u.get("capacidade_mmt","0")) or 0)
            for u in usinas
        )
        estados = sorted({u.get("UF", u.get("uf","?")).strip() for u in usinas})
        products = sorted({
            p.strip()
            for u in usinas
            for p in (u.get("PRODUTOS", u.get("produtos","")).split("|"))
            if p.strip()
        })

        lines = [
            f"GRUPO SUCROENERGÉTICO — {group}",
            f"Número de unidades: {len(usinas)}",
            f"Capacidade total do grupo: {cap_group:.1f} MMT/safra",
            f"Estados de atuação: {', '.join(estados)}",
            f"Produtos: {', '.join(products)}",
            "",
            "UNIDADES:",
        ]
        for u in sorted(usinas, key=lambda x: x.get("CIDADE", x.get("cidade",""))):
            nome   = u.get("UNIDADE", u.get("unidade", "?")).strip()
            uf     = u.get("UF", u.get("uf","?")).strip()
            cidade = u.get("CIDADE", u.get("cidade","?")).strip()
            cap    = u.get("CAPACIDADE_MMT", u.get("capacidade_mmt","?")).strip()
            prods  = u.get("PRODUTOS", u.get("produtos","?")).strip()
            lines.append(f"  {nome} — {cidade}/{uf} | {cap} MMT | {prods}")

        lines.append("")
        lines.append(f"""CONTEXTO:
O grupo {group} é um dos principais produtores sucroenergéticos do Brasil,
com {len(usinas)} unidades industriais e capacidade de {cap_group:.1f} MMT/safra.
Produce {' e '.join(products)}.
Concentrado em {', '.join(estados)}.
Ao negociar açúcar VHP ou ICUMSA 45 com origem Brasil, este grupo representa
potencial de fornecimento de {cap_group:.1f} MMT/ano no mercado exportador.
""")
        content = "\n".join(lines)
        doc_name = f"SAMBA_USINA_{group.replace(' ','_')[:50]}"
        if save_chunk(doc_name, content, dry_run):
            ok += 1
        else:
            errors += 1

    print(f"  Usinas: {ok} grupos inseridos | {errors} erros")
    print(f"  Capacidade total mapeada: {total_cap:.1f} MMT/safra")
    return ok

# ─────────────────────────────────────────────────────────────────────────────
# 6. SERVIÇOS PYTHON — lógica de pricing e roteamento
# ─────────────────────────────────────────────────────────────────────────────
SERVICES_TO_INGEST = [
    ("pricing_builder_service.py", "Lógica de Pricing FOB — Cálculo de Netback e Composição de Preço"),
    ("multimodal_router.py",       "Roteamento Multimodal — Otimização Road/Rail/Waterway"),
    ("freight_service.py",         "Cálculo de Frete Rodoviário — ANTT e Tarifas Dinâmicas"),
    ("port_service.py",            "Operações Portuárias — Capacidade, THC e Gestão de Escala"),
    ("maritime_router.py",         "Roteamento Marítimo — Seleção de Porto e Rota Internacional"),
    ("inland_waterway_service.py", "Hidrovias — Roteamento Fluvial e Custo de Barcaças"),
    ("tax_engine.py",              "Motor de Impostos — Drawback, ICMS, PIS/COFINS Exportação"),
    ("opportunity_engine.py",      "Motor de Oportunidades — Identificação de Arbitragens e Negócios"),
]

def ingest_services(dry_run=False):
    print("\n=== [6/6] SERVIÇOS PYTHON (LÓGICA DE NEGÓCIO) ===")
    services_dir = SAMBA_LIMPO / "services"
    if not services_dir.exists():
        print(f"  [SKIP] {services_dir} não encontrado")
        return 0

    ok = errors = 0
    for filename, title in SERVICES_TO_INGEST:
        filepath = services_dir / filename
        if not filepath.exists():
            print(f"  [SKIP] {filepath}")
            continue
        try:
            code = filepath.read_text(encoding="utf-8", errors="replace")
        except Exception as e:
            print(f"  [ERRO leitura] {filename}: {e}")
            errors += 1
            continue

        # Limita a 6000 chars para não explodir o embedding
        code_preview = code[:6000]
        if len(code) > 6000:
            code_preview += f"\n\n... [{len(code)-6000} caracteres adicionais omitidos] ..."

        content = f"""CÓDIGO DE NEGÓCIO — {title}
Arquivo: services/{filename}
Projeto: SAMBA Pricing Builder (Plataforma Estratégica Agro Global)
Tamanho total: {len(code)} caracteres

DESCRIÇÃO:
{title}

CÓDIGO FONTE (extrato):
```python
{code_preview}
```

CONTEXTO:
Este módulo faz parte do motor de precificação e roteamento logístico do SAMBA.
Encapsula lógica de negócio para traders de commodities agrícolas brasileiras.
Referência técnica para entender como preços FOB, CIF, fretes multimodais
e oportunidades de arbitragem são calculados no sistema.
"""
        doc_name = f"SAMBA_SERVICE_{filename.replace('.py','').replace('_','-').upper()}"
        if save_chunk(doc_name, content, dry_run):
            ok += 1
        else:
            errors += 1

    print(f"  Serviços: {ok} inseridos | {errors} erros")
    return ok

# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Ingere dados do Samba_Limpo na CorporateKnowledge")
    parser.add_argument("--dry-run", action="store_true", help="Mostra chunks sem salvar no DB")
    parser.add_argument("--source",  choices=["basis","ports","fluvial","routes","usinas","services"],
                        help="Roda apenas uma fonte específica")
    args = parser.parse_args()

    if args.dry_run:
        print("[DRY-RUN] nenhum dado sera salvo\n")

    total_ok = 0
    sources = {
        "basis":    ingest_basis,
        "ports":    ingest_ports_international,
        "fluvial":  ingest_ports_fluvial,
        "routes":   ingest_maritime_routes,
        "usinas":   ingest_usinas,
        "services": ingest_services,
    }

    if args.source:
        total_ok = sources[args.source](dry_run=args.dry_run)
    else:
        for name, fn in sources.items():
            total_ok += fn(dry_run=args.dry_run)

    if not args.dry_run:
        from models.database import get_session, CorporateKnowledge
        from sqlalchemy import func
        sess = get_session()
        total = sess.query(func.count(CorporateKnowledge.id)).scalar()
        samba = sess.query(func.count(CorporateKnowledge.id))\
            .filter(CorporateKnowledge.document_name.like("SAMBA_%")).scalar()
        sess.close()
        print(f"\n{'='*60}")
        print(f"BASE FINAL: {total} chunks total | {samba} chunks SAMBA_*")
        print(f"Esta sessão: {total_ok} chunks inseridos/atualizados")
        print(f"{'='*60}")
    else:
        print(f"\n[DRY-RUN] {total_ok} chunks seriam inseridos")


if __name__ == "__main__":
    main()
