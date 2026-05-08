"""
scripts/ingestao_terminais_kb.py
=================================
Injeta os dados técnicos de terminais portuários brasileiros na
CorporateKnowledge do SAMBA_AGENTS para uso em RAG / Samba Assistant.

Cobre 12 portos:
  Paranaguá, Santos, Outeiro/Barcarena (Arco Norte), Rio Grande,
  Antonina, Cabedelo, Fortaleza, Maceió, Recife,
  Salvador/Aratu, São Francisco do Sul (SFS), Suape

Uso:
    python scripts/ingestao_terminais_kb.py
    python scripts/ingestao_terminais_kb.py --dry-run
    python scripts/ingestao_terminais_kb.py --porto PARANAGUA
"""
from __future__ import annotations

import argparse
import os
import struct
import sys
import time
import threading
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(override=True)

from google import genai

GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")
MODEL_EMB  = "models/gemini-embedding-001"
RPM_LIMIT  = 80

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
        print(f"  [DRY] {doc_name}")
        preview = content[:120].replace("\n", " ").encode("ascii", errors="replace").decode("ascii")
        print(f"        {len(content)} chars | {preview}")
        return True
    from models.database import get_session, CorporateKnowledge
    try:
        emb = embed_text(content)
    except Exception as e:
        print(f"  [EMB ERRO] {doc_name}: {e}")
        return False
    sess = get_session()
    try:
        existing = sess.query(CorporateKnowledge).filter(
            CorporateKnowledge.document_name == doc_name
        ).first()
        if existing:
            existing.content   = content
            existing.embedding = emb
            action = "UPDATE"
        else:
            sess.add(CorporateKnowledge(
                document_name=doc_name,
                content=content,
                embedding=emb,
            ))
            action = "INSERT"
        sess.commit()
        print(f"  [{action}] {doc_name}")
        return True
    except Exception as e:
        sess.rollback()
        print(f"  [DB ERRO] {doc_name}: {e}")
        return False
    finally:
        sess.close()

# ═════════════════════════════════════════════════════════════════════════════
# DADOS DOS TERMINAIS — 12 PORTOS BRASILEIROS
# Fonte: scripts/especificar_terminais_*.py (SAMBA_LIMPO V10)
# ═════════════════════════════════════════════════════════════════════════════

TERMINAIS_KB: dict[str, str] = {}

# ─────────────────────────────────────────────────────────────────────────────
# 1. PARANAGUÁ — PR (Sul/Sudeste, grãos e açúcar)
# ─────────────────────────────────────────────────────────────────────────────
TERMINAIS_KB["PARANAGUA"] = """TERMINAIS PORTUÁRIOS — PORTO DE PARANAGUÁ (PARANAGUA) — PR
Estado: Paraná | Região: Sul | Tipo: Granéis Agrícolas + Açúcar
Calado máximo do porto: 12.8 metros | Classe de navio: Panamax (limite prático)
Elevação/FOB referência: USD 7.50/MT (grãos e VHP)
Latitude: -25.5013° | Longitude: -48.5267°

TERMINAIS PRIVADOS (GRANÉIS SÓLIDOS — SOY / CORN / VHP / IC45):

Terminal: Corredor de Exportação (Berço Principal)
  Produtos: SOY, CORN, VHP, IC45
  Taxa de carregamento: 60.000 t/dia (2.500 t/h × 24h)
  Calado máximo: 12.5 metros | LOA máximo: 280 metros
  Tipo: PRIVADO (operado pela ATP / Corredor)
  Elevação USD/MT: 7.50

Terminal: Bunge (Paranaguá)
  Produtos: SOY, CORN
  Taxa de carregamento: 24.000 t/dia (1.000 t/h × 24h)
  Calado máximo: 12.0 metros | LOA máximo: 230 metros
  Tipo: PRIVADO

Terminal: Pasa (Paranaguá)
  Produtos: SOY, CORN, VHP
  Taxa de carregamento: 20.000 t/dia
  Calado máximo: 12.8 metros | LOA máximo: 245 metros
  Tipo: PRIVADO

Terminal: Soceppar/Bunge (Paranaguá)
  Produtos: SOY, CORN
  Taxa de carregamento: 20.000 t/dia
  Calado máximo: 12.8 metros | LOA máximo: 245 metros
  Tipo: PRIVADO

Terminal: Terminal de Cereais (Paranaguá)
  Produtos: SOY, CORN, VHP
  Taxa de carregamento: 20.000 t/dia
  Calado máximo: 12.5 metros | LOA máximo: 230 metros
  Tipo: PRIVADO

CONTEXTO ESTRATÉGICO:
Paranaguá é o maior porto exportador de grãos do Brasil, especialmente soja e milho
do Mato Grosso do Sul, norte do Paraná e Mato Grosso. Via ferrovia (ALL/Rumo),
conecta-se ao interior até Rondonópolis (MT). O Corredor de Exportação é o terminal
de maior throughput, com capacidade de 60.000 t/dia. Navios Panamax (calado ~12.5m)
são o padrão dominante; Post-Panamax encontram restrição de calado. Principal rota:
Paranaguá → Estreito de Malaca → China (SOY). Basis FOB Paranaguá (PGROSSA) tipicamente
varia entre USD -2.80 e -4.50/MT abaixo do CBOT. Elevação portuária média: USD 7.50/MT.
"""

# ─────────────────────────────────────────────────────────────────────────────
# 2. SANTOS — SP (maior porto da América Latina)
# ─────────────────────────────────────────────────────────────────────────────
TERMINAIS_KB["SANTOS"] = """TERMINAIS PORTUÁRIOS — PORTO DE SANTOS (SANTOS) — SP
Estado: São Paulo | Região: Sudeste | Tipo: Granéis Agrícolas + Açúcar + Carga Geral
Calado máximo do canal: 15.0 metros | Classe de navio: Post-Panamax, Capesize parcial
Elevação/FOB referência: USD 7.20/MT (grãos) | USD 7.50/MT (VHP)
Latitude: -23.9618° | Longitude: -46.3322°

TERMINAIS — GRÃOS (SOY / CORN):

Terminal ADM Santos
  Produtos: SOY, CORN | Taxa: 35.000 t/dia | Calado: 13.7m | LOA: 260m | Tipo: PRIVADO
  Coordenadas portão: lat -23.938, lon -46.310 | berço: lat -23.940, lon -46.312

Terminal TEG (Terminal de Exportação de Grãos — Cargill)
  Produtos: SOY, CORN | Taxa: 35.000 t/dia | Calado: 13.3m | LOA: 250m | Tipo: PRIVADO

Terminal TES (Terminal de Exportação Santos — Bunge)
  Produtos: SOY, CORN | Taxa: 30.000 t/dia | Calado: 14.5m | LOA: 270m | Tipo: PRIVADO

Terminal XXXIX (Louis Dreyfus / LDC)
  Produtos: SOY, CORN | Taxa: 72.000 t/dia | Calado: 14.4m | LOA: 290m | Tipo: PRIVADO
  Maior throughput de Santos para grãos.

Terminal TIPLAM (Terminal Integrado Portuário Luiz Antonio Mesquita)
  Produtos: SOY, CORN | Taxa: 50.000 t/dia | Calado: 13.2m | LOA: 260m | Tipo: PRIVADO

Terminal Cutrale Santos
  Produtos: SOY, CORN | Taxa: 25.000 t/dia | Calado: 13.7m | LOA: 245m | Tipo: PRIVADO

GG Terminal (Glencore Grain)
  Produtos: SOY, CORN | Taxa: 50.000 t/dia | Calado: 14.3m | LOA: 280m | Tipo: PRIVADO

Terminal COFCO Santos
  Produtos: SOY, CORN | Taxa: 36.000 t/dia | Calado: 13.4m | LOA: 265m | Tipo: PRIVADO

Terminal T-Grão (Santos)
  Produtos: SOY, CORN | Taxa: 15.000 t/dia | Calado: 14.5m | LOA: 270m | Tipo: PRIVADO

CLI Rumo Santos
  Produtos: SOY, CORN | Taxa: 25.000 t/dia | Calado: 14.0m | LOA: 260m | Tipo: PRIVADO
  Ferroviário integrado (Rumo Logística).

TERMINAIS — AÇÚCAR (VHP / IC45):

Terminal TEAG (Terminal de Exportação de Açúcar em Granel — Copersucar)
  Produtos: VHP | Taxa: 60.000 t/dia | Calado: 13.5m | LOA: 260m | Tipo: PRIVADO

Cooper-Sugar Santos
  Produtos: VHP, IC45 | Taxa: 24.000 t/dia | Calado: 13.0m | LOA: 240m | Tipo: PRIVADO

RUMO Açúcar Santos
  Produtos: VHP | Taxa: 36.000 t/dia | Calado: 13.5m | LOA: 260m | Tipo: PRIVADO

TIPLAM Açúcar
  Produtos: VHP, IC45 | Taxa: 20.000 t/dia | Calado: 13.2m | LOA: 250m | Tipo: PRIVADO

CONTEXTO ESTRATÉGICO:
Santos é o maior porto da América Latina e o principal hub de açúcar VHP do Brasil
(estado de São Paulo = maior produtor mundial). Canal dragado para 15m permite Post-Panamax.
Basis FOB Santos tipicamente USD 0.30–0.80/MT acima do ICE para VHP. Para grãos, Santos
compete com Paranaguá sendo mais caro em logística interna do Mato Grosso, mas tem
maior capacidade de calado e mais terminais. Elevação média grãos: USD 7.20/MT.
IC45 (açúcar ensacado/bagged) tem taxa de carregamento drasticamente menor e eleva
custos de elevação para USD 15-20/MT.
"""

# ─────────────────────────────────────────────────────────────────────────────
# 3. OUTEIRO / BARCARENA — PA (Arco Norte)
# ─────────────────────────────────────────────────────────────────────────────
TERMINAIS_KB["BARCARENA"] = """TERMINAIS PORTUÁRIOS — PORTO DE OUTEIRO / BARCARENA (BARCARENA) — PA
Estado: Pará | Região: Arco Norte | Tipo: Granéis Agrícolas (Soja / Milho)
Porto alternativo: Vila do Conde (Barcarena) | Operador: TVV, Cargill, Amaggi
Calado máximo: 14.0 metros (limite Panamax) | Tipo de navio: Panamax
Elevação/FOB referência: USD 7.00/MT
Latitude: -1.273861° | Longitude: -48.487306°

TERMINAIS — GRÃOS E FERTILIZANTES (SOY / CORN / VHP / FERT):

Terminal Pier Modernizado (Barcarena / TVV)
  Produtos: SOY, CORN, VHP, FERT
  Taxa de carregamento: 7.200 t/dia (300 t/h × 24h)
  Calado máximo: 14.0 metros | LOA máximo: 225 metros
  Tipo: MARITIMO (Porto Organizado de Vila do Conde)
  Elevação USD/MT: 7.00

Terminal Cargill Santarém (operação fluvial + transbordo Barcarena)
  Produtos: SOY, CORN
  Modalidade: Barcaça fluvial → transbordo oceânico em Barcarena
  Calado fluvial: 3.5–4.5 m | Calado oceânico: até 14.0m (ao transbordo)

Terminal Amaggi (Miritituba → Itaituba → Barcarena)
  Produtos: SOY, CORN
  Modalidade: Barcaça no Tapajós + transbordo em Barcarena
  Rota hidroviária: Miritituba (PA) → Barcarena

CONTEXTO ESTRATÉGICO — ARCO NORTE:
O Arco Norte é a principal rota de competitividade para soja e milho do
Mato Grosso destinados à China e Ásia. Inclui os portos de:
  Santarém/PA, Barcarena/PA, Itacoatiara/AM, Miritituba/PA (transbordo)

Vantagem: frete interno Mato Grosso → Barcarena via hidrovias é USD 8–15/MT
mais barato que Mato Grosso → Paranaguá (rodovia/ferrovia). Isso se traduz
em maior competitividade no CIF China.

Desvantagem: menor capacidade de calado e throughput vs Santos/Paranaguá;
dependência de barcaças fluviais com sazonalidade hidrológica.

Código de porto no sistema SAMBA: BARCARENA
Rota obrigatória para soja Arco Norte no motor de pricing.
Elevação portuária: USD 7.00/MT (menor que Sul/Sudeste por menor custo operacional).
Basis FOB Barcarena: tipicamente USD -1.50 a -2.00/MT vs CBOT (mais favorável
que Paranaguá por menor custo de frete interno para MT).
"""

# ─────────────────────────────────────────────────────────────────────────────
# 4. RIO GRANDE — RS (Sul, calado máximo do Brasil para grãos)
# ─────────────────────────────────────────────────────────────────────────────
TERMINAIS_KB["RIO_GRANDE"] = """TERMINAIS PORTUÁRIOS — PORTO DE RIO GRANDE (RIO_GRANDE) — RS
Estado: Rio Grande do Sul | Região: Sul | Tipo: Granéis Agrícolas + Açúcar
Calado máximo: 14.5 metros — MAIOR DO BRASIL para exportação de grãos
Classe de navio: Panamax máximo | Permite Supramax e Ultramax com full load
Elevação/FOB referência: USD 7.00/MT (grãos) | USD 8.50/MT (VHP)

TERMINAIS — GRÃOS (SOY / CORN):

Terminal Bianchini (Rio Grande)
  Produtos: SOY, CORN | Taxa: 67.200 t/dia (2.800 t/h × 24h) | Calado: 14.5m | LOA: 290m
  Tipo: PRIVADO | Maior throughput de grãos do RS

Terminal Bunge (Rio Grande)
  Produtos: SOY, CORN | Taxa: 57.600 t/dia (2.400 t/h × 24h) | Calado: 14.5m | LOA: 290m
  Tipo: PRIVADO

Terminal Termasa (Rio Grande)
  Produtos: SOY, CORN | Taxa: 40.800 t/dia (1.700 t/h × 24h) | Calado: 14.5m | LOA: 270m
  Tipo: PRIVADO

Terminal Tergrasa (Rio Grande)
  Produtos: SOY, CORN | Taxa: 40.800 t/dia (1.700 t/h × 24h) | Calado: 14.5m | LOA: 270m
  Tipo: PRIVADO

TERMINAIS — AÇÚCAR (VHP):

Terminal Rocha (Rio Grande)
  Produtos: VHP | Taxa: 9.600 t/dia (400 t/h × 24h) | Calado: 14.5m | LOA: 250m
  Elevação: USD 8.50/MT (premium para VHP em RS)
  Tipo: PRIVADO

CONTEXTO ESTRATÉGICO:
Rio Grande é o porto de referência do agronegócio gaúcho (soja/milho RS e SC).
Com calado de 14.5m é o mais profundo do Brasil para granéis agrícolas —
permite navios Panamax com carga plena sem restrição de calado.
Principal mercado de origem: RS, SC, sul do PR.
Rota típica: Rio Grande → Canal do Panamá → China (SOY/CORN).
Basis FOB Rio Grande ligeiramente menos competitivo que Paranaguá para origem
Mato Grosso (frete interno mais alto), mas preferencial para grãos do Sul.
Throughput combinado dos 4 terminais de grãos: ~206.400 t/dia — capacidade
de escoamento de uma safra gaúcha em ritmo máximo de exportação.
"""

# ─────────────────────────────────────────────────────────────────────────────
# 5. ANTONINA — PR (porto secundário do Paraná)
# ─────────────────────────────────────────────────────────────────────────────
TERMINAIS_KB["ANTONINA"] = """TERMINAIS PORTUÁRIOS — PORTO DE ANTONINA (ANTONINA) — PR
Estado: Paraná | Região: Sul | Tipo: Granéis + Fertilizantes + Carga Geral
Calado máximo: 9.15 metros — RESTRICAO SEVERA (Panamax inviável, Handysize ideal)
Elevação/FOB referência: USD 8.00/MT (scale premium por menor eficiência de escala)

TERMINAIS:

Terminal Ponta do Félix (Berço 1) — SOY, CORN, WHEAT, VHP, IC45, FERT
  Taxa: 10.000 t/dia (estimada) | Calado: 9.15m | LOA: 200m | Tipo: PRIVADO

Terminal Ponta do Félix (Berço 2) — SOY, CORN, WHEAT, VHP, IC45, FERT
  Taxa: 10.000 t/dia | Calado: 9.15m | LOA: 200m | Tipo: PRIVADO

Terminal Barão de Teffé (Cais Público) — CGC, MISC (carga geral)
  Taxa: 3.000 t/dia | Calado: 5.79m | LOA: 155m | Tipo: PUBLICO
  ATENCAO: calado de 5.79m restringe a embarcações fluviais/costeiras

CONTEXTO ESTRATÉGICO:
Antonina é porto auxiliar do complexo portuário do Paraná (Paranaguá é o principal).
Posicionado na Baía de Paranaguá, tem calado máximo de apenas 9.15m — significativamente
abaixo de Paranaguá (12.5–12.8m). Atende principalmente navios Handysize (20–35k DWT)
e fertilizantes de importação. Não é rota principal do motor de pricing para grãos —
apenas fallback para volumes menores ou navios de menor porte. Elevação USD 8.00/MT
reflete menor economia de escala vs Paranaguá.
"""

# ─────────────────────────────────────────────────────────────────────────────
# 6. CABEDELO — PB (Nordeste, grãos e açúcar)
# ─────────────────────────────────────────────────────────────────────────────
TERMINAIS_KB["CABEDELO"] = """TERMINAIS PORTUÁRIOS — PORTO DE CABEDELO (CABEDELO) — PB
Estado: Paraíba | Região: Nordeste | Tipo: Granéis Sólidos (SOY/CORN/VHP) + IC45
Calado máximo: 9.14 metros | Classe de navio: Handymax (40–50k DWT max)
Elevação/FOB referência: USD 8.00/MT (grãos e VHP) | USD 20.00/MT (IC45 — gargalo)

TERMINAIS:

Terminal de Granéis Sólidos — TGS (Berço 4)
  Produtos: SOY, CORN, VHP
  Taxa: 9.600 t/dia (400 t/h × 24h) | Calado: 9.14m | LOA: 245m
  Tipo: PUBLICO | Melhor berço do porto (granel sólido seco)
  Elevação: USD 8.00/MT

Cais Comercial (Berços 1-3) — IC45 (ENSACADO)
  Taxa: 500 t/dia — GARGALO CRÍTICO | Calado: 9.14m | LOA: 245m
  Tipo: PUBLICO | Elevação: USD 20.00/MT (penalidade por baixa velocidade)

Cais Comercial (Berços 1-3) — FERT (fertilizantes)
  Taxa: 5.000 t/dia | Calado: 9.14m | LOA: 245m

CONTEXTO ESTRATÉGICO:
Cabedelo é o porto de referência da Paraíba. TGS (Berço 4) tem boa performance
para grãos e VHP (9.600 t/dia). IC45 é gargalo severo (500 t/dia apenas) — ao
negociar açúcar branco IC45 por Cabedelo, o custo de elevação dispara para
USD 20/MT e o tempo de carregamento é fator crítico de demurrage.
Calado de 9.14m limita a navios Handymax; Post-Panamax e Panamax não acessam.
Rota típica: Nordeste Brasil → Mediterrâneo / África / Oriente Médio.
"""

# ─────────────────────────────────────────────────────────────────────────────
# 7. FORTALEZA — CE (Mucuripe, Nordeste)
# ─────────────────────────────────────────────────────────────────────────────
TERMINAIS_KB["FORTALEZA"] = """TERMINAIS PORTUÁRIOS — PORTO DE FORTALEZA / MUCURIPE (FORTALEZA) — CE
Estado: Ceará | Região: Nordeste | Tipo: Granéis (SOY/CORN/VHP) + IC45 + FERT
Calado máximo: 11.0 metros | Classe de navio: Handymax / Supramax (até 50k DWT)
Elevação/FOB referência: USD 7.50/MT (grãos e VHP) | USD 18.00/MT (IC45)

TERMINAIS:

Cais Comercial (Berços 104-106) — SOY, CORN, VHP (GRANEL)
  Taxa: 8.400 t/dia (350 t/h × 24h) | Calado: 11.0m | LOA: 232m
  Tipo: PUBLICO | Elevação: USD 7.50/MT

Cais Comercial (Berços 102-103) — IC45 (ENSACADO / BAGGED)
  Taxa: 1.680 t/dia (70 t/h × 24h) — gargalo moderado | Calado: 10.3m | LOA: 197m
  Tipo: PUBLICO | Elevação: USD 18.00/MT

Cais Comercial (Berços 101-102) — FERT (fertilizantes, importação)
  Taxa: 6.000 t/dia | Calado: 11.0m | LOA: 232m

CONTEXTO ESTRATÉGICO:
Fortaleza/Mucuripe tem melhor calado do Nordeste setentrional (11.0m), aceitando
Supramax com plena carga. Grãos e VHP granel tem boa performance (8.400 t/dia).
IC45 é restrito a Berços 102-103 com taxa muito inferior (1.680 t/dia).
Rota: Nordeste Brasil → Mediterrâneo (Espanha/Portugal), Oriente Médio, África Ocidental.
Para soja/milho, Fortaleza é menos usado — principal origem agrícola do CE é baixa
vs MT/MS/PR/RS. Mais relevante para açúcar VHP de usinas do CE/RN.
"""

# ─────────────────────────────────────────────────────────────────────────────
# 8. MACEIÓ — AL (açúcar VHP premium, Sugar Terminal Berço 5)
# ─────────────────────────────────────────────────────────────────────────────
TERMINAIS_KB["MACEIO"] = """TERMINAIS PORTUÁRIOS — PORTO DE MACEIÓ (MACEIO) — AL
Estado: Alagoas | Região: Nordeste | Tipo: Açúcar VHP (especializado) + Grãos + IC45
Calado máximo: 11.0 metros (Berço 5) / 10.5 metros (demais berços)
Classe de navio: Supramax / Handymax
Elevação/FOB referência: USD 7.50/MT (VHP) | USD 10.00/MT (SOY/CORN) | USD 18.00/MT (IC45)

TERMINAIS:

SUGAR TERMINAL (Berço 5) — VHP GRANEL — ALTA PERFORMANCE
  Produtos: VHP | Taxa: 12.000 t/dia (500 t/h × 24h) — melhor de Alagoas
  Calado: 11.0m | LOA: 240m | Tipo: MARITIMO | Elevação: USD 7.50/MT
  REFERÊNCIA: principal terminal de açúcar VHP do Nordeste

Cais Público (Berços 2-3) — SOY, CORN
  Taxa: 1.488 t/dia (62 t/h × 24h) — LENTO | Calado: 10.5m | LOA: 220m
  Tipo: PUBLICO | Elevação: USD 10.00/MT (penalidade por baixa taxa)

Cais Público (Berços 3/4) — SOY, CORN (complementar)
  Taxa: 1.488 t/dia | Calado: 10.5m | LOA: 200m | Tipo: PUBLICO

Cais Público (Berços 3/4) — IC45 (ENSACADO)
  Taxa: 1.080 t/dia (45 t/h × 24h) | Calado: 10.5m | LOA: 200m
  Tipo: PUBLICO | Elevação: USD 18.00/MT

Cais Público (Berço 2) — FERT (importação)
  Taxa: 2.500 t/dia | Calado: 10.5m | LOA: 220m

CONTEXTO ESTRATÉGICO:
Maceió é referência em açúcar VHP do Nordeste. O Sugar Terminal (Berço 5) é o
mais eficiente da região para VHP granel (12.000 t/dia — supera todos os berços
de açúcar do nordeste). Alagoas é o 2° maior produtor de cana do Brasil (após SP).
Para SOY/CORN, Maceió é penalizado pela baixa taxa de carregamento nos berços
públicos (1.488 t/dia) — risco alto de demurrage. Ideal apenas para volumes pequenos.
IC45 igualmente lento (1.080 t/dia). Prioridade do motor: VHP = MACEIO, SOY/CORN = outros.
"""

# ─────────────────────────────────────────────────────────────────────────────
# 9. RECIFE — PE (Sugar Terminal, grãos e açúcar)
# ─────────────────────────────────────────────────────────────────────────────
TERMINAIS_KB["RECIFE"] = """TERMINAIS PORTUÁRIOS — PORTO DO RECIFE (RECIFE) — PE
Estado: Pernambuco | Região: Nordeste | Tipo: Açúcar VHP (alta perf.) + Grãos + IC45
Calado máximo: 10.0 metros (limite operacional efetivo) / 12.0 metros (canal)
Classe de navio: Handymax / Supramax
Elevação/FOB referência: USD 7.50/MT (grãos e VHP) | USD 20.00/MT (IC45)

TERMINAIS:

Grain Berth 1 — SOY, CORN, WHEAT
  Taxa: 14.400 t/dia | Calado: 10.0m | LOA: 160m | Tipo: MARITIMO

Grain Berth 9 — SOY, CORN, WHEAT
  Taxa: 14.400 t/dia | Calado: 10.0m | LOA: 239.9m | Tipo: MARITIMO

REC08 Liquiport — SOY, CORN, WHEAT
  Taxa: 14.400 t/dia | Calado: 10.0m | LOA: 200m | Tipo: MARITIMO

SUGAR TERMINAL — VHP (GRANEL) — ALTA PERFORMANCE
  Produtos: VHP | Taxa: 24.000 t/dia (1.000 t/h × 24h)
  Calado: 10.0m | LOA: 180m | Tipo: MARITIMO | Elevação: USD 7.50/MT

Sugar Berths (berços secundários) — VHP
  Taxa: 12.000 t/dia | Calado: 9.7m | LOA: 180m

Armazéns Cobertos — IC45 (ENSACADO / BAGGED)
  Taxa: 200 t/dia — GARGALO CRÍTICO | Calado: 10.0m | LOA: 200m
  Tipo: MARITIMO | Elevação: USD 20.00/MT

REC09 Petribu — FERT (importação)
  Taxa: 5.000 t/dia | Calado: 10.0m | LOA: 200m

CONTEXTO ESTRATÉGICO:
Recife tem o Sugar Terminal mais eficiente do Nordeste para VHP granel
(24.000 t/dia — supera Maceió e Fortaleza). Serve usinas de PE, PB e AL.
Grain Berths têm taxa competitiva (14.400 t/dia cada), porém calado de 10m
é limitante para navios maiores. IC45 tem gargalo extremo (200 t/dia) —
custo FOB de elevação de USD 20/MT é o mais alto do Nordeste para IC45.
Suape (localizado a 40km ao sul de Recife) é alternativa com maior calado.
"""

# ─────────────────────────────────────────────────────────────────────────────
# 10. SALVADOR / ARATU — BA (maior throughput de grãos do Nordeste)
# ─────────────────────────────────────────────────────────────────────────────
TERMINAIS_KB["SALVADOR"] = """TERMINAIS PORTUÁRIOS — COMPLEXO PORTUÁRIO SALVADOR / ARATU (SALVADOR) — BA
Estado: Bahia | Região: Nordeste | Tipo: Granéis Agrícolas + VHP (alta performance)
Calado máximo: 12.8 metros (Terminal Cotegipe / ATU 18)
Classe de navio: Panamax (calado 12.8m é o maior do Nordeste)
Elevação/FOB referência: USD 7.50/MT (grãos e VHP) | USD 22.00/MT (IC45)

TERMINAIS — ALTA PERFORMANCE (SOY / CORN / VHP):

Terminal Cotegipe (Berços 1-2) — SOY, CORN, VHP
  Taxa: 48.000 t/dia (2.000 t/h × 24h) — MELHOR DO NORDESTE
  Calado: 12.8m | LOA: 240m | Tipo: PRIVADO | Elevação: USD 7.50/MT
  Localização: Baía de Aratu, Candeias/BA

Terminal ATU 18 — SOY, CORN (NOVO — Operacional desde Março 2026)
  Taxa: 48.000 t/dia (2.000 t/h × 24h) | Calado: 12.8m | LOA: 240m
  Tipo: PRIVADO | Elevação: USD 7.50/MT
  Status: Terminal mais moderno do Nordeste (inaugurado 03/2026)

TERMINAIS — IC45 (ENSACADO / BAGGED):

Porto Público (Cais) — IC45
  Taxa: 200 t/dia — GARGALO CRÍTICO | Calado: 11.5m | LOA: 200m
  Tipo: PUBLICO | Elevação: USD 22.00/MT (maior penalidade IC45 do Nordeste)

CONTEXTO ESTRATÉGICO:
Salvador/Aratu tem o maior throughput e calado do Nordeste para grãos (48.000 t/dia,
12.8m). Terminal Cotegipe é referência para exportação de soja do Centro-Oeste via
Bahia. O novo Terminal ATU 18 (2026) duplica a capacidade de escoamento de Aratu.
Panamax (até 80k DWT, calado ~12.5m) operam confortavelmente. Para VHP, Cotegipe
também é o mais eficiente do Nordeste. IC45 é um problema severo (200 t/dia no
porto público). Rota: Salvador → Canal do Panamá → China / Ásia, ou via Estreito
de Gibraltar para Europa.
"""

# ─────────────────────────────────────────────────────────────────────────────
# 11. SÃO FRANCISCO DO SUL — SC (SFS, calado 14.0m, Corredor de Exportação)
# ─────────────────────────────────────────────────────────────────────────────
TERMINAIS_KB["SFS"] = """TERMINAIS PORTUÁRIOS — PORTO DE SÃO FRANCISCO DO SUL (SFS) — SC
Estado: Santa Catarina | Região: Sul | Tipo: Granéis Agrícolas + Açúcar
Código porto: SFS (ou SAO_FRANCISCO_DO_SUL)
Calado máximo: 14.0 metros (limite canal navegação) | Classe: Panamax
Elevação/FOB referência: USD 7.50/MT (grãos e VHP) | USD 15.00/MT (IC45)

TERMINAIS — GRÃOS (SOY / CORN):

Corredor de Exportação (Berço 101)
  Taxa: 72.000 t/dia (3.000 t/h × 24h) — MAIOR THROUGHPUT DO PORTO
  Calado: 14.0m | LOA: 225m | Tipo: MARITIMO

TESC Grãos (Berço 102)
  Taxa: 72.000 t/dia | Calado: 14.0m | LOA: 210m | Tipo: MARITIMO

TESC Grãos (Berço 103)
  Taxa: 40.000 t/dia | Calado: 14.0m | LOA: 163m | Tipo: MARITIMO

Terminal Terlogs
  Taxa: 72.000 t/dia | Calado: 14.0m | LOA: 225m | Tipo: MARITIMO

TERMINAIS — AÇÚCAR VHP (GRANEL):

Corredor de Exportação (Berço 101) — VHP
  Taxa: 72.000 t/dia (3.000 t/h × 24h) | Calado: 14.0m | LOA: 225m

TESC VHP (Berço 302)
  Taxa: 40.000 t/dia | Calado: 14.0m | LOA: 264m | Tipo: MARITIMO

Rocha Terminais — VHP
  Taxa: 30.000 t/dia | Calado: 14.0m | LOA: 225m | Tipo: MARITIMO

Seatrade — VHP
  Taxa: 30.000 t/dia | Calado: 14.0m | LOA: 225m | Tipo: MARITIMO

Master Operações — VHP
  Taxa: 30.000 t/dia | Calado: 14.0m | LOA: 225m | Tipo: MARITIMO

TERMINAIS — AÇÚCAR IC45 (ENSACADO / BAGGED):

TESC IC45 (Berço 300)
  Taxa: 1.500 t/dia | Calado: 14.0m | LOA: 192m | Elevação: USD 15.00/MT

TESC IC45 (Berço 301)
  Taxa: 1.500 t/dia | Calado: 14.0m | LOA: 192m | Elevação: USD 15.00/MT

CONTEXTO ESTRATÉGICO:
SFS é o terceiro maior porto exportador de grãos do Brasil (após Paranaguá e Santos).
Corredor de Exportação (72.000 t/dia, 14.0m calado) é extremamente competitivo.
Serve principalmente a produção de SC, RS e PR sul. VHP tem 5 terminais disponíveis
com calado de 14.0m — único porto do Sul com tantos operadores de açúcar graneleiro.
IC45 isolado nos Berços 300/301 com taxa de apenas 1.500 t/dia — melhor que o Nordeste,
mas ainda muito restritivo vs granel. Elevação IC45 em SFS é USD 15/MT (mais barata
do Sul para ensacado, por menor demurrage comparativo).
"""

# ─────────────────────────────────────────────────────────────────────────────
# 12. SUAPE — PE (profundidade 17.3m, mais moderno do Nordeste)
# ─────────────────────────────────────────────────────────────────────────────
TERMINAIS_KB["SUAPE"] = """TERMINAIS PORTUÁRIOS — PORTO DE SUAPE (SUAPE) — PE
Estado: Pernambuco | Região: Nordeste | Município: Ipojuca
Tipo: Hub Industrial + Granéis (SOY/CORN/VHP) + Combustíveis + Contêineres
Canal externo: calado 17.3 metros (Post-Panamax, Capesize possível) — MAIS PROFUNDO DO NORDESTE
Berços de granéis: calado 14.5m | LOA: 350m
Elevação/FOB referência: USD 7.50/MT (grãos e VHP) | USD 20.00/MT (IC45)

TERMINAIS — GRÃOS E VHP (ATIVOS):

Cais 4 — SOY, CORN, VHP
  Taxa: 24.000 t/dia (1.000 t/h × 24h) | Calado: 14.5m | LOA: 350m
  Tipo: MARITIMO | Ativo: SIM | Elevação: USD 7.50/MT

Cais 1 — VHP (GRANEL)
  Taxa: 24.000 t/dia (1.000 t/h × 24h) | Calado: 14.5m | LOA: 275m
  Tipo: MARITIMO | Ativo: SIM | Elevação: USD 7.50/MT

Cais 4 — VHP (GRANEL, duplo uso com grãos)
  Taxa: 24.000 t/dia | Calado: 14.5m | LOA: 350m | Ativo: SIM

TERMINAIS — IC45 (ENSACADO):

Cais 5 — IC45 ENSACADO
  Taxa: 200 t/dia — GARGALO CRÍTICO | Calado: 14.5m | LOA: 330m
  Tipo: MARITIMO | Ativo: SIM | Elevação: USD 20.00/MT

TERMINAIS — PROJETOS DE EXPANSÃO (INATIVOS no simulador):

Pier 6 (Projeto) — CORN | Calado: 15.5m | LOA: 300m | Taxa: 30.000 t/dia | Ativo: NÃO
Pier 7 (Projeto) — SOY  | Calado: 15.5m | LOA: 300m | Taxa: 30.000 t/dia | Ativo: NÃO

CONTEXTO ESTRATÉGICO:
Suape é o porto mais moderno do Nordeste, inaugurado na década de 1980 como porto
industrial. Canal externo de 17.3m é o mais profundo do Nordeste — permite Capesize
para minérios, mas berços de granéis são limitados a 14.5m (Panamax max).
Cais 4 tem o maior LOA do Brasil para navios de granel agrícola (350m).
Suape compete diretamente com Recife (40km ao norte) e tem vantagem de calado e
modernidade. Para SOY/CORN, capacidade de 24.000 t/dia é boa mas não excepcional.
Para VHP, 48.000 t/dia combinados (Cais 1 + Cais 4) é o melhor do Nordeste ao lado
de Salvador/Aratu. IC45 é gargalo extremo (200 t/dia, igual a Recife).
Projetos Pier 6 e Pier 7 (expansão futura) estão salvos no banco mas INATIVOS
no motor de roteamento Dijkstra até conclusão das obras.
Localização: Ipojuca/PE (6° latitude sul — posição favorável para Atlântico Norte e África).
"""

# ─────────────────────────────────────────────────────────────────────────────
# SUMÁRIO COMPARATIVO — TODOS OS PORTOS
# ─────────────────────────────────────────────────────────────────────────────
TERMINAIS_KB["_RESUMO_PORTOS_BR"] = """RESUMO COMPARATIVO — TERMINAIS PORTUÁRIOS BRASILEIROS (GRANÉIS AGRÍCOLAS)
Fonte: SAMBA Pricing Engine V10 | Atualização: 2025-2026

PORTOS SUL/SUDESTE (principais exportadores de grãos):

Porto         | Estado | Calado max | Throughput grãos (t/dia) | Elevação (USD/MT) | Código SAMBA
Paranaguá     | PR     | 12.5m      | 60.000 (Corredor)        | 7.50              | PARANAGUA
Santos        | SP     | 14.5m      | 72.000 (Terminal XXXIX)  | 7.20              | SANTOS
Rio Grande    | RS     | 14.5m      | 67.200 (Bianchini)       | 7.00              | RIO_GRANDE
SFS           | SC     | 14.0m      | 72.000 (Corredor)        | 7.50              | SFS
Antonina      | PR     | 9.15m      | 10.000 (Ponta do Félix)  | 8.00              | ANTONINA

PORTOS ARCO NORTE (vantagem competitiva para Mato Grosso → Ásia):

Porto         | Estado | Calado max | Throughput grãos (t/dia) | Elevação (USD/MT) | Código SAMBA
Barcarena     | PA     | 14.0m      | 7.200 (Pier Mod.)        | 7.00              | BARCARENA
Santarém*     | PA     | 3.5-4.5m   | via barcaça              | —                 | —
Itacoatiara*  | AM     | 4.0m       | via barcaça              | —                 | —
*terminais fluviais de transbordo, não entram no Dijkstra oceânico diretamente

PORTOS NORDESTE (açúcar VHP e grãos regionais):

Porto         | Estado | Calado max | VHP throughput (t/dia)   | Elevação (USD/MT) | Código SAMBA
Salvador/Aratu| BA     | 12.8m      | 48.000 (Cotegipe + ATU18)| 7.50              | SALVADOR
Recife        | PE     | 10.0m      | 24.000 (Sugar Terminal)  | 7.50              | RECIFE
Suape         | PE     | 14.5m*     | 24.000 (Cais 4)          | 7.50              | SUAPE
Maceió        | AL     | 11.0m      | 12.000 (Sugar Terminal)  | 7.50              | MACEIO
Fortaleza     | CE     | 11.0m      | 8.400 (Cais Com.)        | 7.50              | FORTALEZA
Cabedelo      | PB     | 9.14m      | 9.600 (TGS B4)           | 8.00              | CABEDELO
*Suape: canal 17.3m mas berços granel 14.5m

IC45 (AÇÚCAR ENSACADO) — GARGALO GENERALIZADO:
Todos os portos têm throughput dramaticamente menor para IC45 vs VHP granel.
Taxas: 200 t/dia (Recife/Suape), 500 t/dia (Cabedelo), 1.080 t/dia (Maceió),
       1.500 t/dia (SFS — melhor do Sul), 1.680 t/dia (Fortaleza).
Elevação IC45: USD 15–22/MT vs USD 7–8/MT para VHP/Grãos. Evitar IC45 para
grandes volumes — custo de demurrage pode superar o diferencial de preço.

ARCO NORTE — VANTAGEM COMPETITIVA:
Frete interno MT → Barcarena via hidrovias = USD 8–15/MT mais barato vs MT → Paranaguá.
Isso se traduz em basis FOB Barcarena 1.0–2.0 USD/MT mais favorável que Paranaguá
para origem Mato Grosso. Motor SAMBA usa BARCARENA como porto obrigatório para
soja Arco Norte (código: BARCARENA).
"""

# ═════════════════════════════════════════════════════════════════════════════
# FUNÇÕES DE INGESTÃO
# ═════════════════════════════════════════════════════════════════════════════

def ingest_terminais(porto_filter: str | None = None, dry_run: bool = False) -> int:
    """Injeta chunks de terminais portuários na CorporateKnowledge."""
    print("\n=== TERMINAIS PORTUÁRIOS BRASILEIROS ===")
    ok = errors = 0

    for key, content in TERMINAIS_KB.items():
        # Filtro por porto se especificado
        if porto_filter and key != porto_filter and key != "_RESUMO_PORTOS_BR":
            continue

        doc_name = f"SAMBA_TERMINAL_BR_{key}"
        if save_chunk(doc_name, content, dry_run):
            ok += 1
        else:
            errors += 1

    print(f"\n  Terminais: {ok} chunks inseridos/atualizados | {errors} erros")
    return ok


def main():
    parser = argparse.ArgumentParser(
        description="Injeta dados de terminais portuários brasileiros na KB do SAMBA_AGENTS"
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Mostra chunks sem salvar no DB")
    parser.add_argument("--porto",
                        choices=list(TERMINAIS_KB.keys()),
                        help="Ingere apenas um porto específico")
    args = parser.parse_args()

    if args.dry_run:
        print("[DRY-RUN] nenhum dado sera salvo\n")

    total_ok = ingest_terminais(porto_filter=args.porto, dry_run=args.dry_run)

    if not args.dry_run:
        try:
            from models.database import get_session, CorporateKnowledge
            from sqlalchemy import func
            sess = get_session()
            total = sess.query(func.count(CorporateKnowledge.id)).scalar()
            terminais = sess.query(func.count(CorporateKnowledge.id))\
                .filter(CorporateKnowledge.document_name.like("SAMBA_TERMINAL_%")).scalar()
            sess.close()
            print(f"\n{'='*60}")
            print(f"BASE FINAL: {total} chunks total | {terminais} chunks SAMBA_TERMINAL_*")
            print(f"Esta sessão: {total_ok} chunks inseridos/atualizados")
            print(f"{'='*60}")
        except Exception as e:
            print(f"\n[INFO] Não foi possível consultar totais do DB: {e}")
            print(f"Esta sessão: {total_ok} chunks processados")
    else:
        print(f"\n[DRY-RUN] {total_ok} chunks seriam inseridos")


if __name__ == "__main__":
    main()
