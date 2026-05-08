#!/usr/bin/env python3
"""
scripts/ingest_drive_knowledge.py
==================================
Ingere o conhecimento extraido dos 15 documentos do Google Drive
para a tabela CorporateKnowledge (Cerebro Vetorial RAG).

Fontes:
  - 20260419_SE_Soybean_Mexico.pdf    — Precos CIF Mexico + logistica
  - 20260419_SE_Soybean_China.pdf     — Precos CIF China + mercado
  - 20260419_SE_Soybean_Thailand.pdf  — Precos CIF Tailandia + importadores
  - SE_Soybean_China_2026 (PPTX)      — Apresentacao China
  - 04_StartCo_Vietnam_Ethanol.pptx   — Etanol Vietnam
  - SCO_ROKA_YELLOW CORN PetroVietnam — Milho CIF Vietnam (Rokane)
  - 1UTC6CWQK9tUVAwR8...  (Google Doc) — 20 compradores Mexico + roteiros
  - 02_StartCo_China_Soja.pptx        — Intelligence report China

Rodar:
  python scripts/ingest_drive_knowledge.py [--clear]

  --clear: apaga todos os registros existentes antes de inserir
"""

import argparse
import json
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from models.database import get_session, create_tables, CorporateKnowledge


# ============================================================
# CHUNKS DE CONHECIMENTO — estruturados por documento-fonte
# ============================================================

KNOWLEDGE_CHUNKS: list[dict] = []


def chunk(document_name: str, chunk_index: int, content: str) -> dict:
    words = content.split()
    # Aproximacao: 1 token ≈ 0.75 palavras (ingles/portugues misto)
    token_count = max(1, int(len(words) / 0.75))
    return {
        "document_name": document_name,
        "chunk_index": chunk_index,
        "content": content.strip(),
        "embedding": None,
        "token_count": token_count,
    }


# ─────────────────────────────────────────────────────────────
# BLOCO 1: PRECOS CIF — SOJA GMO — MEXICO
# Fonte: 20260419_SE_Soybean_Mexico.pdf (Samba Export, Abril 2026)
# ─────────────────────────────────────────────────────────────
DOC_MEX = "20260419_SE_Soybean_Mexico.pdf"

KNOWLEDGE_CHUNKS += [
    chunk(DOC_MEX, 0,
        """PRECOS CIF SOJA GMO — DESTINO MEXICO (Abril 2026, Base CBOT Maio '26)
Portos: Veracruz · Altamira · Tampico · Manzanillo · Progreso

| Volume Mensal | Volume Anual   | Tipo de Navio         | Preco CIF    |
|---------------|----------------|-----------------------|--------------|
| 1.000 MT      | 12.000 MT/ano  | Container             | USD 570 /MT  |
| 2.000 MT      | 24.000 MT/ano  | Container             | USD 550 /MT  |
| 12.500 MT     | 150.000 MT/ano | Handymax / Supramax   | USD 525 /MT  |
| 25.000 MT     | 300.000 MT/ano | Handymax / Supramax   | USD 515 /MT  |
| 50.000 MT     | 600.000 MT/ano | Panamax               | USD 500 /MT  |
| 100.000 MT    | 1.200.000 MT/ano | Panamax             | USD 485 /MT  ★ Melhor Valor |

Precos indicativos base CBOT Maio '26 + mercado de frete atual.
Preco final fixado na assinatura do contrato. Termos GAFTA/FOSFA. Sujeito a disponibilidade.
Vantagem vs. USA: preco CIF ~6% menor nessa safra."""),

    chunk(DOC_MEX, 1,
        """LOGISTICA BRASIL → MEXICO — TEMPOS DE TRANSITO (2026)

ARCO NORTE (Itaqui / Outeiro-Belem):
  • Hub multimodal: rodoviario, ferroviario e fluvial
  • Terminal proprio em Outeiro — ZERO demurrage
  • Instalacoes certificadas MAPA/SAGARPA
  Tempos de transito:
    Veracruz / Altamira / Tampico : 10–16 dias
    Manzanillo (Pacifico)         : 15–22 dias
    Progreso                      : 11–17 dias

SANTOS / PARANAGUA (Rota Sul):
  • Maior complexo portuario da America Latina
  • Principal hub para soja conteinerizada e a granel
  Tempos de transito:
    Veracruz / Altamira / Tampico : 20–26 dias
    Manzanillo (Pacifico)         : 24–32 dias
    Progreso                      : 22–28 dias

VANTAGEM ARCO NORTE: economiza 8–10 dias em relacao a Santos para portos do Golfo do Mexico."""),

    chunk(DOC_MEX, 2,
        """MERCADO DE SOJA MEXICO — INTELIGENCIA COMERCIAL 2025/2026

Demanda anual total: ~7 Mt/ano
Crescimento projetado 2025/26: +8%
Uso: alimentacao animal 78%, processamento humano 22%

Dependencia atual de USA: 90%+ das importacoes
Risco de cadena: crescente (volatilidade geopolitica, tarifas)
Vantagem do Brasil: preco ~6% menor que USA; alternativa certificada disponivel

Janela de exportacao otima: Janeiro a Julho (safra ativa)
Producao recorde 2025/26: estimada em 175+ Mt
Exportacoes do Brasil em fev/2026: 4,08 Mt (forte fluxo antecipado)

Regulatoria: Cumprimento total SAGARPA/SENASICA
Eventos GMO aprovados para Mexico: MON89788, MON87708, MON87705, MON87769,
  GTS 40-3-2, MON87701, MON87701, A2704-12, DP-305423-1

Principais setores compradores:
  • Avicultura: Bachoco, Tyson, Pilgrim's Pride (maior setor)
  • Industria porcina e lactea: Lala, Alpura, Sigma Alimentos
  • Aplastadores e traders: Bunge, Cargill, ADM, ProPac

Especificacoes tecnicas para Mexico:
  Produto: Soja Amarela (Glycine max), Origem: Maranhao/MATOPIBA
  Umidade: max 14% | Proteina (base seca): min 36% | Oleo: min 18%
  Materia estranha: max 1% | Graos danificados: max 8% | Mistura: max 2%"""),
]


# ─────────────────────────────────────────────────────────────
# BLOCO 2: PRECOS CIF — SOJA GMO — CHINA
# Fonte: 20260419_SE_Soybean_China.pdf (Samba Export, Abril 2026)
# ─────────────────────────────────────────────────────────────
DOC_CHN = "20260419_SE_Soybean_China.pdf"

KNOWLEDGE_CHUNKS += [
    chunk(DOC_CHN, 0,
        """PRECOS CIF SOJA GMO — DESTINO CHINA (Abril 2026, Base CBOT Maio '26)
Portos: Qingdao · Tianjin · Guangzhou · Ningbo · Dalian · Zhanjiang

| Volume Mensal | Volume Anual     | Tipo de Navio        | Preco CIF     |
|---------------|------------------|----------------------|---------------|
| 1.000 MT      | 12.000 MT/ano    | Container            | USD 580 /MT   |
| 2.000 MT      | 24.000 MT/ano    | Container            | USD 560 /MT   |
| 12.500 MT     | 150.000 MT/ano   | Handymax / Supramax  | USD 535 /MT   |
| 25.000 MT     | 300.000 MT/ano   | Handymax / Supramax  | USD 525 /MT   |
| 50.000 MT     | 600.000 MT/ano   | Panamax              | USD 510 /MT   |
| 100.000 MT    | 1.200.000 MT/ano | Panamax              | USD 495 /MT  ★ Melhor Valor |

Comparativo vs concorrentes (100k MT/ano, CIF China):
  Brasil (Samba Export): USD 495/MT  ✓ MELHOR VALOR
  USA:                   ~USD 510/MT  (tarifa 13% vs Brasil 3%) — premium +3%
  Argentina:             ~USD 520/MT  — premium +5%

Precos indicativos base CBOT Maio '26. Preco final fixado na assinatura."""),

    chunk(DOC_CHN, 1,
        """LOGISTICA BRASIL → CHINA — TEMPOS DE TRANSITO (2026)

ARCO NORTE (Itaqui / Outeiro-Belem):
  • Hub multimodal: rodoviario, ferroviario e fluvial
  • Terminal proprio em Outeiro — zero demurrage
  • Instalacoes certificadas MAPA/GACC
  Tempos de transito:
    Qingdao / Tianjin / Dalian   : 28–35 dias
    Guangzhou / Zhanjiang         : 30–37 dias
    Ningbo / Shanghai             : 29–36 dias

SANTOS / PARANAGUA (Rota Sul):
  Tempos de transito:
    Qingdao / Tianjin / Dalian   : 38–45 dias
    Guangzhou / Zhanjiang         : 40–48 dias
    Ningbo / Shanghai             : 38–46 dias

VANTAGEM ARCO NORTE: economiza 7–10 dias vs Santos/Paranagua para portos chineses.
Frete Arco Norte → China: USD 40–55/MT (competitivo)"""),

    chunk(DOC_CHN, 2,
        """MERCADO DE SOJA CHINA — INTELIGENCIA COMERCIAL 2025/2026

Importacoes anuais totais: ~100 Mt/ano (maior importador mundial)
Crescimento projetado 2025/26: +3–5%
Uso: Racao 60% | Oleo 20% | Alimentacao humana 20%

Market share do Brasil: ~70% (~70 Mt/ano para a China)
Exportacoes Brasil→China em 2025: 111,8 Mt (recorde); Participacao: 73,6%
Participacao USA: 15% (caindo) | Argentina: 7,1%

Vantagem tarifaria: Brasil paga 3% MFN; USA paga 13% (3% + 10% retaliatorio)
Diferencial de preco: soja brasileira USD 452/MT vs soja americana USD 500/MT = USD -48/MT mais barata
Tendencia: 93% das exportacoes brasileiras de setembro/2025 foram para a China

Principais compradores chineses:
  SOEs (Estatais): COFCO, Sinograin, SDIC Agri, Jiusan Group, Dongling, Hopefull,
    Dalian Huanong, Shandong Bohai
  Multinacionais: Louis Dreyfus, Cargill, ADM, Bunge, Wilmar International (top buyer)

Regulatoria: Conformidade total com MARA/GACC
Eventos GMO aprovados para China: MON89788, MON87708, GTS 40-3-2, MON87705,
  MON87701, A2704-12, MON87769, DP-305423-1

Preferencias: DLC/Panamax contratos anuais padrao; GACC pre-registro obrigatorio;
Banco aceito: ICBC, BOC, ABC, CCB (LC at sight)

Producao Brasil 2025/26: 175+ Mt (garante disponibilidade ate Q3/2026)
CBOT Maio '26 subindo (+7,8% no ultimo mes) — niveis de base atrativos"""),
]


# ─────────────────────────────────────────────────────────────
# BLOCO 3: PRECOS CIF — SOJA GMO — TAILANDIA
# Fonte: 20260419_SE_Soybean_Thailand.pdf (Samba Export, Abril 2026)
# ─────────────────────────────────────────────────────────────
DOC_THA = "20260419_SE_Soybean_Thailand.pdf"

KNOWLEDGE_CHUNKS += [
    chunk(DOC_THA, 0,
        """PRECOS CIF SOJA GMO — DESTINO TAILANDIA (Abril 2026, Base CBOT Maio '26)
Portos: Laem Chabang · Bangkok Area · Map Ta Phut

| Volume Mensal | Volume Anual     | Tipo de Navio        | Preco CIF     |
|---------------|------------------|----------------------|---------------|
| 1.000 MT      | 12.000 MT/ano    | Container            | USD 580 /MT   |
| 2.000 MT      | 24.000 MT/ano    | Container            | USD 560 /MT   |
| 12.500 MT     | 150.000 MT/ano   | Handymax / Supramax  | USD 535 /MT   |
| 25.000 MT     | 300.000 MT/ano   | Handymax / Supramax  | USD 525 /MT   |
| 50.000 MT     | 600.000 MT/ano   | Panamax              | USD 510 /MT   |
| 100.000 MT    | 1.200.000 MT/ano | Panamax              | USD 495 /MT  ★ Melhor Valor |

Comparativo vs concorrentes (100k MT, CIF Tailandia):
  Brasil (Samba Export): USD 495/MT  ✓ MELHOR VALOR
  USA:                   USD 505–520/MT — premium +5%
  Argentina:             USD 515–535/MT — premium +11%

SISTEMA DE 16 IMPORTADORES LICENCIADOS (Thai WTO quota):
  TVO (Thai Vegetable Oil) + Thanakorn: dominam 65% do volume de importacao
  CPF (Charoen Pokphand Foods) e Betagro: compradores estrategicos
  TFMA (Thai Feed Mill Association): gateway para toda a rede"""),

    chunk(DOC_THA, 1,
        """LOGISTICA BRASIL → TAILANDIA — TEMPOS DE TRANSITO (2026)

ARCO NORTE (Itaqui / Outeiro-Belem):
  Laem Chabang (principal) : 32–42 dias
  Bangkok Area              : 30–36 dias
  Map Ta Phut               : 29–35 dias

SANTOS / PARANAGUA (Rota Sul):
  Laem Chabang (principal) : 38–45 dias
  Bangkok Area              : 39–46 dias
  Map Ta Phut               : 38–45 dias

VANTAGEM ARCO NORTE: economiza ~6–9 dias vs Santos/Paranagua para portos tailandeses.
Frete competitivo: USD 45–65/MT"""),

    chunk(DOC_THA, 2,
        """MERCADO DE SOJA TAILANDIA — INTELIGENCIA COMERCIAL 2025/2026

Volume anual de importacao: 3,8 Mt/ano (projecao MY 2025/26)
Market share Brasil: 84–90% (dominante)
Uso: Racao 75% | Oleo 18% | Alimentacao humana 7%

Sistema regulatorio: 16 Importadores Licenciados pelo framework WTO quota Thai MoAC
  TVO & Thanakorn: capacidade de aplastamento 12.500 t/dia — dominam 65% do volume
  CPF (Charoen Pokphand Foods): maior produtor de aves do mundo
  Betagro: integrador avicola verticalizado

Regulatoria: Conformidade total Thai MoAC (Ministerio da Agricultura e Cooperativas)
Eventos GMO aprovados: MON89788, MON87708, MON87705, MON87769,
  GTS 40-3-2, MON87701, A2704-12, DP-305423-1

Setores compradores:
  Avicultura: maior setor consumidor; CPF, Betagro — demanda alta proteina constante
  Aplastamento: TVO & Thanakorn com grandes capacidades instaladas
  Alimentos: ~7% de soja inteira para alimentos (tofu, leite de soja, proteinas vegetais)
  Demanda Non-GMO crescente no segmento food

Preferencias comerciais: contratos anuais DLC, Panamax/Handymax"""),
]


# ─────────────────────────────────────────────────────────────
# BLOCO 4: MILHO AMARELO — SCO PETROVIETNAM (ROKANE)
# Fonte: SCO_ROKA_YELLOW CORN PetroVietnam.pdf
# ─────────────────────────────────────────────────────────────
DOC_CORN = "SCO_ROKA_YELLOW_CORN_PetroVietnam.pdf"

KNOWLEDGE_CHUNKS += [
    chunk(DOC_CORN, 0,
        """MILHO AMARELO — PRECOS CIF VIETNA (SCO Rokane, Abril 2026)
Vendedor: Rokane International Business Ltd (parceiro comercial)
Produto: Yellow Corn (Milho Amarelo) | Origem: Brasil | CIF Incoterms 2020

Transacao 1 (pequena):
  Quantidade: 25.000 MT (unica entrega)
  Preco: USD 230,00 /MT CIF
  Valor total: USD 5.750.000,00
  Porto de descarga: Vietnam Port
  Pagamento: LC transferivel, irrevogavel, divisivel, operacional, cash-backed
    Top 50 banco, validade 90 dias, emitido em 10 dias apos assinatura

Transacao 2 (contrato anual):
  Quantidade: 360.000 MT (12 entregas de 30.000 MT)
  Preco: USD 241,00 /MT CIF
  Valor total: USD 86.760.000,00
  Porto de descarga: Phu My Port (VNPHU), Vietnam
  Pagamento: LC at sight, transferivel, irrevogavel, divisivel
    Top 50 banco; pagamento 100% no porto de carregamento contra SGS + Draft B/L

Termos comuns:
  Inspecao: CCIC ou equivalente (porto de carregamento)
  Seguro: 110% do valor da fatura (em nome do comprador)
  Performance Bond: nao especificado neste SCO
  Arbitragem: nao especificado; lei aplicavel brasileira

Comprador: PetroVietnam Oil Corporation (PV Oil)
  CNPJ/Reg: 0305795054
  Endereco: Floor 14-18, PetroVietnam Building, No. 1-5 Le Duan, Sai Gon Ward, Ho Chi Minh City"""),
]


# ─────────────────────────────────────────────────────────────
# BLOCO 5: ETANOL DE CANA — VIETNAM
# Fonte: 04_StartCo_Vietnam_Ethanol.pptx
# ─────────────────────────────────────────────────────────────
DOC_ETH = "04_StartCo_Vietnam_Ethanol.pptx"

KNOWLEDGE_CHUNKS += [
    chunk(DOC_ETH, 0,
        """ETANOL DE CANA BRASILEIRA — MERCADO VIETNA 2025

Mercado vietnamita de etanol: USD 160,7M (2024); CAGR +4,08% ate 2033 → USD 234,7M
Mandato E5 ativo; expansao planejada para E10
Meta NDC Vietnam: reducao 43,5% GHG ate 2030 — etanol e alavanca-chave

Vantagens do etanol brasileiro:
  - Menor pegada de carbono: 70% menos GHG vs gasolina (melhor globalmente)
  - Maior eficiencia: EROI 8:1 (vs 1,3:1 etanol de milho EUA)
  - Certificacoes: RenovaBio, ISCC, Bonsucro (EU/UN/ICAO compliance)
  - Brasil = 2o maior produtor mundial (EUA + Brasil = 80% oferta global)

Especificacoes:
  Etanol Anidro (blend E5/E10):
    Pureza: min 99,6% (ASTM D4806) | Agua: max 0,4%
    Densidade: 0,7893 a 20°C | App: blending E5/E10/E27+
    Vol. minimo: 500 MT por embarque | Embalagem: ISO flexitanks

  Etanol Hidratado (flex-fuel/industrial):
    Pureza: min 95,1% (ABNT NBR 5992) | Agua: max 4,9%
    App: veiculos flex, solvente industrial, farma, sanitizantes

Logistica:
  Rota: Santos/Paranagua → Cingapura/Ho Chi Minh City
  Transito: ~20–25 dias CIF portos vietnamitas

Processo de compra Vietnam:
  1. LOI com tipo (anidro/hidratado), volume, porto (HCMC/Hai Phong)
  2. KYC/DD — licenca importacao, capacidade financeira, uso final
  3. Match com usina brasileira certificada RenovaBio/ISCC
  4. FCO + Certificados → SPA + Embarque"""),
]


# ─────────────────────────────────────────────────────────────
# BLOCO 6: 20 COMPRADORES MEXICO — LISTA COMPLETA
# Fonte: 1UTC6CWQK9tUVAwR8... (Google Doc) — Rokane/Samba Export
# ─────────────────────────────────────────────────────────────
DOC_MEX_BUYERS = "COMPRADORES_MEXICO_2026_500kMT.gdoc"

KNOWLEDGE_CHUNKS += [
    chunk(DOC_MEX_BUYERS, 0,
        """ROTEIRO COMERCIAL — 20 COMPRADORES SOJA MEXICO 2026
Produto: 500.000 MT Soja Brasileira | Safra 2025/26 | Origem Maranhao/MATOPIBA
Contato Samba/Rokane: Leonardo Brunelli, Export Manager | bdm@rokane.com.br | +55 13 99140-5566

==== TIER A — 7 EMPRESAS (GRANDES VOLUMES) ====

#1 — Bunge Mexico S.A. de C.V. [TIER A]
  Tipo: Aplastador & Trader de Graos | Porto: Veracruz / Tampico | Volume: 1.000.000 MT/ano
  Contato: Carlos Mendoza Garcia — Director de Compras — Granos y Oleaginosas
  Email: compras.granos@bunge.com | WhatsApp: +52 55 5000-1234
  Navio: Panamax (75.000–100.000 MT)
  Angulo: Maior aplastador de soja no Mexico. Prioridade #1. Transito 18-25 dias Itaqui→Golfo.
  Argumento: Escala Panamax + transito otimizado Arco Norte + preco competitivo safra recorde

#2 — Cargill Mexico S.A. de C.V. [TIER A]
  Tipo: Aplastador, Trader & Processador | Porto: Veracruz / Manzanillo | Volume: 800.000 MT/ano
  Contato: Alejandro Torres Ruiz — VP de Ingredientes y Granos
  Email: granos.mexico@cargill.com | WhatsApp: +52 55 1234-5678
  Navio: Handymax + Panamax | Planta em Guadalajara + Veracruz
  Angulo: Diversificacao desde USA + preco CIF ~6% menor + cobertura Veracruz e Manzanillo

#3 — ADM Mexico (Archer Daniels Midland) [TIER A]
  Tipo: Aplastador & Trader Internacional | Porto: Veracruz / Altamira | Volume: 600.000 MT/ano
  Contato: Maria Fernandez Lopez — Director de Compras Internacionales
  Email: compras.intl@adm.com | WhatsApp: +52 55 9876-5432
  Navio: Panamax (75–100k MT) + Handymax (35–60k MT)
  Angulo: Comprador ativo de Brasil + preco recorde + CIF Veracruz/Altamira + Panamax/Handymax

#4 — Proteinas del Pacifico S.A. (ProPac) [TIER A]
  Tipo: Aplastador — Jalisco/Occidente | Porto: Manzanillo | Volume: 400.000 MT/ano
  Contato: Roberto Jimenez Castillo — Gerente de Compras de Materias Primas
  Email: materias.primas@propac.com.mx | WhatsApp: +52 33 1234-9876
  Navio: Handymax + Panamax | Aceita Non-GMO tambem
  Angulo: Rota Pacifico a Manzanillo + opcao No-GMO + aplastador #1 occidente

#5 — Industrias Bachoco S.A.B. de C.V. [TIER A]
  Tipo: Avicultura Integrada — Feed | Porto: Veracruz / Tampico | Volume: 300.000 MT/ano
  Contato: Luis Angel Morales Vega — Director de Compras Agricolas
  Email: compras.agricolas@bachoco.net | WhatsApp: +52 71 7000-2000
  Navio: Handymax regulares (35–60k MT)
  Angulo: Maior empresa avicola do Mexico. Importador frequente. Excelente solvencia.

#6 — Pilgrim's Pride Mexico (subsidiaria JBS) [TIER A]
  Tipo: Aves & Proteina Animal | Porto: Veracruz | Volume: 250.000 MT/ano
  Contato: Ana Cristina Vasquez — Sourcing Manager — Feed Ingredients
  Email: feedingredients@ppc.com.mx | WhatsApp: +52 55 4444-5555
  Navio: Handymax regulares (35–60k MT)
  Angulo: Padrao global JBS + solvencia + alimento balanceado avicola + Veracruz

#7 — Tyson Foods Mexico [TIER A]
  Tipo: Proteina Animal Integrada | Porto: Veracruz / Tampico | Volume: 200.000 MT/ano
  Contato: Jorge Espinoza Guerrero — Gerente de Ingredientes — Mexico
  Email: ingredients.mexico@tyson.com | WhatsApp: +52 55 7777-8888
  Navio: Handymax + Panamax
  Angulo: Diversificacao desde USA + risco cadeia norte-americana + preco Brasil ~6% menor"""),

    chunk(DOC_MEX_BUYERS, 1,
        """COMPRADORES SOJA MEXICO 2026 — TIER B (7 empresas)

#8 — Sigma Alimentos S.A. de C.V. (Grupo ALFA) [TIER B]
  Tipo: Processadora de Alimentos | Porto: Veracruz / Tampico | Volume: 150.000 MT/ano
  Contato: Claudia Hernandez Ramos — Director de Abastecimiento Global
  Email: abastecimiento@sigma-alimentos.com | WhatsApp: +52 81 8000-6000
  Navio: Handymax | Excelente historico de credito (Grupo ALFA)
  Angulo: Proteina premium carnes frias + solvencia ALFA Group + certificacoes Halal/Kosher

#9 — Aceitera El Dorado S.A. de C.V. [TIER B]
  Tipo: Aplastador de Oleaginosas — Nordeste Mexico | Porto: Tampico / Altamira
  Volume: 100.000 MT/ano
  Contato: Hector Ortiz Fuentes — Gerente de Compras
  Email: compras@aceitera-eldorado.com.mx | WhatsApp: +52 83 1234-0000
  Navio: Handymax (35.000–60.000 MT)
  Angulo: Handymax Tampico + oleo >=18% para extracao + aplastador regional Nordeste

#10 — Molinos del Papaloapan S.A. [TIER B]
  Tipo: Processador de Graos — Veracruz | Porto: Veracruz | Volume: 80.000 MT/ano
  Contato: Francisco Reyes Alvarado — Director General
  Email: direccion@molinopapaloapan.com.mx | WhatsApp: +52 27 8123-4567
  Navio: Handymax regulares
  Angulo: Relacao longo prazo + Veracruz + fornecimento regular + preco estabilizavel

#11 — SEISA — Soya Especial Industrial S.A. [TIER B]
  Tipo: Processador Especializado de Soja | Porto: Tampico | Volume: 70.000 MT/ano
  Contato: Patricia Guerrero Luna — Gerente de Importaciones
  Email: importaciones@seisa.com.mx | WhatsApp: +52 83 9876-1234
  Navio: Handymax
  Angulo: Soja Non-GMO alimentacao humana + documentacao premium + Tampico + nicho estavel

#12 — Impulsora Agricola S.A. de C.V. [TIER B]
  Tipo: Trader & Distribuidor Agricola | Porto: Veracruz | Volume: 60.000 MT/ano
  Contato: Eduardo Mendez Torres — Director Comercial
  Email: comercial@impulsora-agricola.com.mx | WhatsApp: +52 22 9000-1111
  Navio: Container + Handymax
  Angulo: Alianca de distribuicao + precos para trading + porta de entrada ao mercado mexicano

#13 — Grupo Lala S.A.B. de C.V. [TIER B]
  Tipo: Industria Lactea — Feed Inputs | Porto: Veracruz / Tampico | Volume: 50.000 MT/ano
  Contato: Ricardo Sanchez Moreno — Jefe de Compras Agropecuarias
  Email: compras.agro@grupolala.com.mx | WhatsApp: +52 47 7000-0000
  Navio: Handymax ou Container
  Angulo: Alimento bovino leiteiro + estabilidade + solvencia Lala + fornecimento recorrente

#14 — Alpura S.A. de C.V. (Cooper) [TIER B]
  Tipo: Cooperativa Lactea | Porto: Veracruz / Tampico | Volume: 80.000 MT/ano
  Contato: Sandra Rios Espino — Coordinador de Materias Primas
  Email: materias.primas@alpura.com.mx | WhatsApp: +52 55 2000-3333
  Navio: Handymax (35–60k MT) por carregamento
  Angulo: 2-3 carregamentos anuais programados + preco forward + cooperativa leiteira confiavel"""),

    chunk(DOC_MEX_BUYERS, 2,
        """COMPRADORES SOJA MEXICO 2026 — TIER C (6 empresas)

#15 — Minsa Corporation (subsidiaria GRUMA) [TIER C]
  Tipo: Processadora de Graos | Porto: Veracruz | Volume: 40.000 MT/ano
  Contato: Guillermo Castro Pena — Gerente de Compras de Insumos
  Email: insumos@minsa.com.mx | WhatsApp: +52 81 5000-4444
  Navio: Container → Handymax (escalavel)
  Angulo: Diversificacao proteina GRUMA + volume escalavel container→Handymax + Veracruz

#16 — Agropecuaria La Magdalena S.A. [TIER C]
  Tipo: Integrador Pecuario — Alimento | Porto: Tampico / Altamira | Volume: 50.000 MT/ano
  Contato: Manuel Gutierrez Ramos — Director de Abasto
  Email: abasto@agromag.com.mx | WhatsApp: +52 83 6789-0000
  Navio: Handymax (35–60k MT)
  Angulo: Integrador porcicola/avicola regional + Tampico + alimento balanceado preco competitivo

#17 — Proteinas de Occidente S.A. [TIER C]
  Tipo: Processador de Proteinas — Jalisco | Porto: Manzanillo | Volume: 60.000 MT/ano
  Contato: Laura Villanueva Cruz — Jefe de Compras
  Email: compras@proteinas-occidente.com.mx | WhatsApp: +52 33 5678-9012
  Navio: rota Pacifico
  Angulo: Soja Non-GMO + Manzanillo rota Pacifico + Guadalajara/Jalisco

#18 — Sinergia Animal S.A. de C.V. [TIER C]
  Tipo: Nutricao Animal — Feed Mill | Porto: Veracruz | Volume: 30.000 MT/ano
  Contato: Arturo Morales Vega — Gerente Comercial
  Email: comercial@sinergiaanimal.com.mx | WhatsApp: +52 22 3456-7890
  Navio: Container (piloto), depois crescimento
  Angulo: Primeiro container piloto + alta fidelidade + crescimento gradual + Veracruz

#19 — Nutricion y Bienestar Animal (NBA) [TIER C]
  Tipo: Feed Mill — Centro-Sul Mexico | Porto: Veracruz / Coatzacoalcos | Volume: 25.000 MT/ano
  Contato: Karla Perez Dominguez — Responsable de Compras
  Email: compras@nba-animal.com.mx | WhatsApp: +52 92 1234-5678
  Navio: Container + pequeno Handymax
  Angulo: Alta lealdade + primeiro envio container/Handymax + relacao de longo prazo

#20 — Agroindustrias Bonanza S.A. [TIER C]
  Tipo: Agroindustrial Regional | Porto: Tampico | Volume: 20.000 MT/ano
  Contato: Oswaldo Flores Bravo — Director General
  Email: dgral@bonanza-agro.com.mx | WhatsApp: +52 83 0987-6543
  Navio: Container ou Handymax pequeno
  Angulo: Primeiro contato Brasil + empresa crescente + Tampico + potencial longo prazo

SEGMENTACAO TOTAL (20 empresas):
  Tier A (7 empresas): 200k–1M MT/ano | Bunge, Cargill, ADM, ProPac, Bachoco, Pilgrim's, Tyson
  Tier B (7 empresas): 50k–150k MT/ano | Sigma, El Dorado, Molinos, SEISA, Impulsora, Lala, Alpura
  Tier C (6 empresas): 20k–60k MT/ano  | Minsa, La Magdalena, Proteinas Occ., Sinergia, NBA, Bonanza

Termos comerciais para todos:
  Incoterm: CIF | Inspecao: SGS / Bureau Veritas | Contrato: GAFTA 100 / FOSFA 54
  Pagamento: DLC irrevogavel | Contratos forward: Q2, Q3, Q4/2026 disponiveis
  Vantagem de preco vs USA: ~6% menor esta safra"""),
]


# ─────────────────────────────────────────────────────────────
# BLOCO 7: ESPECIFICACOES TECNICAS CONSOLIDADAS
# ─────────────────────────────────────────────────────────────
DOC_SPECS = "SAMBA_EXPORT_SPECS_CONSOLIDADO_2026.internal"

KNOWLEDGE_CHUNKS += [
    chunk(DOC_SPECS, 0,
        """ESPECIFICACOES TECNICAS — SOJA BRASILEIRA (PADRAO SAMBA EXPORT 2026)
Produto: Soja Amarela (Glycine max) | Origem: Maranhao, MATOPIBA
Variedade: GMO disponivel (todos os eventos aprovados); Non-GMO disponivel

Especificacoes padrao (validas para todos os destinos):
  Umidade (Moisture)        : max 14,0%
  Proteina (dry basis)      : min 36,0% (Samba/FCO usa 34-38% como range; min comercial 36%)
  Teor de oleo (Oil Content): min 18,0%
  Materia estranha (FM)     : max 1,0%
  Graos danificados (DG)    : max 8,0% (com max 2% ardidos)
  Mistura (Admixture)       : max 2,0%
  Peso hectolitrico         : min 69 kg/hl (min comercial Samba)
  Test weight               : min 54 lbs/bushel
  Aflatoxinas               : max 20 ppb
  Salmonella                : Ausente em 25g
  Safra/Crop Year           : 2025/26

Normas: GAFTA 100 | ANEC | Decreto 9523/2018

Inspecao: SGS / CCIC / Bureau Veritas / Intertek
  Local: Porto de embarque (50/50 custo entre vendedor e comprador)
  Certificados: Certificate of Quality, Certificate of Quantity,
    Phytosanitary Certificate, Certificate of Origin

Seguro: 110% do valor da fatura CIF | Institute Cargo Clauses A (All Risks)
  Beneficiario: Comprador

Eventos GMO aprovados para exportacao (todos os destinos cobertos):
  MON89788 (Genuity RR2 Yield), MON87708 (RR2 Xtend), MON87705 (Vistive Gold),
  MON87769 (Omega-3), GTS 40-3-2 (Roundup Ready), MON87701 (Insect Resistance),
  A2704-12 (Liberty Link), DP-305423-1 (High Oleic)

Regulatoria por destino:
  China: MARA/GACC pre-registro obrigatorio
  Mexico: SAGARPA/SENASICA — todos os eventos acima aprovados
  Tailandia: Thai MoAC — todos os eventos acima aprovados
  Vietnam: MoARD — conformidade com normas vietnamitas"""),

    chunk(DOC_SPECS, 1,
        """CERTIFICACOES E CAPACIDADE OPERACIONAL — SAMBA EXPORT 2026

Capacidade de exportacao: 500.000 MT/ano
Armazenagem: silos proprios e parceiros com capacidade 3M+ MT
Area agricola propria/parceiros: 37.000 ha de soja (Maranhao/MATOPIBA)

Certificacoes mantidas:
  FOSFA International (oleos e oleaginosas)
  RTRS — Round Table on Responsible Soy (rastreabilidade, zero desmatamento)
  ISO 9001:2015 (Qualidade)
  ISO 14001:2015 (Meio Ambiente)
  ISO 22000:2018 (Seguranca Alimentar)
  HACCP (Hazard Analysis and Critical Control Points)
  Halal (mercados islamicos — Oriente Medio, Sudeste Asiatico)
  Kosher (mercados judaicos)
  GACC (registro no sistema chines de controle alfandegario)
  SAGARPA (aprovacao mexicana GMO — todos os eventos)

Portos operados:
  Arco Norte (preferencial): Itaqui (Sao Luis/MA), Outeiro-Belem (PA)
    Terminal proprio em Outeiro — zero demurrage garantido
  Rota Sul (alternativo): Santos/SP, Paranagua/PR, Rio Grande/RS

Parceiro estrategico: Rokane International Business Ltd
  Responsavel: Fabricio Gardin da Rocha (CEO)
  CNPJ: 04.702.383/0001-09 | bdm@rokane.com.br | +55 51 981340773
  Endereco: Arizona Street, 491/8o andar, Brooklin, Sao Paulo/SP"""),
]


# ─────────────────────────────────────────────────────────────
# BLOCO 8: INTELIGENCIA COMPETITIVA — BRASIL VS CONCORRENTES
# Fonte: SE_Soybean_China_2026.pptx + PDFs Mexico/China/Thailand
# ─────────────────────────────────────────────────────────────
DOC_COMP = "COMPETITIVE_INTELLIGENCE_SAMBA_2026.internal"

KNOWLEDGE_CHUNKS += [
    chunk(DOC_COMP, 0,
        """VANTAGENS COMPETITIVAS DO BRASIL — SOJA 2026

vs. USA:
  - Tarifa China: Brasil paga 3% MFN; USA paga 13% (3%+10% retaliatorio) → diferencial USD 48/MT
  - Tarifa Mexico: Brasil paga tarifa preferencial; USA corre risco geopolitico crescente
  - Preco CIF China: Brasil USD 452/MT vs USA USD 500/MT → Brasil 10% mais barato
  - Risco geopolitico: Brasil ZERO risco; USA: Alta exposicao guerra comercial
  - Alianca BRICS: parceria China-Brasil se aprofundando (portos, ferrovia, infraestrutura)
  - Safra contrassazonal: Jan-Jul (quando safra EUA termina) = seguranca de fornecimento

vs. Argentina:
  - Logistica: Brasil embarques mais rapidos, menores custos de transito
  - Estabilidade economica: Argentina tem riscos cambiais e politicos; Brasil mais estavel
  - Preco CIF (100k MT): Brasil USD 495/MT vs Argentina ~USD 515-520/MT → Brasil -5% a -11%
  - Certificacoes: Brasil com cobertura mais ampla de certificacoes internacionais

Producao Brasil 2025/26: 175+ Mt (estimativa; recorde historico)
Exportacoes para China 2025: 82,33 Mt (+10,3%); EUA para China: 16,8 Mt (-24,1%)
Participacao de mercado Brasil na China: 73,6% (vs 71% em 2024)

Sazonalidade:
  Jan-Jul: Janela de exportacao otima (colheita ativa → plena capacidade)
  Ago-Set: Reducao gradual de estoques
  Out-Dez: Entressafra (USA assume; premium de preco para soja brasileira)"""),
]


# ─────────────────────────────────────────────────────────────
# BLOCO 9: PROCEDIMENTO COMERCIAL — PROCESSO PADRAO SAMBA
# ─────────────────────────────────────────────────────────────
DOC_PROC = "SAMBA_EXPORT_PROCESSO_COMERCIAL_2026.internal"

KNOWLEDGE_CHUNKS += [
    chunk(DOC_PROC, 0,
        """PROCESSO COMERCIAL PADRAO — SAMBA EXPORT (FCO Rokane base 2026)

Sequencia obrigatoria de documentos:
  1. CIS/KYC — Company Information Sheet do comprador (12 blocos de verificacao)
  2. LOI — Letter of Intent (comprador envia; validade 3 dias)
  3. FCO — Full Corporate Offer (vendedor emite; validade 5 dias uteis)
  4. POF — Proof of Funds (comprador apresenta para ativar Soft Probe)
  5. SPA — Sale & Purchase Agreement (contrato definitivo)
  6. Draft DLC — Minuta da Carta de Credito para aprovacao
  7. PI — Proforma Invoice
  8. DLC Operativa — Emitida pelo banco do comprador (Top 50/100) em 7-10 dias
  9. Embarque — Apos confirmacao da DLC; seller envia em 25-40 dias

Documentos de embarque exigidos:
  (1) Commercial Invoice (Fatura Comercial)
  (2) Full Set Bill of Lading (B/L)
  (3) Packing List
  (4) Certificate of Origin (Brasil)
  (5) Phytosanitary Certificate (MAPA)
  (6) SGS Certificate of Quality
  (7) SGS Certificate of Quantity
  (8) Insurance Certificate (110% CIF)
  (9) Fumigation Certificate
  (10) Certificate of Weight
  (11) Non-GMO Certificate (se aplicavel)

Performance Bond:
  Valor: 2% do valor total do contrato
  Instrumento: TT (Telegraphic Transfer)
  Momento: Apos assinatura do SPA, antes do embarque
  Penalidade por inadimplencia: performance bond retido + arbitragem GAFTA

Pagamento (DLC/UCP 600):
  Tipo: Irrevogavel, Transferivel, Divisivel, Totalmente Operativo
  Banco emitente: Top 50 ou Top 100 mundial (por ativos — ranking Bloomberg/FT)
  Prazo emissao apos SPA: 7 dias (max 10 dias)
  Validade minima DLC: 90 dias
  Pagamento: 100% no porto de embarque contra documentos SGS + Draft B/L

DLC SBLC: Disponivel como alternativa; diferencial de preco aplicavel"""),

    chunk(DOC_PROC, 1,
        """KYC 12 BLOCOS — CRITERIOS DE QUALIFICACAO DO COMPRADOR (Samba Export)

Para receber FCO, o comprador deve apresentar CIS com:
  1. Razao social completa e numero de registro corporativo
  2. Pais de constituicao e endereco sede
  3. Representante legal (nome, cargo, passaporte)
  4. Contato comercial (nome, cargo, email, telefone)
  5. Coordenadas bancarias (banco, agencia, conta, SWIFT/BIC, IBAN)
  6. Carta de conforto bancario (BCL) ou Proof of Funds
  7. Historico de importacoes (commodity, volumes, datas)
  8. Licencas de importacao validas (GACC/SAGARPA/MoAC conforme destino)
  9. Beneficiario final (UBO — Ultimate Beneficial Owner)
  10. Estrutura corporativa / organograma
  11. Referencias comerciais (2-3 fornecedores internacionais)
  12. Declaracao de uso final (feed/food/crushing/processing)

Prazo verificacao KYC: ate 48 horas uteis
Criterios de rejeicao automatica: sancoes, opacidade de propriedade, inconsistencia historico

Processo de prospeccao ativa (pelo agente):
  Email inicial → Follow-up D+3 → Chamada qualificacao → CIS/KYC → FCO
  Plataformas: email + LinkedIn + WhatsApp (conforme contato de cada empresa)"""),
]


# ─────────────────────────────────────────────────────────────
# BLOCO 10: DADOS DE MERCADO — MACRO E BOLSAS
# Fonte: 02_StartCo_China_Soja.pptx
# ─────────────────────────────────────────────────────────────
DOC_MACRO = "MARKET_INTELLIGENCE_MACRO_ABRIL2026.internal"

KNOWLEDGE_CHUNKS += [
    chunk(DOC_MACRO, 0,
        """DADOS DE MERCADO — SOJA — ABRIL 2026

CBOT (Chicago Board of Trade) — Contrato Referencia:
  CBOT Soja Maio '26: subindo (+7,8% no ultimo mes); niveis de base atrativos
  Preco referencia CIF China: ~USD 452/MT (soja brasileira) vs USD 500/MT (americana)

CHINA — importacoes 2025:
  Total importado: 111,8 Mt (recorde historico)
  Do Brasil: 82,33 Mt (+10,3% vs 2024) — participacao: 73,6%
  Do EUA: 16,8 Mt (-24,1%)
  Projecao 2025/26: crescimento +3–5%

MEXICO — importacoes projetadas 2025/26:
  Volume total: ~7 Mt/ano | Crescimento: +8%
  Origem principal: USA (90%+) → diversificacao em andamento
  Vantagem brasil: ~6% mais barato que USA nesta safra

TAILANDIA — MY 2025/26:
  Volume anual: 3,8 Mt | Market share Brasil: 84–90%

BRASIL — producao e exportacoes:
  Producao 2025/26: 175+ Mt estimado (recorde)
  Exportacoes fev/2026: 4,08 Mt (forte fluxo antecipado de safra)
  Area MATOPIBA: fronteira agricola em expansao (MA, TO, PI, BA)
  Portos Arco Norte: Itaqui, Outeiro-Belem — crescimento forte 2024–2026

FRETE MARITIMO (referencia Abril 2026):
  Arco Norte → China: USD 40–55/MT
  Arco Norte → Tailandia: USD 45–65/MT
  Arco Norte → Mexico Golfo: incluido no CIF; transito 10–16 dias"""),
]


# ─────────────────────────────────────────────────────────────
# BLOCO 11: ACUCAR — CONHECIMENTO GERAL MERCADO SAMBA
# (dados de mercado conhecidos — a ser enriquecido com PDFs Drive)
# ─────────────────────────────────────────────────────────────
DOC_SUGAR = "ACUCAR_MERCADO_SAMBA_2026.internal"

KNOWLEDGE_CHUNKS += [
    chunk(DOC_SUGAR, 0,
        """ACUCAR REFINADO — PORTFOLIO SAMBA EXPORT 2026

Produtos disponiveis:
  1. ICUMSA 45 — Acucar Branco Refinado (White Crystal Sugar)
     HS Code: 1701.99.90 | Bolsa: ICE SB
     Umidade: max 0,04% | ICUMSA: max 45 | Pol: min 99,8%
     Granulometria: 0,3–1,0mm | SO2: max 20ppm
     Embalagem: Big Bag 1MT ou Sacos 50kg
     Destinos tipicos: Oriente Medio, Africa, Asia, Europa Oriental
     Inspecao: SGS / CCIC | Pagamento: DLC Top 25 Banco Mundial

  2. ICUMSA 150 — Acucar Cristal (Crystal Sugar)
     Qualidade intermediaria; uso industrial e consumo
     ICUMSA: max 150 | Pol: min 99,5%
     Embalagem: Big Bag 1MT, Sacos 50kg

  3. ICUMSA 600-1200 — VHP (Very High Polarization) — Acucar Bruto/Demerara
     Exportacao em granel (bulk vessels)
     ICUMSA: 600–1200 | Pol: min 99,2%
     Navio: Handymax ou Panamax (bulk)
     Destinos: refinarias mundiais (Oriente Medio, India, China)

Volumes tipicos por navio:
  Container (FCL): 22 MT por container
  Handymax: 25.000–50.000 MT (bulk/bag)
  Panamax: 50.000–75.000 MT (bulk)

Portos de embarque acucar:
  Santos/SP | Paranagua/PR | Maceio/AL | Recife/PE
  (Arco Norte menos usado para acucar — concentrado em Santos/PR)

Normas: CODEX STAN 212-1999 | EU Directive 2001/111/EC | ABNT NBR 15.635
Inspecao: Pol (Polarimetry), ICUMSA Method GS2-9, Grain Size Distribution

Clientes/Prospects Sugar identificados (Drive):
  Al Khaleej Sugar — UAE (Tier 1) | Template E-01
  Dangote Sugar — Nigeria (Tier 1) | Template E-06 | dangotesugar@dangotesugar.com.ng
  Cevital — Algeria | CAM_2026Q2
  (Ver pasta CAM_2026Q2_ACUCAR_UAE/NIGERIA/ALGERIA no Drive)"""),
]


# ─────────────────────────────────────────────────────────────
# BLOCO 12: TABELA COMPARATIVA CIF — TODOS OS DESTINOS
# (tabela de referencia rapida para agentes)
# ─────────────────────────────────────────────────────────────
DOC_REF = "TABELA_REFERENCIA_CIF_SOJA_GMO_ABR2026.internal"

KNOWLEDGE_CHUNKS += [
    chunk(DOC_REF, 0,
        """TABELA DE REFERENCIA RAPIDA — PRECOS CIF SOJA GMO (Abril 2026)
Base: CBOT Maio '26 | Todos os precos em USD/MT

Volume Mensal → Destino:        Mexico    China    Tailandia   Vietnam*
  1.000 MT/mes  (Container)     $570      $580     $580        $580-600
  2.000 MT/mes  (Container)     $550      $560     $560        $560-580
 12.500 MT/mes  (Handymax)      $525      $535     $535        $535-550
 25.000 MT/mes  (Handymax)      $515      $525     $525        $525-540
 50.000 MT/mes  (Panamax)       $500      $510     $510        $510-525
100.000 MT/mes  (Panamax) ★     $485      $495     $495        $495-510

*Vietnam: preco estimado com base na distancia/frete vs Tailandia; nao ha tabela oficial

Milho Amarelo CIF Vietnam (Rokane, Abril 2026):
  25.000 MT (spot): USD 230/MT
  30.000 MT × 12 (contrato anual 360.000 MT): USD 241/MT

Transit times resumidos (Arco Norte como base):
  → Mexico Golfo (Veracruz/Tampico): 10–16 dias
  → Mexico Pacifico (Manzanillo):    15–22 dias
  → China (Qingdao/Tianjin):         28–35 dias
  → Tailandia (Laem Chabang):        32–42 dias
  → Vietnam (Ho Chi Minh/Phu My):    ~35–45 dias

Nota: Todos os precos sao indicativos. Preco final fixado na assinatura do contrato.
Fonte dos precos: Samba Export pitch books (SE_Soybean_Mexico/China/Thailand.pdf, Abril 2026)"""),
]


# ============================================================
# FUNCAO PRINCIPAL DE INGESTAO
# ============================================================

def ingest_all(clear: bool = False) -> None:
    print(f"\n{'='*60}")
    print(f"SAMBA EXPORT — Knowledge Base Ingestion")
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total de chunks a inserir: {len(KNOWLEDGE_CHUNKS)}")
    print(f"{'='*60}\n")

    # Garantir que as tabelas existem
    create_tables()

    session = get_session()

    try:
        if clear:
            deleted = session.query(CorporateKnowledge).delete()
            session.commit()
            print(f"  [CLEAR] {deleted} registros apagados.\n")

        # Verificar duplicatas por document_name + chunk_index
        existing = {
            (r.document_name, r.chunk_index)
            for r in session.query(
                CorporateKnowledge.document_name,
                CorporateKnowledge.chunk_index
            ).all()
        }

        inserted = 0
        skipped = 0

        for c in KNOWLEDGE_CHUNKS:
            key = (c["document_name"], c["chunk_index"])
            if key in existing:
                skipped += 1
                continue

            record = CorporateKnowledge(
                document_name=c["document_name"],
                chunk_index=c["chunk_index"],
                content=c["content"],
                embedding=c["embedding"],
                token_count=c["token_count"],
            )
            session.add(record)
            inserted += 1

        session.commit()

        print(f"  ✅ Inseridos : {inserted} chunks")
        print(f"  ⏭  Ignorados : {skipped} chunks (ja existiam)")
        print(f"\n  Documentos cobertos:")

        docs_seen = {}
        for c in KNOWLEDGE_CHUNKS:
            dn = c["document_name"]
            docs_seen[dn] = docs_seen.get(dn, 0) + 1

        for doc, count in docs_seen.items():
            print(f"    • {doc} ({count} chunk{'s' if count > 1 else ''})")

        total_tokens = sum(c["token_count"] for c in KNOWLEDGE_CHUNKS)
        print(f"\n  Total de tokens estimados: {total_tokens:,}")
        print(f"\n{'='*60}")
        print("  INGESTAO CONCLUIDA COM SUCESSO")
        print(f"{'='*60}\n")

    except Exception as e:
        session.rollback()
        print(f"\n  ❌ ERRO: {e}")
        raise
    finally:
        session.close()


def verify_ingestion() -> None:
    """Imprime uma amostra do que foi inserido."""
    session = get_session()
    try:
        total = session.query(CorporateKnowledge).count()
        print(f"\n  Total de registros na tabela corporate_knowledge: {total}")

        # Amostras por documento
        docs = session.query(CorporateKnowledge.document_name).distinct().all()
        for (doc,) in docs:
            count = session.query(CorporateKnowledge).filter_by(document_name=doc).count()
            first = session.query(CorporateKnowledge).filter_by(
                document_name=doc, chunk_index=0
            ).first()
            preview = (first.content[:120] + "...") if first else "(vazio)"
            print(f"\n  [{doc}] — {count} chunk(s)")
            print(f"    Preview: {preview}")
    finally:
        session.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingere conhecimento do Google Drive na tabela CorporateKnowledge."
    )
    parser.add_argument(
        "--clear", action="store_true",
        help="Apaga todos os registros existentes antes de inserir."
    )
    parser.add_argument(
        "--verify", action="store_true",
        help="Apenas verifica o que esta no banco (sem inserir)."
    )
    args = parser.parse_args()

    if args.verify:
        verify_ingestion()
    else:
        ingest_all(clear=args.clear)
        verify_ingestion()
