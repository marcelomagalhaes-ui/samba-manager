"""
agents/wpp_enrichment_agent.py
==============================
Agente de Enriquecimento de Dados via WhatsApp
-----------------------------------------------
Lê os dados extraídos das conversas de WhatsApp e preenche SOMENTE
os campos em branco na aba "todos andamento" da planilha Google Sheets.

REGRA FUNDAMENTAL: Nunca sobrescreve célula com conteúdo existente.

Conversas processadas:
  • Eric Shee          → 2026ERIC0001–0005
  • Basis Corretora    → 2026BASIS0001, 2026BASIS002
  • Bahov              → 2026BAH0001–0002
  • Tratto/Cammus      → 2026CAM0001–0004
  • Conex Brasil World → 2026CONEX0001–0002
  • Carlos Bicca       → 2026BUC0001
  • Gui Wicthoffet GWI → 2026GWI0001, 2026GWI0002, 2026GWI003
  • Huggo Fehr/Primex  → 2026PRIMX0001–0006
  • Vilson Curvello    → 2026VIL0001–0005

Uso direto:
    python -X utf8 agents/wpp_enrichment_agent.py

Também disponível como Celery task via task_wpp_enrichment.
"""
from __future__ import annotations

import logging
import os
import sys
from typing import Any

# Garante que a raiz do projeto esteja no path (necessário ao rodar direto)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from googleapiclient.discovery import build

from services.google_drive import drive_manager

logger = logging.getLogger("WppEnrichmentAgent")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

# ─── Planilha ────────────────────────────────────────────────────────────────
SPREADSHEET_ID = os.getenv(
    "SAMBA_SPREADSHEET_ID",
    "1ToNZxYYi0dPQkQ0bRE8W3DWXJxkzEVS4vLQyrj2VP9U",
)
SHEET_TAB = "todos andamento"

# ─── Índices de colunas (0-based, A=0) ───────────────────────────────────────
COL_JOB          = 0   # A — identificador do deal
COL_DATA         = 1   # B — data de entrada
COL_OFERTA       = 2   # C — OFERTA ou PEDIDO
COL_GRUPO        = 3   # D — grupo WhatsApp (chave de agrupamento)
COL_SOLICITANTE  = 4   # E — solicitante
COL_STATUS       = 5   # F — status comercial
COL_PRODUTO      = 6   # G — produto / commodity
COL_COMPRADOR    = 7   # H — empresa compradora
COL_FORNECEDOR   = 8   # I — empresa fornecedora
COL_VIZ          = 9   # J — viz rápida (resumo 1-linha)
COL_ESPECIFICACAO = 11  # L — especificação completa
COL_SITUACAO     = 12  # M — situação atual da negociação
COL_ACAO         = 13  # N — próxima ação
COL_STATUS_AUTO  = 14  # O — status automação

# ─── Status de automação que excluem a linha do processamento ────────────────
SKIP_AUTO_STATUSES = {"REJECTED", "SKIPPED"}

# ─── Mapeamento de GRUPO (col D) → código do JOB ────────────────────────────
# Usado para linhas sem JOB preenchido: atribui JOB sequencialmente por grupo.
GROUP_CODE_MAP: dict[str, str] = {
    "eric":          "ERIC",
    "eric shee":     "ERIC",
    "basis":         "BASIS",
    "basis corretora": "BASIS",
    "bahov":         "BAH",
    "bah":           "BAH",
    "cammus":        "CAM",
    "cam":           "CAM",
    "tratto":        "CAM",
    "conex":         "CONEX",
    "alexandre":     "CONEX",
    "buc":           "BUC",
    "bicca":         "BUC",
    "carlos bicca":  "BUC",
    "gwi":           "GWI",
    "gui":           "GWI",
    "primex":        "PRIMX",
    "primx":         "PRIMX",
    "huggo":         "PRIMX",
    "vilson":        "VIL",
    "vil":           "VIL",
    "akxon":         "VIL",
    "vania":         "VAN",
    "van":           "VAN",
    "maxin":         "MAX",
    "max":           "MAX",
    "oport":         "OPORT",
    "oportunidades": "OPORT",
}

# ─── Duplicatas conhecidas → JOB canônico ────────────────────────────────────
# A coluna N (ACAO) receberá um aviso de duplicata se o JOB estiver em branco.
DUPLICATE_JOB_MAP: dict[str, str] = {
    "WPP_260414_ERIC_SOJA_TESTE": "2026ERIC0001",
}

# ─── Base de Conhecimento Extraída das Conversas de WhatsApp ─────────────────
# Chave: JOB code (exato, case-sensitive)
# Valor: dict col_index → valor a preencher (somente se a célula estiver vazia)
#
# Campos:
#   "oferta"       → COL_OFERTA       (C)  "OFERTA" ou "PEDIDO"
#   "status"       → COL_STATUS       (F)  status comercial
#   "produto"      → COL_PRODUTO      (G)
#   "comprador"    → COL_COMPRADOR    (H)
#   "fornecedor"   → COL_FORNECEDOR   (I)
#   "viz"          → COL_VIZ          (J)  resumo 1 linha
#   "especificacao"→ COL_ESPECIFICACAO(L)  spec completa
#   "situacao"     → COL_SITUACAO     (M)
#   "acao"         → COL_ACAO         (N)

KNOWLEDGE_BASE: dict[str, dict[str, Any]] = {

    # ── ERIC SHEE ──────────────────────────────────────────────────────────────
    "2026ERIC0001": {
        "oferta":        "PEDIDO",
        "produto":       "Soja GMO",
        "comprador":     "BRAKO (Korea)",
        "fornecedor":    "Rokane",
        "viz":           "Soja GMO 50K+200K MT CIF Qingdao DLC 490/520 USD",
        "especificacao": "Volume: 50.000 MT + 200.000 MT | Incoterm: CIF Qingdao | Pgto: DLC | Preço: 490 net / 520 invoice USD | REF 2026NC0001 | Co-ref: WPP_260414_ERIC_SOJA_TESTE (DUPLICATA)",
        "situacao":      "SPA em revisão - Aguardando assinatura BRAKO",
        "acao":          "Aguardar retorno BRAKO / enviar SPA revisado",
    },
    "2026ERIC0002": {
        "oferta":        "PEDIDO",
        "produto":       "Milho",
        "comprador":     "BRAKO (Korea)",
        "fornecedor":    "",
        "viz":           "Milho CIF China - BRAKO - preço a definir",
        "especificacao": "Incoterm: CIF China | Pgto: TBD | Preço: target a definir | REF 2026NC0002",
        "situacao":      "No aguardo - levantando fornecedor e preço",
        "acao":          "Buscar fornecedor de milho para cotação CIF China",
    },
    "2026ERIC0003": {
        "oferta":        "PEDIDO",
        "produto":       "Açúcar IC45",
        "comprador":     "BRAKO (Korea)",
        "fornecedor":    "WRM / CONEX Brasil World",
        "viz":           "Açúcar IC45 200K MT x12 CIF Qingdao SBLC 430 USD",
        "especificacao": "Volume: 1 FCL trial + 200.000 MT x 12 meses | Incoterm: CIF Qingdao | Pgto: SBLC | Preço: 430 net (2,5/2,5 comissão) | Co-seller: CONEX | REF 2026NC0003",
        "situacao":      "SPA em elaboração - BRAKO confirmou interesse",
        "acao":          "Elaborar SPA com WRM/CONEX e enviar para BRAKO",
    },
    "2026ERIC0004": {
        "oferta":        "PEDIDO",
        "produto":       "Açúcar IC45",
        "comprador":     "CELOGIM / LOGIMFARM (Korea)",
        "fornecedor":    "WRM / CONEX Brasil World",
        "viz":           "Açúcar IC45 200K MT x12 CIF Qingdao SBLC 410 USD",
        "especificacao": "Volume: 1 FCL trial + 200.000 MT x 12 meses | Incoterm: CIF Qingdao | Pgto: SBLC | Preço: 410 net (2,5/2,5 comissão) | Co-seller: CONEX | REF 2026NC0004",
        "situacao":      "No aguardo posicionamento CELOGIM/LOGIMFARM",
        "acao":          "Aguardar resposta CELOGIM - enviar SCO/SPA se solicitado",
    },
    "2026ERIC0005": {
        "oferta":        "PEDIDO",
        "produto":       "Açúcar IC45",
        "comprador":     "Ms. JAMAICA (Italy/Nigeria)",
        "fornecedor":    "WRM / CONEX Brasil World",
        "viz":           "Açúcar IC45 50K MT x12 CIF Abidjan SBLC 425 / DLC 435 USD",
        "especificacao": "Volume: 50.000 MT x 12 meses | Incoterm: CIF APA Abidjan (Costa do Marfim) | Pgto: SBLC 425 / DLC 435 net (2,5/2,5 comissão) | Co-seller: CONEX | REF 2026NC0005",
        "situacao":      "No aguardo posicionamento Ms. JAMAICA",
        "acao":          "Aguardar retorno Ms. JAMAICA / enviar SPA",
    },

    # ── BASIS CORRETORA ────────────────────────────────────────────────────────
    "2026BASIS0001": {
        "oferta":        "PEDIDO",
        "produto":       "Soja GMO",
        "comprador":     "Thiago Toledo / Basis Corretora",
        "fornecedor":    "Rokane / Fabricio (bdm@rokane.com.br)",
        "viz":           "Soja GMO 60K MT/mês CIF China 490 USD / FOB Outeiro 445 USD",
        "especificacao": "Volume: 60.000 MT/mês | Incoterm: CIF China / FOB Outeiro | Preço: CIF 490 USD / FOB 445 USD | Fornecedor: Rokane (bdm@rokane.com.br)",
        "situacao":      "No aguardo Basis - update enviado em 22/04",
        "acao":          "Aguardar posicionamento Basis - follow up em 48h",
    },
    "2026BASIS002": {
        "oferta":        "PEDIDO",
        "produto":       "Açúcar IC45",
        "comprador":     "via Basis Corretora",
        "fornecedor":    "",
        "viz":           "Açúcar IC45 12.500 MT CIF China DLC 510 USD",
        "especificacao": "Volume: 12.500 MT | Incoterm: CIF China | Pgto: DLC | Preço: 510 USD",
        "situacao":      "No aguardo Basis",
        "acao":          "Aguardar posicionamento Basis - buscar fornecedor",
    },

    # ── BAHOV ─────────────────────────────────────────────────────────────────
    "2026BAH0001": {
        "oferta":        "PEDIDO",
        "produto":       "Açúcar VHP",
        "comprador":     "ZN Holding / China (via Bahov - CHRISTO BAHIOV LAW FIRM)",
        "fornecedor":    "CONEX Brasil World (co-seller Rokane)",
        "viz":           "Açúcar VHP 300K MT/mês x12 CIF multiportos SBLC",
        "especificacao": "Volume: 300.000 MT/mês x 12 meses | Incoterm: CIF (Grécia/Turquia/Dubai) | Pgto: SBLC / Security Bond | Comprador: ZN Holding China via Bahov intermediário | SPA em revisão",
        "situacao":      "Aguardando - Bahov intermitente (hospitalizações recorrentes causando atrasos)",
        "acao":          "Aguardar retorno Bahov - reenviar SPA para assinatura quando disponível",
    },
    "2026BAH0002": {
        "oferta":        "PEDIDO",
        "produto":       "Açúcar IC45",
        "comprador":     "via Bahov (intermediário)",
        "fornecedor":    "Rokane / CONEX Brasil World",
        "viz":           "Açúcar IC45 100K MT CIF SBLC via Bahov",
        "especificacao": "Volume: 100.000 MT | Incoterm: CIF | Pgto: SBLC | Intermediário: Bahov",
        "situacao":      "No aguardo Bahov",
        "acao":          "Aguardar retorno Bahov",
    },

    # ── CAMMUS / TRATTO ────────────────────────────────────────────────────────
    "2026CAM0001": {
        "oferta":        "PEDIDO",
        "produto":       "Farelo de Soja",
        "comprador":     "Alejandro (alex@intlbdadvisors.com) / Rep. Dominicana",
        "fornecedor":    "Rokane (via Tratto/Marcio)",
        "viz":           "Farelo Soja 8.950 MT (5x1.790) CFR Haina 395 USD",
        "especificacao": "Volume: 1.790 MT x 5 embarques = 8.950 MT total | Incoterm: CFR Haina (Rep. Dominicana) | Preço: 395 USD/MT | Comprador mandatário: Alejandro | Reunião realizada 15/04",
        "situacao":      "No aguardo próximos passos - NDA sendo assinado",
        "acao":          "Retomar contato 22/04 - finalizar NDA e enviar SCO",
    },
    "2026CAM0002": {
        "oferta":        "PEDIDO",
        "produto":       "Milho",
        "comprador":     "Alejandro (alex@intlbdadvisors.com) / Rep. Dominicana",
        "fornecedor":    "Rokane (via Tratto/Marcio)",
        "viz":           "Milho 17.800 MT (5x3.560) CFR Haina 238 USD",
        "especificacao": "Volume: 3.560 MT x 5 embarques = 17.800 MT total | Incoterm: CFR Haina (Rep. Dominicana) | Preço: 238 USD/MT | Mesmo comprador CAM0001",
        "situacao":      "No aguardo próximos passos - NDA sendo assinado",
        "acao":          "Retomar contato 22/04 - finalizar NDA",
    },
    "2026CAM0003": {
        "oferta":        "PEDIDO",
        "produto":       "Café",
        "comprador":     "Alejandro (alex@intlbdadvisors.com) / Rep. Dominicana",
        "fornecedor":    "",
        "viz":           "Café - Rep. Dominicana - Alejandro - volume a definir",
        "especificacao": "Pedido recebido 15/04 | Aguardando reunião com Alejandro para definir especificações e volume",
        "situacao":      "Aguardando reunião para definição de specs",
        "acao":          "Agendar reunião com Alejandro para specs de Café",
    },
    "2026CAM0004": {
        "oferta":        "PEDIDO",
        "produto":       "Farelo de Trigo",
        "comprador":     "Alejandro (alex@intlbdadvisors.com) / Rep. Dominicana",
        "fornecedor":    "",
        "viz":           "Farelo de Trigo - Rep. Dominicana - Alejandro - volume a definir",
        "especificacao": "Pedido recebido 15/04 junto com Café | Mesmo comprador CAM0001/CAM0002 | Specs a definir",
        "situacao":      "Aguardando reunião para definição de specs",
        "acao":          "Agendar reunião com Alejandro para specs de Farelo de Trigo",
    },

    # ── CONEX BRASIL WORLD ─────────────────────────────────────────────────────
    "2026CONEX0001": {
        "oferta":        "OFERTA",
        "produto":       "Algodão em Pluma",
        "comprador":     "Turquia (Mersin)",
        "fornecedor":    "CONEX Brasil World (alexandre.oliveira@wrm.global)",
        "viz":           "Algodão Pluma 150K MT FOB/FAS Paranaguá destino Mersin Turquia",
        "especificacao": "Volume: 150.000 MT | Incoterm: FOB / FAS Paranaguá | Destino: Mersin, Turquia | Fornecedor: CONEX Brasil World / Alexandre Oliveira",
        "situacao":      "No aguardo - levantando logística e comprador firme",
        "acao":          "Confirmar comprador em Mersin / cotação logística",
    },
    "2026CONEX0002": {
        "oferta":        "PEDIDO",
        "produto":       "Açúcar IC45",
        "comprador":     "China (via Maxtor)",
        "fornecedor":    "CONEX Brasil World / Maxtor",
        "viz":           "Açúcar IC45 100K MT x12 CIF China DLC 430+5 USD via Maxtor",
        "especificacao": "Volume: 100.000 MT x 12 meses | Incoterm: CIF China | Pgto: DLC via Maxtor | Preço: 430 USD + 5 over",
        "situacao":      "No aguardo - Maxtor estruturando operação",
        "acao":          "Aguardar Maxtor - enviar SCO/SPA quando solicitado",
    },

    # ── CARLOS BICCA ──────────────────────────────────────────────────────────
    "2026BUC0001": {
        "oferta":        "PEDIDO",
        "produto":       "",
        "comprador":     "via Carlos Bicca",
        "fornecedor":    "",
        "viz":           "Bicca - tem comprador - produto e preço TBD",
        "especificacao": "Carlos Bicca tem um comprador. Produto ainda a definir. Samba fornece o produto. Preço: target a receber do comprador.",
        "situacao":      "Aguardando definição de produto e target de preço do comprador",
        "acao":          "Solicitar a Bicca: produto desejado, target de preço, volume e destino",
    },

    # ── GWI (Gui Wicthoffet) ──────────────────────────────────────────────────
    "2026GWI0001": {
        "oferta":        "PEDIDO",
        "produto":       "Enxofre (Sulphur)",
        "comprador":     "via GWI / Guilherme Carlão",
        "fornecedor":    "",
        "viz":           "Enxofre 100K MT x12 CIF China DLC 320 USD",
        "especificacao": "Volume: 100.000 MT/mês x 12 meses | Incoterm: CIF China | Pgto: DLC / LC 100% | Preço: 320 USD/MT | HS 2503 Sulphur",
        "situacao":      "No aguardo - buscando fornecedor de enxofre",
        "acao":          "Buscar fornecedor de Enxofre para cotação CIF China",
    },
    "2026GWI0002": {
        "oferta":        "PEDIDO",
        "produto":       "Milho",
        "comprador":     "via GWI / Guilherme Carlão",
        "fornecedor":    "",
        "viz":           "Milho 50 containers ração CIF Suriname",
        "especificacao": "Volume: 50 containers (ração animal) | Incoterm: CIF Suriname | Pgto: TBD | Aguardando SCO",
        "situacao":      "No aguardo - solicitação enviada para fornecedores",
        "acao":          "Enviar SCO de milho para GWI",
    },
    "2026GWI003": {
        # Produto/fornecedor/viz/situacao já preenchidos na planilha.
        # Apenas ACAO e COMPRADOR estão em branco.
        # Deal: Cacau 600 MT FOB Barcarena/PA via Vilson (Akxon) — LOI recebida.
        "oferta":        "OFERTA",
        "produto":       "Cacau",
        "comprador":     "",          # pendente vendedor — comprador ainda não definido
        "fornecedor":    "Vilson Curvello / Akxon Trading LLC",
        "viz":           "Cacau 600 MT FOB Barcarena/PA 3290 USD - 22 containers - origem Brasil/Norte",
        "especificacao": "Volume: 600 MT (22 containers) | Incoterm: FOB Barcarena/PA | Preço: 3.290 USD/MT (25% abaixo NYSE) | Pgto: 50/50 spot | Comissão: 2% | Origem: Brasil/Norte | Embarque: janela 05/abril | Umidade: 7% | REF VIL260001",
        "situacao":      "LOI recebida - aguardando VILSON",
        "acao":          "Confirmar com Vilson disponibilidade pós-janela 05/abril - buscar comprador para Cacau",
    },

    # ── HUGGO FEHR / PRIMEX ────────────────────────────────────────────────────
    "2026PRIMX0001": {
        "oferta":        "PEDIDO",
        "produto":       "Chicken Paw (Pé de Frango)",
        "comprador":     "empresa Shanghai (holding estatal, Banco Agrícola da China)",
        "fornecedor":    "via Primex / HC Suprimentos (Huggo/Hamilton)",
        "viz":           "Chicken Paw 100 cont/mês x12 CIF Qingdao/Ningbo DLC 3.680 USD",
        "especificacao": "Volume: 100 containers 40'REF x 12 meses | Incoterm: CIF Qingdao / Porto Ningbo | Pgto: DLC irrevogável transferível divisível | Preço: 3.680 USD/MT (aprovado) | GACC Grau A | 30-40g/pc | Certificação HALAL | Origem: Santos ou Paranaguá | Destino: Ningbo China",
        "situacao":      "Tratativas em curso - buscando frigorífico parceiro",
        "acao":          "Confirmar frigorífico fornecedor (não-JBS) via Huggo / agendar reunião",
    },
    "2026PRIMX0002": {
        "oferta":        "OFERTA",
        "produto":       "Soja Padrão Exportação",
        "comprador":     "via BTG Agro (Thiago Nino) / comprador Huggo",
        "fornecedor":    "Produtores MT - região Sorriso (via Primex/HC)",
        "viz":           "Soja 90K MT EXW Sorriso R$110 / FOB Paranaguá R$140",
        "especificacao": "Volume: 90.000 MT (1,7 milhão sacas 60kg) | EXW: R$ 110,00 Sorriso/MT | FOB: R$ 140,00 Paranaguá | Comissão: 0,50 BRL/saca cada lado | Requisitos: CIS + SINTEGRA + Firme de Compra | REF ROMA260002",
        "situacao":      "No aguardo - em contato com BTG Agro para viabilizar compra",
        "acao":          "Aguardar posicionamento BTG Agro / Huggo - confirmar disponibilidade com produtor",
    },
    "2026PRIMX0003": {
        "oferta":        "OFERTA",
        "produto":       "Milho (Consumo Humano)",
        "comprador":     "via Primex / GWI",
        "fornecedor":    "4 Fazendas - Mato Grosso do Sul",
        "viz":           "Milho consumo humano 28.800 MT EXW 58 BRL / CIF 405 USD",
        "especificacao": "Volume: 28.800 MT (380.000 sacas 60kg) | EXW: 58 BRL | FCA: 75 BRL | CIF: 405 USD ASWP | Início: imediato | Comissão CIF: 2,5/2,5 USD/MT | REF REFZE0001 | Origem: 4 fazendas MS",
        "situacao":      "No aguardo comprador",
        "acao":          "Buscar comprador para milho consumo humano CIF",
    },
    "2026PRIMX0004": {
        "oferta":        "PEDIDO",
        "produto":       "Farelo de Soja",
        "comprador":     "HC Suprimentos / Hamilton",
        "fornecedor":    "Dureino",
        "viz":           "Farelo Soja - HC Suprimentos - Dureino - Rep. Pará",
        "especificacao": "Comprador: HC Suprimentos (Hamilton - hcsuprimentosltda@gmail.com) | Fornecedor: Dureino | Necessidade: compra de Farelo de Soja no Pará",
        "situacao":      "No aguardo - Dureino sendo contatada",
        "acao":          "Conectar Hamilton (HC Suprimentos) com Fabricio (Dureino)",
    },
    "2026PRIMX0005": {
        "oferta":        "OFERTA",
        "produto":       "Mel HALAL",
        "comprador":     "",
        "fornecedor":    "via Primex",
        "viz":           "Mel HALAL 6 ton/mês 18 USD/kg",
        "especificacao": "Volume: 6 toneladas/mês | Preço: 18 USD/kg | Certificação: HALAL | Fornecedor: via rede Primex",
        "situacao":      "No aguardo comprador",
        "acao":          "Buscar comprador de mel HALAL",
    },
    "2026PRIMX0006": {
        "oferta":        "OFERTA",
        "produto":       "Chicken Paw (Pé de Frango)",
        "comprador":     "",
        "fornecedor":    "parceiro de Huggo - disponibilidade mensal",
        "viz":           "Chicken Paw - disponibilidade mensal via parceiro Huggo",
        "especificacao": "Fornecedor: parceiro de Huggo com disponibilidade mensal de Chicken Paw | Detalhes a confirmar em reunião agendada",
        "situacao":      "Aguardando reunião para definição de volumes e preços",
        "acao":          "Realizar reunião com parceiro de Huggo - levantar volumes e preços disponíveis",
    },

    # ── VILSON CURVELLO / AKXON TRADING LLC ───────────────────────────────────
    "2026VIL0001": {
        "oferta":        "OFERTA",
        "produto":       "Açúcar IC150",
        "comprador":     "Nader / N&H (Bahrein) - destino Yemen",
        "fornecedor":    "Akxon Trading LLC / Vilson Curvello (vilson@akxontrading.com)",
        "viz":           "Açúcar IC150 12x25MT CIF Porto Aden Yemen DLC 470 USD",
        "especificacao": "Volume: 12 meses x 25.000 MT | Incoterm: CIF Porto de Aden, Yemen | Pgto: DLC transferível | Preço: 470 USD/MT (over mínimo 10 USD 5/5) | Comissão: 7 USD/MT | Banco intermediário: JP MORGAN | Comprador: Nader/N&H Bahrein | Fornecedor: Akxon - cotista com cotas em várias usinas",
        "situacao":      "ICPO recebida - FCO/SPA em preparação - aguardando confirmação bancária",
        "acao":          "Enviar FCO revisada para Nader - confirmar banco intermediário JP Morgan",
    },
    "2026VIL0002": {
        "oferta":        "OFERTA",
        "produto":       "Chicken Paw (Pé de Frango)",
        "comprador":     "China (Qingdao)",
        "fornecedor":    "Akxon Trading / Vilson (via BRF/JBS trade)",
        "viz":           "Chicken Paw 20 cont x12 CIF Qingdao DLC/SBLC 3.500-3.650 USD",
        "especificacao": "Volume: 20 containers 40'REF x 12 meses | Incoterm: CIF China (Qingdao) | Pgto: DLC/SBLC irrevogável transferível divisível banco top 50 | Preço: 3.500-3.650 USD/MT | GACC Grau A | Fornecedor: Akxon (cotista BRF ou JBS)",
        "situacao":      "Mandatário sinalizou positivamente - aguardando LOI/ICPO",
        "acao":          "Enviar LOI → SCO / ICPO → SPA com Vilson",
    },
    "2026VIL0003": {
        "oferta":        "OFERTA",
        "produto":       "Alumínio Lingote (P1020A)",
        "comprador":     "China (CIF Xangai/Yangshan)",
        "fornecedor":    "Akxon Trading / Vilson (origem Cazaquistão)",
        "viz":           "Alumínio P1020A 25K MT trial / 50K MT x12 CIF Xangai DLC 2.450 USD",
        "especificacao": "Pureza: 99,76% | Trial: 25.000 MT direto Cazaquistão→China | Extensivo: 50.000 MT/mês x 12 meses | Incoterm: CIF Porto Xangai/Yangshan | Pgto: DLC irrevogável transferível | Preço: target 2.450 USD/MT | Inspeção: SGS no porto descarga | Paletizado em cintas alumínio | LME standard",
        "situacao":      "Negociação de comissão - comprador precisa POP banco antes DLC",
        "acao":          "Definir estrutura de comissão com Vilson - resolver questão POP pré-DLC com comprador",
    },
    "2026VIL0004": {
        "oferta":        "OFERTA",
        "produto":       "Lagosta Viva",
        "comprador":     "",
        "fornecedor":    "Akxon Trading / Vilson (3 países de origem)",
        "viz":           "Lagosta Viva 100 ton/mês FOB USD 43/kg",
        "especificacao": "Volume: 100 toneladas/mês | Incoterm: FOB (3 países origem) | Preço: USD 43/kg (preço fornecedor) | Comprador target: 18 USD/kg FOB Brasil | Status: deal inativo (preço muito elevado para comprador)",
        "situacao":      "Deal inativo - gap de preço comprador/vendedor muito grande",
        "acao":          "Aguardar - reavaliar se surgir comprador para preço USD 43/kg",
    },
    "2026VIL0005": {
        "oferta":        "OFERTA",
        "produto":       "Algodão em Pluma",
        "comprador":     "Turquia ou Bangladesh",
        "fornecedor":    "Akxon Trading / Vilson Curvello",
        "viz":           "Algodão Pluma 31-2 10K MT/mês x12 LC Turquia/Bangladesh",
        "especificacao": "Produto: Algodão em Pluma Branco Grau 31-2 | Volume: 10.000 MT/mês x 12 meses | Incoterm: FOB ou CIF Turquia ou Bangladesh | Pgto: Carta de Crédito à Vista | Micronaire: 4.61 | Resistência: 32.6 | Comprimento: 31.09 mm | Uniformidade: 82.4 | Umidade: 6.3% | Impurezas: 0.10%",
        "situacao":      "No aguardo - Vilson buscando SCO/preço",
        "acao":          "Solicitar SCO de algodão para Vilson - confirmar preço e prazo",
    },
}

# ─── KB dinâmica — lê deals do banco de dados ────────────────────────────────

def _load_kb_from_db() -> dict[str, dict[str, Any]]:
    """
    Carrega deals ativos do banco de dados e converte para o formato KNOWLEDGE_BASE.

    Retorna um dict {job_code: {field: value}} para deals que:
      - têm nome (usado como job_code)
      - estão com status != 'inativo'
      - não estão já cobertos pelo KNOWLEDGE_BASE estático (que tem precedência)

    Campos mapeados:
      direcao  → oferta  ("OFERTA" | "PEDIDO")
      stage    → status
      commodity→ produto
      destination → comprador (se direcao=venda) ou fornecedor (se compra)
      notes    → situacao
    """
    try:
        import sys as _sys
        import os as _os
        _sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), ".."))
        from models.database import get_session, Deal
        from sqlalchemy import text

        sess = get_session()
        deals = (
            sess.query(Deal)
            .filter(
                Deal.name.isnot(None),
                Deal.status != "inativo",
            )
            .order_by(Deal.updated_at.desc())
            .limit(200)
            .all()
        )
        sess.close()
    except Exception as exc:
        logger.warning("_load_kb_from_db: erro ao carregar DB — %s", exc)
        return {}

    db_kb: dict[str, dict[str, Any]] = {}
    for deal in deals:
        job_code = (deal.name or "").strip()
        if not job_code or job_code in KNOWLEDGE_BASE:
            continue   # hardcoded tem precedência

        direction = (deal.direcao or "").upper()
        oferta = "OFERTA" if direction in ("VENDA", "ASK") else "PEDIDO"

        # Volume + preço em 1 linha para a viz
        vol_str = f"{deal.volume:,.0f} {deal.volume_unit}" if deal.volume and deal.volume_unit else ""
        price_str = f"{deal.price:,.2f} {deal.currency}/MT" if deal.price and deal.currency else ""
        inco_str  = deal.incoterm or ""
        viz_parts = filter(None, [deal.commodity, vol_str, inco_str, price_str, deal.destination or deal.origin])
        viz = " | ".join(viz_parts)[:120]

        # Comprador/fornecedor heurística por direção
        if direction in ("VENDA", "ASK"):
            comprador  = deal.destination or ""
            fornecedor = deal.source_sender or deal.assignee or ""
        else:
            comprador  = deal.source_sender or deal.source_group or ""
            fornecedor = deal.origin or ""

        db_kb[job_code] = {
            "oferta":        oferta,
            "status":        deal.stage or "",
            "produto":       deal.commodity or "",
            "comprador":     comprador,
            "fornecedor":    fornecedor,
            "viz":           viz,
            "especificacao": (deal.notes or "")[:300],
            "situacao":      deal.stage or "",
            "acao":          "",   # não temos ação automática — deixa vazio para não sobrescrever
        }

    logger.info("_load_kb_from_db: %d deals carregados do banco", len(db_kb))
    return db_kb


# ─── Mapeamento de campo → índice de coluna ──────────────────────────────────
FIELD_TO_COL: dict[str, int] = {
    "oferta":        COL_OFERTA,
    "status":        COL_STATUS,
    "produto":       COL_PRODUTO,
    "comprador":     COL_COMPRADOR,
    "fornecedor":    COL_FORNECEDOR,
    "viz":           COL_VIZ,
    "especificacao": COL_ESPECIFICACAO,
    "situacao":      COL_SITUACAO,
    "acao":          COL_ACAO,
}

# ─── Utilities ────────────────────────────────────────────────────────────────

def _col_letter(col_idx: int) -> str:
    """Converte índice 0-based para letra da coluna (A, B, ..., Z)."""
    return chr(ord("A") + col_idx)


def _cell_ref(row_1based: int, col_idx: int) -> str:
    """Ex: row=5, col=6 → "G5"."""
    return f"'{SHEET_TAB}'!{_col_letter(col_idx)}{row_1based}"


def _get_cell(row: list[Any], col_idx: int) -> str:
    """Retorna o valor da célula (string) ou '' se não existir."""
    if col_idx < len(row):
        return str(row[col_idx]).strip()
    return ""


def _normalize_grupo(grupo: str) -> str:
    return grupo.strip().lower()


def _infer_group_code(grupo: str) -> str | None:
    """Inferir o código do grupo a partir do nome na coluna D."""
    normalized = _normalize_grupo(grupo)
    for key, code in GROUP_CODE_MAP.items():
        if key in normalized:
            return code
    return None


def _build_expected_job(group_code: str, seq: int) -> str:
    """Monta o JOB esperado: 2026ERIC0001, 2026BAH0002, etc."""
    return f"2026{group_code}{seq:04d}"


# ─── Classe principal ─────────────────────────────────────────────────────────

class WppEnrichmentAgent:
    """
    Lê a planilha, identifica campos em branco por JOB e preenche com
    dados das conversas de WhatsApp. NUNCA sobrescreve células com conteúdo.
    """

    def __init__(self) -> None:
        creds = drive_manager.creds
        self._service = build("sheets", "v4", credentials=creds)
        self._sheets = self._service.spreadsheets()
        self._effective_kb: dict[str, dict[str, Any]] | None = None  # lazy cache

    def _get_effective_kb(self) -> dict[str, dict[str, Any]]:
        """
        Retorna o Knowledge Base efetivo: hardcoded KNOWLEDGE_BASE + deals do banco.

        KNOWLEDGE_BASE estático tem precedência — nunca é sobrescrito por dados do DB.
        O resultado é cacheado por instância (1 sessão = 1 carga de DB).
        """
        if self._effective_kb is None:
            db_kb = _load_kb_from_db()
            # Merge: hardcoded first (higher priority), then db extras
            self._effective_kb = {**db_kb, **KNOWLEDGE_BASE}
            logger.info(
                "_get_effective_kb: %d hardcoded + %d DB-only = %d total JOBs",
                len(KNOWLEDGE_BASE),
                len([k for k in db_kb if k not in KNOWLEDGE_BASE]),
                len(self._effective_kb),
            )
        return self._effective_kb

    # ── Leitura da planilha ───────────────────────────────────────────────────

    def _read_sheet(self) -> list[list[Any]]:
        """Lê todas as linhas da aba 'todos andamento'."""
        result = (
            self._sheets.values()
            .get(
                spreadsheetId=SPREADSHEET_ID,
                range=f"'{SHEET_TAB}'!A:P",
                valueRenderOption="UNFORMATTED_VALUE",
            )
            .execute()
        )
        return result.get("values", [])

    # ── Mapeamento JOB → linha ────────────────────────────────────────────────

    def _build_job_row_map(
        self, rows: list[list[Any]]
    ) -> tuple[dict[str, int], dict[str, int]]:
        """
        Retorna dois dicts:
          job_map:   JOB_CODE → sheet_row_1based
          blank_job_map: computed_JOB → sheet_row_1based  (para linhas sem JOB)

        Linhas de cabeçalho e REJECTED/SKIPPED são ignoradas.
        """
        job_map: dict[str, int] = {}
        blank_job_map: dict[str, int] = {}

        # Contador de sequência por grupo (para linhas sem JOB)
        group_seq: dict[str, int] = {}

        for data_idx, row in enumerate(rows):
            sheet_row = data_idx + 1  # 1-based

            grupo = _get_cell(row, COL_GRUPO)
            if not grupo or grupo.lower() in {"grupo", ""}:
                continue  # linha de cabeçalho ou estrutural

            status_auto = _get_cell(row, COL_STATUS_AUTO)
            if status_auto.upper() in SKIP_AUTO_STATUSES:
                continue  # deal rejeitado/skipped pela automação

            job = _get_cell(row, COL_JOB)

            if job:
                # Linha já tem JOB
                job_map[job] = sheet_row
            else:
                # Linha sem JOB — inferir pelo grupo + sequência
                group_code = _infer_group_code(grupo)
                if group_code:
                    group_seq[group_code] = group_seq.get(group_code, 0) + 1
                    computed_job = _build_expected_job(group_code, group_seq[group_code])
                    blank_job_map[computed_job] = sheet_row
                    logger.info(
                        "  Linha %d (GRUPO=%s) sem JOB → atribuindo %s",
                        sheet_row,
                        grupo,
                        computed_job,
                    )

        return job_map, blank_job_map

    # ── Construção das atualizações ───────────────────────────────────────────

    def _build_updates(
        self,
        rows: list[list[Any]],
        job_map: dict[str, int],
        blank_job_map: dict[str, int],
    ) -> tuple[list[dict], list[dict]]:
        """
        Retorna:
          value_updates: lista de ValueRange para values().batchUpdate()
          job_writes:    lista de ValueRange para escrever JOB codes em linhas vazias
        """
        value_updates: list[dict] = []
        job_writes: list[dict] = []

        # ── Processa JOBs com match direto ──
        for job_code, kb_data in self._get_effective_kb().items():
            # Verifica duplicata
            if job_code in DUPLICATE_JOB_MAP:
                canonical = DUPLICATE_JOB_MAP[job_code]
                logger.warning("JOB %s é duplicata de %s — ignorando KB separada", job_code, canonical)
                continue

            sheet_row = job_map.get(job_code) or blank_job_map.get(job_code)
            if sheet_row is None:
                logger.debug("JOB %s não encontrado na planilha — pulando", job_code)
                continue

            # Linha atual da planilha
            row = rows[sheet_row - 1]  # back to 0-based

            # Se veio de blank_job_map, precisa escrever o JOB na coluna A
            if job_code in blank_job_map and job_code not in job_map:
                job_current = _get_cell(row, COL_JOB)
                if not job_current:
                    job_writes.append({
                        "range": _cell_ref(sheet_row, COL_JOB),
                        "values": [[job_code]],
                    })
                    logger.info("  Escrevendo JOB %s → linha %d col A", job_code, sheet_row)

            # Para cada campo do KB, preenche somente se estiver em branco
            for field_name, value in kb_data.items():
                if not value:  # não preenche campo vazio no KB
                    continue
                col_idx = FIELD_TO_COL.get(field_name)
                if col_idx is None:
                    continue

                current = _get_cell(row, col_idx)
                if current:
                    logger.debug(
                        "  JOB %s col %s já tem valor '%s' — mantendo",
                        job_code,
                        _col_letter(col_idx),
                        current[:40],
                    )
                    continue

                value_updates.append({
                    "range": _cell_ref(sheet_row, col_idx),
                    "values": [[str(value)]],
                })
                logger.info(
                    "  Preenchendo JOB %s col %s (%s) = '%s'",
                    job_code,
                    _col_letter(col_idx),
                    field_name,
                    str(value)[:60],
                )

        # ── Sinaliza duplicatas na planilha ──────────────────────────────────
        for dup_job, canonical in DUPLICATE_JOB_MAP.items():
            sheet_row = job_map.get(dup_job)
            if sheet_row is None:
                continue
            row = rows[sheet_row - 1]
            acao_current = _get_cell(row, COL_ACAO)
            dup_msg = f"⚠ DUPLICATA de {canonical} — verificar e mesclar"
            if not acao_current:
                value_updates.append({
                    "range": _cell_ref(sheet_row, COL_ACAO),
                    "values": [[dup_msg]],
                })
                logger.info("  Marcando %s como DUPLICATA de %s", dup_job, canonical)

        return value_updates, job_writes

    # ── Execução da escrita ───────────────────────────────────────────────────

    def _apply_updates(
        self,
        value_updates: list[dict],
        job_writes: list[dict],
    ) -> None:
        all_updates = job_writes + value_updates
        if not all_updates:
            logger.info("Nenhuma atualização necessária — planilha já está completa!")
            return

        logger.info("Aplicando %d atualizações na planilha...", len(all_updates))
        body = {
            "valueInputOption": "USER_ENTERED",
            "data": all_updates,
        }
        resp = (
            self._sheets.values()
            .batchUpdate(spreadsheetId=SPREADSHEET_ID, body=body)
            .execute()
        )
        updated = resp.get("totalUpdatedCells", len(all_updates))
        logger.info("✅ %d células atualizadas com sucesso.", updated)

    # ── Método público ────────────────────────────────────────────────────────

    def run(self) -> dict:
        """
        Executa o ciclo completo:
        1. Lê a planilha
        2. Mapeia JOBs → linhas
        3. Gera atualizações (somente células em branco)
        4. Aplica na planilha
        5. Retorna relatório
        """
        logger.info("=" * 60)
        logger.info("WppEnrichmentAgent — iniciando enriquecimento")
        logger.info("=" * 60)

        rows = self._read_sheet()
        logger.info("Linhas lidas da planilha: %d", len(rows))

        job_map, blank_job_map = self._build_job_row_map(rows)
        logger.info(
            "JOBs com código existente: %d | JOBs inferidos por grupo+seq: %d",
            len(job_map),
            len(blank_job_map),
        )

        kb_coverage = set(KNOWLEDGE_BASE.keys())
        found_in_sheet = (set(job_map) | set(blank_job_map)) & kb_coverage
        not_found = kb_coverage - found_in_sheet
        if not_found:
            logger.warning("JOBs no KB mas não encontrados na planilha: %s", sorted(not_found))

        value_updates, job_writes = self._build_updates(rows, job_map, blank_job_map)

        self._apply_updates(value_updates, job_writes)

        result = {
            "rows_read": len(rows),
            "jobs_with_existing_code": len(job_map),
            "jobs_inferred": len(blank_job_map),
            "kb_jobs": len(KNOWLEDGE_BASE),
            "kb_jobs_found_in_sheet": len(found_in_sheet),
            "kb_jobs_not_in_sheet": sorted(not_found),
            "job_writes": len(job_writes),
            "value_updates": len(value_updates),
            "total_cells_updated": len(job_writes) + len(value_updates),
        }
        logger.info("Resultado: %s", result)
        return result


# ─── Execução direta ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    agent = WppEnrichmentAgent()
    result = agent.run()
    print("\n── RESULTADO FINAL ──────────────────────────────────────────")
    for k, v in result.items():
        print(f"  {k}: {v}")
