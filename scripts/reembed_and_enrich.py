"""
scripts/reembed_and_enrich.py
==============================
1. Re-embeda todos os chunks existentes no DB com gemini-embedding-001
   (3072 dims, float32 binary — formato correto para cosine similarity)
2. Gera e embeda os 45 artigos novos de domínio de commodities

Uso:
    python scripts/reembed_and_enrich.py            # tudo
    python scripts/reembed_and_enrich.py --only-reembed  # só re-embeda existentes
    python scripts/reembed_and_enrich.py --only-enrich   # só gera artigos novos
"""
from __future__ import annotations

import argparse
import json
import os
import struct
import sys
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(override=True)

from google import genai
from google.genai import types as gtypes

GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")
MODEL_GEN  = "models/gemini-2.5-flash-lite"
MODEL_EMB  = "models/gemini-embedding-001"   # 3072 dims, float32
RPM_LIMIT  = 80   # billing ativo — real: ~115 RPM, margem de segurança 80

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
    """gemini-embedding-001 → float32 binary (3072 dims = 12288 bytes)."""
    _rpm_wait()
    result = client.models.embed_content(
        model=MODEL_EMB,
        contents=text[:8000],
    )
    floats = result.embeddings[0].values
    return struct.pack(f"{len(floats)}f", *floats)

# ── Re-embeda existentes ──────────────────────────────────────────────────────
def reembed_existing():
    from models.database import get_session, CorporateKnowledge
    from sqlalchemy import func
    from datetime import datetime

    sess = get_session()
    chunks = sess.query(CorporateKnowledge).all()
    total  = len(chunks)
    print(f"\n=== Re-embedando {total} chunks existentes ===")
    print(f"Modelo: {MODEL_EMB} (3072 dims, float32 binary)\n")

    ok = 0
    for i, chunk in enumerate(chunks, 1):
        if not chunk.content:
            print(f"  [{i:3d}/{total}] SKIP (sem conteúdo) — {str(chunk.document_name)[:50]}")
            continue

        # Verifica se já está no formato correto (12288 bytes = 3072 * 4)
        if isinstance(chunk.embedding, bytes) and len(chunk.embedding) == 12288:
            print(f"  [{i:3d}/{total}] OK   (já correto) — {str(chunk.document_name)[:50]}")
            ok += 1
            continue

        try:
            emb_bytes = embed_text(chunk.content)
            chunk.embedding = emb_bytes
            sess.commit()
            ok += 1
            print(f"  [{i:3d}/{total}] OK   {len(emb_bytes)} bytes — {str(chunk.document_name)[:50]}")
        except Exception as e:
            sess.rollback()
            print(f"  [{i:3d}/{total}] ERRO: {e}")
            time.sleep(5)

    sess.close()
    print(f"\nRe-embedding concluído: {ok}/{total} chunks atualizados")

# ── Artigos novos ─────────────────────────────────────────────────────────────
KNOWLEDGE_TAXONOMY = [
    {
        "category": "Incoterms 2020",
        "articles": [
            {"title": "FOB (Free On Board) — Definição Completa",
             "prompt": "Escreva um artigo técnico detalhado sobre o Incoterm FOB (Free On Board) 2020. Inclua: definição precisa, ponto de transferência de risco, quem paga frete/seguro/despacho, obrigações do vendedor vs comprador, quando usar FOB em commodities agrícolas (soja, milho, açúcar, café, algodão), diferença entre FOB porto de origem e FOB vessel, exemplos práticos com preços e portos brasileiros (Santos, Paranaguá, Maceió)."},
            {"title": "CIF (Cost, Insurance and Freight) — Definição Completa",
             "prompt": "Escreva um artigo técnico sobre o Incoterm CIF (Cost, Insurance and Freight) 2020. Inclua: definição precisa, ponto de transferência de risco vs propriedade, quem contrata e paga o frete marítimo e seguro, obrigações detalhadas do exportador e importador, quando CIF é preferido por compradores do Oriente Médio e África, cálculo do preço CIF a partir do FOB, porcentagem típica de frete e seguro para rotas Brasil-China/Brasil-Oriente Médio."},
            {"title": "CFR, EXW, DAP, DDP, FCA — Guia Comparativo",
             "prompt": "Escreva um guia técnico comparando os Incoterms CFR, EXW, DAP, DDP e FCA (versão 2020). Para cada um: definição, ponto de risco, quem paga o quê, casos de uso em commodities agrícolas. Inclua uma tabela comparativa de responsabilidades (frete, seguro, desembaraço, carga). Destaque quando cada termo é usado em contratos de soja, açúcar e milho."},
            {"title": "Incoterms na Prática: Erros Comuns e Boas Práticas",
             "prompt": "Escreva um artigo prático sobre erros comuns no uso de Incoterms em contratos de commodities agrícolas. Inclua: confusão FOB vs CFR no Brasil, risco de usar EXW em exportação, problemas com CIF quando o seguro é insuficiente, como especificar o ponto geográfico correto, cláusulas adicionais necessárias, diferença Incoterms 2010 vs 2020. Exemplos reais do mercado."},
        ]
    },
    {
        "category": "Documentação Export/Import",
        "articles": [
            {"title": "Bill of Lading (BL/MBL/HBL) — Guia Completo",
             "prompt": "Escreva um artigo técnico completo sobre o Bill of Lading (Conhecimento de Embarque). Inclua: o que é e sua função jurídica, tipos (Master BL, House BL, Switch BL, Straight BL, Order BL, Bearer BL), campos obrigatórios, endosso e negociabilidade, BL vs Sea Waybill, problemas comuns, procedimento de emissão com armadores."},
            {"title": "Certificado de Origem — Tipos e Procedimentos",
             "prompt": "Escreva um artigo técnico sobre Certificados de Origem para exportação brasileira de commodities. Inclua: CO Não-Preferencial vs Preferencial, quem emite no Brasil, quando é obrigatório, drawback e CO, como preencher corretamente, erros frequentes e consequências. Exemplos para soja, açúcar, milho."},
            {"title": "Certificado Fitossanitário e Documentação MAPA",
             "prompt": "Escreva um artigo técnico sobre documentação fitossanitária para exportação de commodities agrícolas do Brasil. Inclua: Certificado Fitossanitário (MAPA/VIGIAGRO), Certificado de Fumigação, quando cada documento é exigido por país de destino, prazo de validade, o que é LPCO, fluxo de inspeção nos portos, custo médio, tabela de exigências por commodity/destino."},
            {"title": "Invoice Comercial e Packing List — Preenchimento Correto",
             "prompt": "Escreva um guia técnico sobre Commercial Invoice e Packing List para exportação de commodities. Inclua: campos obrigatórios da Invoice, erros que travam o despacho aduaneiro, como descrever commodities agrícolas corretamente, pesos líquido vs bruto, diferença Invoice vs Proforma Invoice vs Final Invoice, rastreabilidade do packing list."},
            {"title": "SISCOMEX, RADAR e Registro de Exportação (RE/DU-E)",
             "prompt": "Escreva um artigo técnico sobre o sistema de exportação brasileiro: SISCOMEX, RADAR Receita Federal (tipos: limitado, ilimitado), como habilitar empresa para exportar, transição para DU-E, campos do DU-E, integração com MAPA, prazos e penalidades, papel do despachante aduaneiro."},
            {"title": "Inspeção SGS/Bureau Veritas/Intertek — Como Funciona",
             "prompt": "Escreva um artigo técnico sobre inspeção de commodities por empresas certificadoras. Inclua: papel da SGS, Bureau Veritas, Intertek e COTECNA, tipos de inspeção (pre-shipment, loading, arrival), o que é inspecionado, Certificate of Analysis vs Certificate of Weight, Draft Survey, amostragem, resultados fora de spec, custo médio por serviço."},
        ]
    },
    {
        "category": "Specs de Commodities",
        "articles": [
            {"title": "Soja Brasileira — Especificações Técnicas Completas",
             "prompt": "Escreva um artigo técnico completo sobre especificações da soja brasileira para exportação. Inclua: classificação CONAB/MAPA, parâmetros-chave (umidade máx 14%, impurezas, proteína, óleo), diferença soja GMO vs non-GMO, soja para farelo vs óleo, safras, cotação base CBOT, portos de embarque, umidade e desconto."},
            {"title": "Açúcar VHP e ICUMSA 45 — Especificações e Mercados",
             "prompt": "Escreva um artigo técnico sobre especificações de açúcar para exportação. Inclua: o que é ICUMSA, escala ICUMSA (45 = branco, 600-1200 = VHP), parâmetros VHP, diferença VHP vs ICUMSA 45 vs cristal, quais mercados compram qual tipo, preço diferencial entre tipos, embalagem (bulk vs big bag vs 50kg)."},
            {"title": "Milho — Especificações Técnicas e Padrões Internacionais",
             "prompt": "Escreva um artigo técnico sobre especificações do milho brasileiro para exportação. Inclua: classificação CONAB, parâmetros (umidade, impurezas, aflatoxina), diferença milho No.2 USDA vs padrão brasileiro, exigências da China, sazonalidade, portos, cotação CBOT vs diferencial local, uso final (ração vs etanol vs alimentação)."},
            {"title": "Farelo e Óleo de Soja — Specs e Mercados",
             "prompt": "Escreva um artigo técnico sobre farelo de soja (soybean meal) e óleo de soja para exportação. FARELO: proteína 46-48% vs 44-46%, umidade, atividade ureática, mercados, preço base CME ZM. ÓLEO: acidez, umidade, índice de iodo, mercados alimentício vs biodiesel, diferencial crude vs degomado vs refinado. Sazonalidade e logística."},
            {"title": "Café — Tipos, Graus e Especificações para Export",
             "prompt": "Escreva um artigo técnico sobre especificações de café para exportação. Inclua: Arábica vs Robusta, classificação brasileira, defeitos, certificações (RFA, UTZ, Fair Trade, Organic), cotação base ICE KC=F, embalagem, principais origens brasileiras, destino por tipo, impacto de safra bicolor."},
            {"title": "Algodão — Especificações HVI e Mercado Internacional",
             "prompt": "Escreva um artigo técnico sobre algodão para exportação. Inclua: parâmetros HVI (MIKE, UHML, UI, STR, SCI, Rd, +b, trash), grades USDA, plumas brasileiras, certificação BCI, cotação ICE CT=F, conversão cents/lb para USD/MT, embalagem (fardos 225kg), principais destinos, rastreabilidade."},
            {"title": "Cacau — Especificações, Fermentação e Mercado",
             "prompt": "Escreva um artigo técnico sobre cacau para exportação. Inclua: grau de fermentação, umidade máx 7.5%, impurezas, aflatoxinas, parâmetros ICCO, origem brasileira (Bahia, Pará), diferença Forastero vs Trinitario vs Criollo, cotação ICE CC=F, sazonalidade, principal destino Holanda, certificações, crise oferta 2023-2025."},
            {"title": "Óleo de Girassol — Specs e Contexto Geopolítico",
             "prompt": "Escreva um artigo técnico sobre óleo de girassol para importação e reexportação. Inclua: especificações (acidez, umidade, índice de iodo), tipos (bruto vs refinado vs alto oleico), origens (Ucrânia, Rússia, Argentina, Brasil), impacto da guerra Ucrânia-Rússia, alternativas de origem, certificação, embalagem, cotação e basis, rotas logísticas alternativas."},
            {"title": "Frango (Chicken Paw/Cortes) — Specs para Exportação Halal",
             "prompt": "Escreva um artigo técnico sobre exportação de frango e derivados do Brasil. Inclua: cortes principais para export (pata China, coxa+sobrecoxa Oriente Médio, peito UE), especificações sanitárias e de temperatura, certificação Halal, SIF, mercados por produto, barreiras (embargos por gripe aviária, zoneamento), cotação e formação de preço, exportadores principais (JBS, BRF, Seara, Marfrig)."},
        ]
    },
    {
        "category": "Trade Finance",
        "articles": [
            {"title": "Letter of Credit (LC) — Guia Completo para Commodities",
             "prompt": "Escreva um artigo técnico completo sobre Letter of Credit (Carta de Crédito) em operações de commodities. Inclua: definição e partes, tipos (irrevogável, confirmada, standby/SBLC, revolving, transferível), documentos exigidos, UCP 600 regras, discrepâncias comuns e como evitar, prazo de apresentação, custo médio, quando LC é preferível, uso para compradores do Oriente Médio e África."},
            {"title": "SBLC, D/P, D/A e Open Account — Alternativas à LC",
             "prompt": "Escreva um artigo técnico sobre modalidades de pagamento em trade finance além da LC. Inclua: SBLC, D/P, D/A, Open Account, Advance Payment, factoring e forfaiting de recebíveis, seguro de crédito SBCE/Coface/Euler Hermes. Quando usar cada modalidade em commodities."},
            {"title": "Câmbio e Hedge em Operações de Commodities",
             "prompt": "Escreva um artigo técnico sobre gestão cambial e hedge em exportações de commodities do Brasil. Inclua: ACC e ACE, NCE e CCE, NDF, opções de câmbio, swaps, Resolução BCB 277, hedge natural vs financeiro, impacto USD/BRL nas margens, obrigatoriedade de ingresso de divisas, exemplos práticos de hedge para vendas futuras de soja."},
            {"title": "Estruturação de Preço e Margem em Operações de Commodity",
             "prompt": "Escreva um artigo técnico sobre como estruturar e calcular preço e margem em operações de trading de commodities. Inclua: componentes do preço (CBOT/ICE futures + basis + premium + logística + impostos + margem), cálculo CIF a partir do FOB, impacto de câmbio, custo de carregamento, back-to-back vs posição proprietária, hedge de preço, P&L de uma operação típica de 5000 MT de soja FOB Santos."},
        ]
    },
    {
        "category": "Contratos de Commodities",
        "articles": [
            {"title": "Estrutura do Contrato de Compra e Venda de Commodity",
             "prompt": "Escreva um artigo técnico sobre a estrutura de um contrato internacional de compra e venda de commodity agrícola. Inclua: identificação das partes, definição do produto com especificações e tolerâncias, quantidade (tolerância MOLCHC), qualidade, preço (fixo vs provisional, base price, prêmio), Incoterm e porto, data de embarque e laycan, termos de pagamento, penalidades, governing law."},
            {"title": "Cláusulas de Qualidade, Peso e Arbitragem",
             "prompt": "Escreva um artigo técnico sobre cláusulas críticas em contratos de commodity: QUALIDADE — specs com tolerâncias, certificadora final, amostragem, penalidades por fora-de-spec. PESO — peso de embarque vs chegada, Draft Survey, margem de tolerância. ARBITRAGEM — ICC Paris, GAFTA, FOSFA, ICAC, mediação prévia, lei aplicável, execução de laudo arbitral."},
            {"title": "Force Majeure, Hardship e Cláusulas de Risco",
             "prompt": "Escreva um artigo técnico sobre cláusulas de risco em contratos de commodities: Force Majeure (definição, notificação, documentação, efeito, COVID-19, guerra Ucrânia), Hardship (desequilíbrio econômico, renegociação), cláusula OFAC/UE sanctions, Quality Force Majeure. Exemplos reais de acionamento."},
            {"title": "GAFTA e FOSFA — Contratos Padrão do Mercado",
             "prompt": "Escreva um artigo técnico sobre contratos padrão GAFTA e FOSFA para grãos, óleos e gorduras. GAFTA: principais formulários (GAFTA 100, 49, 78), regras de arbitragem, Default Rules. FOSFA: óleos vegetais e sementes oleaginosas, regras de arbitragem. Por que usar contratos-padrão vs customizados, adaptações para operações Brasil."},
        ]
    },
    {
        "category": "Logística Portuária",
        "articles": [
            {"title": "Porto de Santos — Operações de Granel Agrícola",
             "prompt": "Escreva um artigo técnico sobre o Porto de Santos para exportação de commodities agrícolas. Inclua: terminais graneleiros (Rumo, Cargill, Bunge, Terminal 37, Termag), capacidade e taxa de embarque MT/hora, draft máximo por berço, filas e sazonalidade, taxa de demurrage típica USD/dia, custo de THC e capatazia, caminhão vs ferrovia vs hidrovia, prazo médio de espera, procedimentos de inspeção, como agendar berço."},
            {"title": "Paranaguá e Terminais — Segundo Porto de Grãos do Brasil",
             "prompt": "Escreva um artigo técnico sobre o Porto de Paranaguá (APPA) para exportação de commodities. Inclua: terminais (Coamo, Cotriguaçu, Cargill, Bunge), capacidade total, ferrovia Rumo corredor Centro-Leste, sazonalidade, draft máximo, comparação com Santos, porto de Antonina como alternativa, taxas portuárias, procedimento MAPA."},
            {"title": "Demurrage, Despatch e Gestão de Navio",
             "prompt": "Escreva um artigo técnico completo sobre demurrage e despatch em operações de commodities. Inclua: laytime (SHEX, SSHINC, WWD), NOR (quando e como apresentar), Time Sheet, demurrage (USD/dia), despatch (50% demurrage), responsabilidade por Incoterm, Statement of Facts, causas comuns de demurrage no Brasil, como negociar cláusula de demurrage."},
            {"title": "Armadores, Chartering e Afretamento de Navios",
             "prompt": "Escreva um artigo técnico sobre afretamento e logística marítima para commodities. Inclua: tipos de navio (Handysize, Supramax, Panamax, Post-Panamax, Capesize), qual navio para cada commodity/rota, tipos de charter (voyage, time, COA), principais armadores, fatores de frete, Baltic Dry Index (BDI), corretores de frete (Clarksons, Braemar), book-ahead."},
            {"title": "Fumigação, Tratamento e Saúde de Carga",
             "prompt": "Escreva um artigo técnico sobre tratamento fitossanitário de cargas para exportação. Inclua: fumigação com fosfina (PH3) — protocolo, dosagem, tempo, Certificate of Fumigation; tratamento térmico; MB (Brometo de Metila) e restrições; exigências por país de destino; custo médio; prazo de realização; responsabilidade exportador vs armador."},
        ]
    },
    {
        "category": "Regulatório Brasil",
        "articles": [
            {"title": "MAPA — Habilitação e Certificações para Exportar Alimentos",
             "prompt": "Escreva um artigo técnico sobre o papel do MAPA na exportação de commodities. Inclua: SIF, VIGIAGRO, certificados emitidos (fitossanitário, zoossanitário, fumigação, origem), LPCO no SISCOMEX, habilitação para China (GACC), UE, EUA (FSVP), auditorias por país, prazo médio de emissão, custo."},
            {"title": "Receita Federal — Regimes Aduaneiros Especiais para Export",
             "prompt": "Escreva um artigo técnico sobre regimes aduaneiros especiais para exportação de commodities no Brasil. Inclua: Drawback (suspensão de impostos, simples vs especial, comprovação), RECOF, entreposto aduaneiro, canais verde/amarelo/vermelho/cinza, AFRMM, ICMS na exportação, PIS/COFINS, como recuperar créditos tributários."},
            {"title": "RADAR — Habilitação para Exportar no SISCOMEX",
             "prompt": "Escreva um artigo técnico sobre o RADAR da Receita Federal. Inclua: o que é e por que é obrigatório, tipos de habilitação (expressa, limitada, ilimitada — critérios), documentos exigidos, prazo de análise, motivos de indeferimento, como monitorar situação, RADAR para corretores e despachantes, suspensão e reativação, importância para ACC."},
        ]
    },
    {
        "category": "Mercado Futuro CBOT/ICE",
        "articles": [
            {"title": "CBOT — Futuros de Soja, Milho e Trigo: Como Funcionam",
             "prompt": "Escreva um artigo técnico sobre contratos futuros de soja, milho e trigo na CBOT/CME Group. Inclua: especificação do contrato de soja (5000 bushels, cents/bushel, meses ZF/ZH/ZK/ZN/ZU/ZX), conversão bushel para MT, contrato de milho, leitura de cotação, basis (diferença físico-futuro), backwardation vs contango, como exportadores brasileiros usam CBOT, participantes (hedgers, especuladores, fundos)."},
            {"title": "ICE Futuros — Açúcar #11, Café C e Algodão #2",
             "prompt": "Escreva um artigo técnico sobre contratos futuros de açúcar, café e algodão na ICE. AÇÚCAR #11 (SB): 50 long tons, cents/lb, conversão cents/lb para USD/MT (×22.0462), meses, determinantes de preço. CAFÉ C (KC): arábica 37.500 lb, cents/lb, diferenciais por origem. ALGODÃO #2 (CT): 50.000 lb, cents/lb, grading. Uso prático pelo exportador brasileiro para hedge."},
            {"title": "Basis, Prêmio e Formação de Preço Físico",
             "prompt": "Escreva um artigo técnico sobre basis e formação de preço no mercado físico de commodities. Inclua: definição de basis, por que o basis existe, basis FOB Santos para soja (histórico: -40 a +20 cents/bu), basis positivo vs negativo, como o exportador precifica (CBOT + basis + câmbio), offer price, negociação de basis fixo vs floating price, rolled hedge, exemplos numéricos completos."},
        ]
    },
    {
        "category": "Destinos por Commodity",
        "articles": [
            {"title": "China — Maior Importador de Commodities: Exigências e Procedimentos",
             "prompt": "Escreva um artigo técnico sobre exportação de commodities agrícolas do Brasil para a China. Inclua: registro no GACC, exigências fitossanitárias (soja: Phyto + Fumigação obrigatórios), tolerâncias para aflatoxinas/pesticidas/OGM, sazonalidade de compras, principais importadores chineses (COFCO, Sinograin), impacto das tarifas guerra comercial EUA-China, pagamento (LC por grandes traders), documentos adicionais exigidos."},
            {"title": "Oriente Médio — Açúcar, Soja e Cereais: Exigências Halal",
             "prompt": "Escreva um artigo técnico sobre exportação de commodities do Brasil para Arábia Saudita, EAU, Kuwait, Qatar, Egito, Jordânia. Inclua: exigências Halal (certificadores no Brasil, para quais produtos, auditoria de planta), SASO, SFDA Saudi, Egito como maior importador de trigo e açúcar, UAE como hub de reexportação, pagamento (LC irrevogável confirmada padrão), Incoterm preferido (CIF destino), sazonalidade religiosa (Ramadã), importadores principais."},
            {"title": "Sudeste Asiático — Filipinas, Vietnã, Indonésia, Tailândia",
             "prompt": "Escreva um artigo técnico sobre exportação de commodities para o Sudeste Asiático. FILIPINAS: açúcar refinado ICUMSA 45, VHP para refinação, milho para ração; VIETNAM: soja farelo, milho, frango congelado; INDONESIA: açúcar VHP para refinação, regulação import sugar; TAILÂNDIA: mandioca/amido, soja farelo. Pagamento, certificações especiais, agentes locais vs trading house."},
            {"title": "África Subsaariana — Oportunidades e Riscos",
             "prompt": "Escreva um artigo técnico sobre exportação de commodities para a África Subsaariana. Inclua: principais destinos (Nigéria, Etiópia, África do Sul, Quênia, Angola), commodities mais demandadas, riscos específicos (câmbio instável, risco político, fraudes, infraestrutura limitada), como mitigar riscos (LC confirmada obrigatória, ratings bancários, intermediários), Incoterm recomendado, certificações religiosas Halal."},
        ]
    },
    {
        "category": "Fraudes e Red Flags",
        "articles": [
            {"title": "Ghost Offers e Fraudes em Trading de Commodities",
             "prompt": "Escreva um artigo técnico detalhado sobre fraudes comuns no mercado de trading de commodities. Inclua: Ghost Offers (identificar preços abaixo do mercado, urgência artificial, documentos falsos FCO/LOI/ICPO), Sellers falsos (verificar estoque, visita técnica, AIS tracking), Mandatários (daisy chain de intermediários), Soft Probe, red flags: MT760/MT799 exigidos antes de verificação, termos 'ready-willing-able', 'PB first'."},
            {"title": "Due Diligence em Compradores e Vendedores Internacionais",
             "prompt": "Escreva um artigo técnico sobre due diligence em operações de trading de commodities. Inclua: verificação de contraparte (registro, balanço, referências), compliance (OFAC, UE Sanctions, COAF, PEP), validação de banco emitente de LC, validação de documentos (BL, COO, Phyto), site visit, contratos de confidencialidade, referências de traders, plataformas de verificação (D&B, Bureau van Dijk, Panjiva)."},
            {"title": "SWIFT Falsos e Instrumentos Financeiros Fraudulentos",
             "prompt": "Escreva um artigo técnico sobre fraudes com instrumentos financeiros em comércio exterior. Inclua: MT103/MT760/MT799/MT700 (o que cada um é), como verificar autenticidade de SWIFT (Swift GPI tracker, correspondente bancário), BG e SBLC falsos, 'monetização de instrumentos', Performance Bond legítimo vs fraudulento, PPP (Private Placement Programs — sempre fraude), como banco legítimo procede, casos reais documentados."},
        ]
    },
]

def generate_article(title: str, prompt: str) -> str:
    full_prompt = f"""Você é um especialista em trading internacional de commodities agrícolas com 20 anos de experiência.

TAREFA: {prompt}

DIRETRIZES:
- Conteúdo técnico, factual e preciso — sem generalidades
- Inclua números, percentuais, valores típicos de mercado onde relevante
- Use terminologia técnica correta do setor
- Estruture com seções claras usando markdown (## subtítulo)
- Entre 900 e 1400 palavras
- Foco em aplicação prática para traders/exportadores brasileiros

TÍTULO: {title}

ARTIGO:"""

    _rpm_wait()
    resp = client.models.generate_content(
        model=MODEL_GEN,
        contents=full_prompt,
        config=gtypes.GenerateContentConfig(max_output_tokens=2000, temperature=0.2),
    )
    return (resp.text or "").strip()


def save_article(title: str, category: str, content: str, embedding: bytes):
    from models.database import get_session, CorporateKnowledge
    from datetime import datetime

    sess = get_session()
    try:
        doc_name = f"KB_{category}_{title[:50]}"
        existing = sess.query(CorporateKnowledge)\
            .filter(CorporateKnowledge.document_name == doc_name).first()
        if existing:
            existing.content    = content
            existing.embedding  = embedding
            existing.indexed_at = datetime.utcnow()
            print(f"    [update] {title[:65]}")
        else:
            sess.add(CorporateKnowledge(
                document_name = doc_name,
                content       = content,
                embedding     = embedding,
            ))
            print(f"    [insert] {title[:65]}")
        sess.commit()
    finally:
        sess.close()


def enrich_new():
    total = sum(len(c["articles"]) for c in KNOWLEDGE_TAXONOMY)
    print(f"\n=== Gerando {total} artigos novos de domínio ===\n")
    ok = errors = 0

    for cat_data in KNOWLEDGE_TAXONOMY:
        print(f"\n[{cat_data['category']}]")
        for art in cat_data["articles"]:
            try:
                content = generate_article(art["title"], art["prompt"])
                print(f"  {len(content.split()):4d} palavras — gerando embedding...")
                emb = embed_text(content)
                save_article(art["title"], cat_data["category"], content, emb)
                ok += 1
            except Exception as e:
                print(f"  ERRO: {e}")
                errors += 1
                time.sleep(5)

    print(f"\nArtigos novos: {ok} gerados | {errors} erros")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only-reembed", action="store_true")
    parser.add_argument("--only-enrich",  action="store_true")
    args = parser.parse_args()

    if not args.only_enrich:
        reembed_existing()

    if not args.only_reembed:
        enrich_new()

    # Resumo final
    from models.database import get_session, CorporateKnowledge
    from sqlalchemy import func
    sess = get_session()
    total = sess.query(func.count(CorporateKnowledge.id)).scalar()
    with_emb = sess.query(func.count(CorporateKnowledge.id))\
        .filter(CorporateKnowledge.embedding != None).scalar()
    # conta embeddings no formato correto (12288 bytes = 3072 * 4)
    correct = 0
    for c in sess.query(CorporateKnowledge).filter(CorporateKnowledge.embedding != None).all():
        if isinstance(c.embedding, bytes) and len(c.embedding) == 12288:
            correct += 1
    sess.close()

    print(f"\n{'='*60}")
    print(f"BASE FINAL: {total} chunks | {with_emb} com embedding | {correct} no formato correto (3072 dims)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
