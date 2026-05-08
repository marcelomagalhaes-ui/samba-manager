"""
scripts/enrich_knowledge_base.py
==================================
Enriquecimento sistemático da CorporateKnowledge com domínio de
trading de commodities: Incoterms, documentação, specs, trade finance,
contratos, logística portuária, regulatório Brasil, futuros, destinos
por commodity, fraudes.

Cada artigo é gerado pelo Gemini 2.5 Flash Lite com instrução precisa
de conteúdo factual e técnico, depois embedado e salvo no banco.

Custo estimado: USD 0.21 (~BRL 1.22) para 259 artigos.

Uso:
    python scripts/enrich_knowledge_base.py
    python scripts/enrich_knowledge_base.py --dry-run   # só mostra tópicos
    python scripts/enrich_knowledge_base.py --category "Incoterms 2020"
"""
from __future__ import annotations

import argparse
import struct
import sys
import time
import os

# ── Path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(override=True)

from google import genai
from google.genai import types as gtypes

GEMINI_KEY   = os.getenv("GEMINI_API_KEY", "")
MODEL_GEN    = "models/gemini-2.5-flash-lite"   # geração de texto
MODEL_EMB    = "models/text-embedding-004"       # embeddings
RPM_LIMIT    = 8    # chamadas/min (free tier: 10 — com margem)

client = genai.Client(api_key=GEMINI_KEY)

# ── Rate limiter simples ───────────────────────────────────────────────────────
import threading
_rpm_lock  = threading.Lock()
_rpm_calls: list[float] = []

def _rpm_wait():
    with _rpm_lock:
        now = time.monotonic()
        _rpm_calls[:] = [t for t in _rpm_calls if now - t < 60.0]
        if len(_rpm_calls) >= RPM_LIMIT:
            wait = 60.0 - (now - _rpm_calls[0]) + 1.0
            print(f"    [rate limit] aguardando {wait:.0f}s...", flush=True)
            time.sleep(max(wait, 0))
            _rpm_calls[:] = [t for t in _rpm_calls if time.monotonic() - t < 60.0]
        _rpm_calls.append(time.monotonic())


# ── Taxonomia de conhecimento ──────────────────────────────────────────────────

KNOWLEDGE_TAXONOMY = [

    # ── 1. INCOTERMS 2020 ─────────────────────────────────────────────────────
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
             "prompt": "Escreva um artigo prático sobre erros comuns no uso de Incoterms em contratos de commodities agrícolas. Inclua: confusão FOB vs CFR no Brasil, risco de usar EXW em exportação, problemas com CIF quando o seguro é insuficiente, como especificar o ponto geográfico correto (ex: 'FOB Santos' vs 'FOB vessel Santos'), cláusulas adicionais necessárias no contrato, diferença Incoterms 2010 vs 2020. Exemplos reais do mercado."},
        ]
    },

    # ── 2. DOCUMENTAÇÃO EXPORT/IMPORT ─────────────────────────────────────────
    {
        "category": "Documentação Export/Import",
        "articles": [
            {"title": "Bill of Lading (BL/MBL/HBL) — Guia Completo",
             "prompt": "Escreva um artigo técnico completo sobre o Bill of Lading (Conhecimento de Embarque). Inclua: o que é e sua função jurídica, tipos (Master BL, House BL, Switch BL, Straight BL, Order BL, Bearer BL), campos obrigatórios (shipper, consignee, notify party, port of loading, port of discharge, description of goods, quantity/weight, freight terms), endosso e negociabilidade, BL vs Sea Waybill, problemas comuns (erros de digitação, cláusulas de reserva), procedimento de emissão com armadores."},
            {"title": "Certificado de Origem — Tipos e Procedimentos",
             "prompt": "Escreva um artigo técnico sobre Certificados de Origem para exportação brasileira de commodities. Inclua: CO Não-Preferencial (Form A, Câmara de Comércio), CO Preferencial (Mercosul, SGP, SGPC, acordo UE-Mercosul), quem emite no Brasil (MDIC, Câmaras de Comércio acreditadas), quando é obrigatório vs opcional, drawback e CO, como preencher corretamente, erros frequentes e consequências, diferença CO origem vs CO do produto final. Exemplos para soja, açúcar, milho."},
            {"title": "Certificado Fitossanitário e Documentação MAPA",
             "prompt": "Escreva um artigo técnico sobre documentação fitossanitária para exportação de commodities agrícolas do Brasil. Inclua: Certificado Fitossanitário (emitido pelo MAPA/VIGIAGRO), Certificado de Fumigação (MB, Foster, SF), Certificado de Tratamento Térmico, quando cada documento é exigido por país de destino, prazo de validade, o que é LPCO (Licença de Produto Controlado), fluxo de inspeção nos portos, custo médio de fumigação por container/BU, tabela de exigências por commodity/destino."},
            {"title": "Invoice Comercial e Packing List — Preenchimento Correto",
             "prompt": "Escreva um guia técnico sobre Commercial Invoice e Packing List para exportação de commodities. Inclua: campos obrigatórios da Invoice (seller, buyer, invoice number, date, description, HS code, quantity, unit price, total, Incoterm, payment terms, bank details), erros que travam o despacho aduaneiro, como descrever commodities agrícolas (soybean meal 48% protein, raw sugar ICUMSA 45, yellow corn No.2), pesos líquido vs bruto, diferença Invoice vs Proforma Invoice vs Final Invoice, rastreabilidade do packing list."},
            {"title": "SISCOMEX, RADAR e Registro de Exportação (RE/DU-E)",
             "prompt": "Escreva um artigo técnico sobre o sistema de exportação brasileiro. Inclua: o que é SISCOMEX e quem pode acessar, RADAR Receita Federal (tipos: limitado, ilimitado), como habilitar empresa para exportar, o que é Registro de Exportação (RE) e quando era usado, transição para DU-E (Declaração Única de Exportação), campos do DU-E, integração com MAPA (ADE-MAPA), vinculação com BL e Invoice, prazos e penalidades por irregularidades, papel do despachante aduaneiro."},
            {"title": "Inspeção SGS/Bureau Veritas/Intertek — Como Funciona",
             "prompt": "Escreva um artigo técnico sobre inspeção de commodities por empresas certificadoras. Inclua: papel da SGS, Bureau Veritas, Intertek e COTECNA, tipos de inspeção (pre-shipment, loading, arrival), o que é inspecionado (peso, umidade, proteína, impurezas, grãos avariados), Certificate of Analysis vs Certificate of Weight, Draft Survey, amostragem e procedimento (quantas amostras por BU), resultados fora de spec — quem paga, quando o comprador exige certificação e quando é negociável, custo médio por serviço."},
        ]
    },

    # ── 3. SPECS DE COMMODITIES ───────────────────────────────────────────────
    {
        "category": "Specs de Commodities",
        "articles": [
            {"title": "Soja Brasileira — Especificações Técnicas Completas",
             "prompt": "Escreva um artigo técnico completo sobre especificações da soja brasileira para exportação. Inclua: classificação CONAB/MAPA (tipos 1, 2, 3), parâmetros-chave (umidade máx 14%, impurezas máx 1%, grãos avariados, proteína 34-38%, óleo 18-22%), diferença soja GMO vs non-GMO (preço premium, certificação RTRS, mercados que exigem non-GMO como UE), soja para farelo vs soja para óleo, safras brasileira (jan-abril) e americana (out-nov), cotação base CBOT, portos de embarque (Paranaguá, Santos, Aratu, Itaqui), umidade e desconto."},
            {"title": "Açúcar VHP e ICUMSA 45 — Especificações e Mercados",
             "prompt": "Escreva um artigo técnico sobre especificações de açúcar para exportação. Inclua: o que é ICUMSA (International Commission for Uniform Methods of Sugar Analysis), escala ICUMSA (45 = branco refinado, 150 = cristal especial, 600-1200 = VHP/demerara), parâmetros VHP (cor ICUMSA 600-1200, polarização mín 99.3°, umidade máx 0.15%, cinzas condutimétrica), diferença VHP vs ICUMSA 45 vs cristal, quais mercados compram qual tipo (Oriente Médio e Ásia: ICUMSA 45; refinarias: VHP), preço diferencial entre tipos, embalagem (bulk vs big bag vs 50kg)."},
            {"title": "Milho — Especificações Técnicas e Padrões Internacionais",
             "prompt": "Escreva um artigo técnico sobre especificações do milho brasileiro para exportação. Inclua: classificação CONAB (tipos 1, 2, 3), parâmetros (umidade máx 13.5-14%, impurezas máx 1%, grãos avariados máx 6%, aflatoxina máx 20ppb), diferença milho amarelo No.2 USDA vs padrão brasileiro, exigências da China (aflatoxinas, OGM), sazonalidade (safra verão jan-mar, safrinha maio-jul), portos (Paranaguá 40% do volume, Santos, Aratu, Itaqui), cotação CBOT vs diferencial local, uso final (ração animal vs etanol vs alimentação humana)."},
            {"title": "Farelo e Óleo de Soja — Specs e Mercados",
             "prompt": "Escreva um artigo técnico sobre farelo de soja (soybean meal) e óleo de soja para exportação. FARELO: proteína 46-48% (high pro) ou 44-46% (low pro), umidade máx 12%, fibra, atividade ureática, lisina, parâmetros de qualidade, mercados (UE: ração para aves e suínos, Oriente Médio, Sudeste Asiático), preço base CME (ZM futures). ÓLEO: acidez, umidade e impurezas, índice de iodo, degomagem, mercados (alimentício vs biodiesel), diferencial crude vs degomado vs refinado. Sazonalidade e logística."},
            {"title": "Café — Tipos, Graus e Especificações para Export",
             "prompt": "Escreva um artigo técnico sobre especificações de café para exportação. Inclua: Arábica vs Robusta (Conilon), classificação brasileira (Fine Cup, Good Cup), grão verde vs torrado e moído, defeitos (peneira, black beans, sticks), certificações (RFA, UTZ, Fair Trade, Organic, 4C), cotação base ICE (KC=F) para arábica e LIFFE para robusta, embalagem (juta 60kg vs big bag vs granel), principais origens (Sul de Minas, Cerrado, Bahia Oeste, Espírito Santo Conilon), destino por tipo (Alemanha, EUA, Japão, Bélgica), impacto de safra bicolor."},
            {"title": "Algodão — Especificações HVI e Mercado Internacional",
             "prompt": "Escreva um artigo técnico sobre algodão (cotton) para exportação. Inclua: parâmetros HVI (High Volume Instrument): MIKE (finura), UHML (comprimento), UI (uniformidade), STR (resistência), SCI (cotação), Rd e +b (cor/reflectância), trash (impurezas), grades USDA, plumas brasileiras (Cerrado MT/BA/GO, premiun mundial), certificação BCI e Organic, cotação ICE (CT=F cents/lb vs USD/MT conversão), embalagem (fardos 225kg), principais destinos (Bangladesh, Paquistão, Vietnã, Turquia, China), rastreabilidade e blockchain no setor."},
            {"title": "Cacau — Especificações, Fermentação e Mercado",
             "prompt": "Escreva um artigo técnico sobre cacau (cocoa) para exportação. Inclua: cacau em amêndoa vs manteiga vs massa, grau de fermentação (mín 75% fermentado para qualidade), umidade máx 7.5%, impurezas, aflatoxinas, parâmetros ICCO, origem brasileira (Bahia, Pará — cacau Fino de Aroma), diferença Forastero vs Trinitario vs Criollo, cotação ICE (CC=F USD/MT), hedge e basis, sazonalidade (duas safras: outubro-janeiro e maio-julho), principal destino Holanda (processamento), certificações (Rainforest, Fairtrade, RSPO livre), crise oferta 2023-2025."},
            {"title": "Óleo de Girassol — Specs e Contexto Geopolítico",
             "prompt": "Escreva um artigo técnico sobre óleo de girassol para importação e reexportação. Inclua: especificações (acidez, umidade, índice de iodo, refração), tipos (bruto vs refinado vs alto oleico), origens (Ucrânia 46% do mercado mundial, Rússia, Argentina, Brasil — plantio crescente no Cerrado), impacto da guerra Ucrânia-Rússia nos preços e supply chain 2022-2025, alternativas de origem (Argentina, Turquia, Moldávia), certificação (identity preserved, non-GMO), embalagem (IBC 1000L, tambores 200L, a granel), cotação e basis, rotas logísticas alternativas pós-bloqueio Mar Negro."},
            {"title": "Frango (Chicken Paw/Cortes) — Specs para Exportação Halal",
             "prompt": "Escreva um artigo técnico sobre exportação de frango e derivados do Brasil. Inclua: cortes principais para export (paw/pé de frango China, coxa+sobrecoxa Oriente Médio, peito UE), especificações sanitárias e de temperatura (congelado -18°C mín), certificação Halal (exigências, organismos certificadores no Brasil, abate ritual), certificações SIF (Serviço de Inspeção Federal), mercados por produto (China: 60% das patas; Arábia Saudita, EAU: cortes halal), barreiras (embargos por gripe aviária, zoneamento), cotação e formação de preço (real/kg), exportadores principais (JBS, BRF, Seara, Marfrig)."},
        ]
    },

    # ── 4. TRADE FINANCE ──────────────────────────────────────────────────────
    {
        "category": "Trade Finance",
        "articles": [
            {"title": "Letter of Credit (LC) — Guia Completo para Commodities",
             "prompt": "Escreva um artigo técnico completo sobre Letter of Credit (Carta de Crédito) em operações de commodities. Inclua: definição e partes (issuing bank, advising bank, confirming bank, beneficiary, applicant), tipos (irrevogável, confirmada, standby/SBLC, revolving, transferível, back-to-back), documentos exigidos pela LC (BL, Invoice, COO, Phyto, Certificate of Weight), UCP 600 regras, discrepâncias comuns e como evitar, prazo de apresentação de documentos, custo médio (% do valor), quando LC é preferível vs outros métodos, uso em operações de soja, açúcar, milho para compradores do Oriente Médio e África."},
            {"title": "SBLC, D/P, D/A e Open Account — Alternativas à LC",
             "prompt": "Escreva um artigo técnico sobre modalidades de pagamento em trade finance além da LC. Inclua: SBLC (Standby Letter of Credit) — diferença de LC documentária, uso como garantia, quando usar; D/P (Documents against Payment) — risco do exportador, fluxo documentário; D/A (Documents against Acceptance) — risco de crédito, desconto de promissória; Open Account — risco total exportador, quando aceitar, uso com seguro de crédito; Advance Payment — % adiantado, riscos, uso em small buyers; factoring e forfaiting de recebíveis de exportação; seguro de crédito SBCE/Coface/Euler Hermes."},
            {"title": "Câmbio e Hedge em Operações de Commodities",
             "prompt": "Escreva um artigo técnico sobre gestão cambial e hedge em exportações de commodities do Brasil. Inclua: ACC (Adiantamento sobre Cambiais Contratadas) e ACE — fluxo, prazo, custo, vantagem fiscal; NCE e CCE — notas de crédito à exportação; travamento de câmbio (NDF — Non-Deliverable Forward), opções de câmbio, swaps; Resolução BCB 277 — prazo de internalização; hedge natural vs financeiro; impacto USD/BRL nas margens do exportador; Receita de Exportação — obrigatoriedade de ingresso de divisas; exemplos práticos de hedge para vendas futuras de soja."},
            {"title": "Estruturação de Preço e Margem em Operações de Commodity",
             "prompt": "Escreva um artigo técnico sobre como estruturar e calcular preço e margem em operações de trading de commodities agrícolas. Inclua: componentes do preço (CBOT/ICE futures + basis + premium/discount + logística + impostos + margem), cálculo CIF a partir do FOB (frete marítimo + seguro + margem), diferenciação por Incoterm, impacto de câmbio, custo de carregamento (carry), back-to-back vs posição proprietária, quando fazer hedge de preço vs deixar aberto, P&L de uma operação típica de 5000 MT de soja FOB Santos, breakeven e stressed scenarios."},
        ]
    },

    # ── 5. CONTRATOS DE COMMODITIES ───────────────────────────────────────────
    {
        "category": "Contratos de Commodities",
        "articles": [
            {"title": "Estrutura do Contrato de Compra e Venda de Commodity",
             "prompt": "Escreva um artigo técnico sobre a estrutura de um contrato internacional de compra e venda de commodity agrícola. Inclua: partes contratantes (identificação completa, CNPJ, registro no país), definição do produto (descrição técnica, especificações, tolerâncias), quantidade (métrica de pesagem, tolerância +/-5% MOLCHC), qualidade (parâmetros, como/onde medir, autoridade final), preço (fixo vs provisional, base price, prêmio, revisão de preço), Incoterm e porto, data de embarque e laycan, termos de pagamento (% adiantamento, % contra BL), penalidades, governing law."},
            {"title": "Cláusulas de Qualidade, Peso e Arbitragem",
             "prompt": "Escreva um artigo técnico sobre cláusulas críticas em contratos de commodity: QUALIDADE — especificações técnicas com tolerâncias, certificadora final (SGS/BV/Intertek), procedimento de amostragem, lote de referência, penalidades por fora-de-spec (rejection vs renegotiation), análise de chegada vs embarque. PESO — peso de embarque vs chegada, Draft Survey, margem de tolerância (0.5% padrão), responsabilidade por perdas em trânsito. ARBITRAGEM — ICC Paris, GAFTA (London), FOSFA, ICAC (algodão), cláusula de mediação prévia, lei aplicável (English law vs Brazilian law), execução de laudo arbitral."},
            {"title": "Force Majeure, Hardship e Cláusulas de Risco",
             "prompt": "Escreva um artigo técnico sobre cláusulas de risco em contratos de commodities. FORCE MAJEURE — definição (eventos fora do controle), lista exemplificativa vs conceito aberto, obrigação de notificação (prazo 5-10 dias), documentação (certificates of impossibility), efeito (suspensão vs extinção), COVID-19 como força maior em 2020, guerra Ucrânia 2022. HARDSHIP — quando invocar (desequilíbrio econômico > 20%), renegociação obrigatória. SANCTIONS — cláusula OFAC/UE sanctions, consequências para partes sancionadas. QUALITY FORCE MAJEURE — safra com qualidade abaixo do normal."},
            {"title": "GAFTA e FOSFA — Contratos Padrão do Mercado",
             "prompt": "Escreva um artigo técnico sobre os contratos padrão GAFTA e FOSFA para grãos e óleos/gorduras. GAFTA (Grain and Feed Trade Association): principais formulários usados (GAFTA 100 para grãos FOB, GAFTA 49 para farinhas, GAFTA 78 para CIF), regras de arbitragem GAFTA, Default Rules, cláusulas especiais para contratos futuros. FOSFA (Federation of Oils, Seeds and Fats Associations): contratos de óleos vegetais e sementes oleaginosas, regras de arbitragem FOSFA. Por que usar contratos-padrão vs contratos customizados, adaptações necessárias para operações Brasil."},
        ]
    },

    # ── 6. LOGÍSTICA PORTUÁRIA ────────────────────────────────────────────────
    {
        "category": "Logística Portuária",
        "articles": [
            {"title": "Porto de Santos — Operações de Granel Agrícola",
             "prompt": "Escreva um artigo técnico sobre o Porto de Santos para exportação de commodities agrícolas. Inclua: terminais graneleiros (Rumo/ALL, Cargill, Bunge, Terminal 37, Termag), capacidade de armazenagem e taxa de embarque (MT/hora), draft máximo por berço, filas e sazonalidade (jan-abril = soja, ago-set = pico milho), taxa de demurrage típica (USD 15-25k/dia Panamax), custo de THC e capatazia, caminhão vs ferrovia (Rumo) vs hidrovia, prazo médio de espera, congestionamento vs períodos tranquilos, procedimentos de inspeção MAPA/Receita Federal, como agendar berço."},
            {"title": "Paranaguá e Terminals — Segundo Porto de Grãos do Brasil",
             "prompt": "Escreva um artigo técnico sobre o Porto de Paranaguá (APPA) para exportação de commodities. Inclua: terminais (Coamo, Cotriguaçu, Cargill, Bunge, Terminal de Contêineres, Fospar para fertilizantes), capacidade total, ferrovia América Latina Logística (ALL/Rumo) — corredor Centro-Leste, sazonalidade (forte em soja safra PR/MT), draft máximo, comparação com Santos (frete menor mas fila similar), Corredor de Exportação do Paraná, porto de Antonina como alternativa pequenos volumes, taxas portuárias, procedimento MAPA."},
            {"title": "Demurrage, Despatch e Gestão de Navio",
             "prompt": "Escreva um artigo técnico completo sobre demurrage e despatch em operações de commodities. Inclua: definição de laytime (tempo permitido para carga/descarga), como calcular laytime (SHEX, SSHINC, WWD, Weather Working Days), NOR (Notice of Readiness) — quando e como apresentar, who tenders NOR, Time Sheet — registro hora a hora, demurrage (multa por atraso — USD/dia), despatch (bônus por rapidez — 50% da demurrage), responsabilidade demurrage (FOB: vendedor até loading; CIF: comprador no destino), Statement of Facts (SOF), causas comuns de demurrage no Brasil (filas em Santos), como negociar cláusula de demurrage."},
            {"title": "Armadores, Chartering e Afretamento de Navios",
             "prompt": "Escreva um artigo técnico sobre afretamento e logística marítima para commodities. Inclua: tipos de navio (Handysize 20-35k MT, Supramax 50-60k MT, Panamax 65-80k MT, Post-Panamax 80k+, Capesize 150k+), qual navio para cada commodity/volume/rota, tipos de charter (voyage charter, time charter, COA — Contract of Affreightment), principais armadores de granel seco (Norden, Star Bulk, Pacific Basin), fatores de frete (combustível bunker, rota, congestionamento portuário), Baltic Dry Index (BDI) como referência, corretores de frete (Clarksons, Braemar), Book-Ahead de espaço em navios linha."},
            {"title": "Fumigação, Tratamento e Saúde de Carga",
             "prompt": "Escreva um artigo técnico sobre tratamento fitossanitário de cargas de commodities para exportação. Inclua: fumigação com fosfina (PH3) — protocolo, dosagem, tempo de exposição, temperatura mínima, Certificate of Fumigation, quem pode emitir no Brasil, tratamento térmico (60°C/60min — heat treatment), MB (Brometo de Metila) — restrições e banimento gradual, exigências por país de destino (China exige Certificate of Fumigation para soja, milho; Austrália/NZ exigências rigorosas), custo médio por BU/container, prazo de realização após embarque, responsabilidade do exportador vs armador."},
        ]
    },

    # ── 7. REGULATÓRIO BRASIL ─────────────────────────────────────────────────
    {
        "category": "Regulatório Brasil",
        "articles": [
            {"title": "MAPA — Habilitação e Certificações para Exportar Alimentos",
             "prompt": "Escreva um artigo técnico sobre o papel do MAPA (Ministério da Agricultura) na exportação de commodities e alimentos do Brasil. Inclua: SIF (Serviço de Inspeção Federal) — obrigatório para produtos de origem animal, como habilitar frigorífico/planta; VIGIAGRO — controle fitossanitário nos portos e aeroportos; certificados emitidos pelo MAPA (fitossanitário, zoossanitário, de fumigação, de origem); LPCO (Licença, Permissão, Certificado ou Outros) no SISCOMEX; habilitação de empresas para exportar para China (registro no GACC), UE, EUA (FSVP); auditorias por país; prazo médio de emissão de certificados; custo."},
            {"title": "Receita Federal — Regimes Aduaneiros Especiais para Export",
             "prompt": "Escreva um artigo técnico sobre regimes aduaneiros especiais relevantes para exportação de commodities no Brasil. Inclua: Drawback (suspensão de impostos na importação de insumos destinados à exportação) — simples vs especial, como calcular benefício, comprovação de exportação, prazo; RECOF — Regime Aduaneiro Especial; entreposto aduaneiro; DAP (Despacho Aduaneiro de Exportação); canal verde/amarelo/vermelho/cinza; atuação do despachante aduaneiro; AFRMM (fundo marinha mercante); ICMS — imunidade constitucional na exportação; PIS/COFINS — não incide sobre receita de exportação; como recuperar créditos tributários."},
            {"title": "RADAR — Habilitação para Exportar no SISCOMEX",
             "prompt": "Escreva um artigo técnico sobre o RADAR (Registro e Rastreamento da Atuação dos Intervenientes Aduaneiros) da Receita Federal. Inclua: o que é o RADAR e por que é obrigatório, tipos de habilitação (expressa, limitada, ilimitada — critérios de capital social e faturamento), documentos exigidos para habilitação inicial (pessoa jurídica e física), prazo de análise, motivos de indeferimento ou impedimento, como monitorar situação, alterações cadastrais (sócios, endereço, atividade), RADAR para corretores e despachantes, suspensão e reativação, importância do RADAR para receita de câmbio e ACC."},
        ]
    },

    # ── 8. MERCADO FUTURO CBOT/ICE ────────────────────────────────────────────
    {
        "category": "Mercado Futuro CBOT/ICE",
        "articles": [
            {"title": "CBOT — Futuros de Soja, Milho e Trigo: Como Funcionam",
             "prompt": "Escreva um artigo técnico sobre os contratos futuros de soja, milho e trigo na CBOT (Chicago Board of Trade, CME Group). Inclua: especificação do contrato de soja (5000 bushels, centavos/bushel, variação mínima, meses de vencimento ZF, ZH, ZK, ZN, ZU, ZX, ZZ), conversão bushel para MT (1 bushel soja = 27.2155 kg), contrato de milho (5000 bu), leitura de cotação, o que é basis (diferença físico-futuro), backwardation vs contango, como exportadores brasileiros usam CBOT para precificação (preço = CBOT + basis FOB Santos), quem são os participantes (hedgers, especuladores, fundos)."},
            {"title": "ICE Futuros — Açúcar #11, Café C e Algodão #2",
             "prompt": "Escreva um artigo técnico sobre os contratos futuros de açúcar, café e algodão na ICE (Intercontinental Exchange). AÇÚCAR #11 (SB): especificação (50 long tons, cents/lb), conversão cents/lb para USD/MT (×22.0462), meses de vencimento, o que determina o preço (oferta Brasil+Índia, câmbio BRL/USD, relação açúcar-etanol); CAFÉ C (KC): arábica 37.500 lb, cents/lb, diferenciais por origem (Santos 4, Colombia +20, Ethiopia +30); ALGODÃO #2 (CT): 50.000 lb, cents/lb, grading do cotton; uso prático pelo exportador brasileiro para hedge de posição vendida."},
            {"title": "Basis, Prêmio e Formação de Preço Físico",
             "prompt": "Escreva um artigo técnico sobre basis e formação de preço no mercado físico de commodities. Inclua: definição de basis (físico - futuro), por que o basis existe (frete, armazenagem, qualidade, localização), basis FOB Santos para soja (histórico: -40 a +20 cents/bu dependendo da demanda asiática), basis positivo (mercado pressionado) vs negativo, como o exportador brasileiro precifica (CBOT + basis + câmbio), offer price: CBOT settlement + basis + prêmio de qualidade, negociação de basis fixo vs floating price, rolled hedge quando se vende físico mas não há futuro correspondente, exemplos numéricos."},
        ]
    },

    # ── 9. DESTINOS POR COMMODITY ─────────────────────────────────────────────
    {
        "category": "Destinos por Commodity",
        "articles": [
            {"title": "China — Maior Importador de Commodities: Exigências e Procedimentos",
             "prompt": "Escreva um artigo técnico sobre exportação de commodities agrícolas do Brasil para a China. Inclua: registro de estabelecimentos no GACC (General Administration of Customs of China) — obrigatoriedade para soja, milho, carne, açúcar, procedimento de registro; exigências fitossanitárias (soja: Certificate of Phytosanitary + Certificate of Fumigation obrigatório); tolerâncias da China para aflatoxinas, pesticidas, OGM — diferenças vs padrão internacional; sazonalidade de compras (soja: out-jan para embarque nov-abr); estratégia de compras (COFCO, Sinograin, Amaggi-China, ADM-China); impacto das tarifas guerra comercial EUA-China em compras do Brasil; pagamento: LC por grandes traders, open account com garantia bancária; documentos adicionais exigidos."},
            {"title": "Oriente Médio — Açúcar, Soja e Cereais: Exigências Halal",
             "prompt": "Escreva um artigo técnico sobre exportação de commodities do Brasil para o Oriente Médio (Arábia Saudita, EAU, Kuwait, Qatar, Egito, Jordânia). Inclua: exigências Halal — quem certifica no Brasil (CDIAL Halal, CIBAL Halal, IBR — Islamic Brazilian Reference), para quais produtos é obrigatório vs opcional, procedimento de auditoria de planta; SASO (Saudi Standards) para açúcar e produtos alimentícios; importância da certificação ISCC para sustentabilidade; GACC árabe equivalent (SFDA Saudi); Egito como maior importador de trigo e açúcar; UAE como hub de reexportação; pagamento: LC irrevogável confirmada (padrão); Incoterm preferido: CIF destino; sazonalidade religiosa (Ramadã — pico de consumo); importadores principais (Al-Khaleej Sugar, FARAGALLA Group)."},
            {"title": "Sudeste Asiático — Filipinas, Vietnã, Indonésia, Tailândia",
             "prompt": "Escreva um artigo técnico sobre exportação de commodities agrícolas para o Sudeste Asiático. FILIPINAS: maior importador de açúcar refinado ICUMSA 45, açúcar VHP para refinação local, milho para ração; VIETNAM: soja farelo (forte crescimento suinocultura), milho, frango congelado, volume crescente 2020-2025; INDONESIA: açúcar VHP para refinação (200+ refinarias), regulação import sugar (cota e licença), impacto regulação local; TAILÂNDIA: exportador e importador de mandioca/amido, soja farelo; pagamento: mix LC e D/P; certificações especiais por país (Vietnam: QC, Filipinas: SPS clearance); agentes locais vs trading house."},
            {"title": "África Subsaariana — Oportunidades e Riscos",
             "prompt": "Escreva um artigo técnico sobre exportação de commodities para a África Subsaariana. Inclua: principais destinos (Nigéria — maior PIB, importador de arroz/trigo/açúcar; Etiópia — crescimento rápido; África do Sul — reexportador regional; Quênia; Angola); commodities mais demandadas (açúcar, frango, milho, trigo, óleo de soja); riscos específicos (risco de câmbio — moedas locais instáveis, como Naira nigeriana; risco político; fraudes documentais; infraestrutura portuária limitada); como mitigar riscos (LC confirmada obrigatória, avaliar ratings bancários, usar intermediários estabelecidos); incoterm recomendado (CIF ou DAP para controle total); certificações religiosas (Halal para norte/leste da África)."},
        ]
    },

    # ── 10. FRAUDES E RED FLAGS ───────────────────────────────────────────────
    {
        "category": "Fraudes e Red Flags",
        "articles": [
            {"title": "Ghost Offers e Fraudes em Trading de Commodities",
             "prompt": "Escreva um artigo técnico detalhado sobre fraudes comuns no mercado de trading de commodities agrícolas. Inclua: GHOST OFFERS — definição, como identificar (preços muito abaixo do mercado, urgência artificial, documentos falsos como FCO/LOI/ICPO), o que é FCO (Full Corporate Offer) legítimo vs fraudulento; SELLERS FALSOS — como verificar existência do estoque, visita técnica obrigatória, rastreio de navio no AIS; MANDATÁRIOS — estrutura 'daisy chain' de intermediários sem acesso ao produto, como identificar; SOFT PROBE — procedimento enganoso de verificação; red flags principais: MT760/MT799 exigidos antes de verificação, termos como 'ready-willing-able', 'PB (Performance Bond) first'."},
            {"title": "Due Diligence em Compradores e Vendedores Internacionais",
             "prompt": "Escreva um artigo técnico sobre due diligence em operações de trading de commodities. Inclua: verificação de contraparte (empresa existe? registro comercial, balanço, referências bancárias); compliance (listas OFAC, UE Sanctions, COAF, PEP — Politically Exposed Persons); verificação de banco emitente de LC (SWIFT BIC, rating, correspondente); validação de documentos (como verificar autenticidade de BL, COO, Phyto via organismos emissores); site visit e pré-inspeção de estoque físico; contratos de confidencialidade (NDA) — quando assinar e quando suspeitar de NDA antes de qualquer informação; referências de traders — como pedir e usar; plataformas de verificação (Dun & Bradstreet, Bureau van Dijk, Panjiva)."},
            {"title": "SWIFT Falsos e Instrumentos Financeiros Fraudulentos",
             "prompt": "Escreva um artigo técnico sobre fraudes com instrumentos financeiros em comércio exterior. Inclua: MT103/MT760/MT799/MT700 — o que cada mensagem SWIFT é para (MT700 = LC, MT760 = Bank Guarantee, MT799 = free format, MT103 = customer payment), como verificar autenticidade de um SWIFT (portal Swift MyStandards, correspondente bancário, SWIFT GPI tracker); BG (Bank Guarantee) e SBLC falsos — características, como identificar; 'monetização de instrumentos' — esquemas fraudulentos; Performance Bond legítimo vs fraudulento; PPP (Private Placement Programs) — sempre fraude; como um banco legítimo procede para emitir LC vs como fraudadores se comportam; casos reais documentados."},
        ]
    },
]

# ── Funções de geração e embedding ────────────────────────────────────────────

def generate_article(title: str, prompt: str) -> str:
    """Gera artigo técnico com Gemini."""
    full_prompt = f"""Você é um especialista em trading internacional de commodities agrícolas com 20 anos de experiência.

TAREFA: {prompt}

DIRETRIZES:
- Conteúdo técnico, factual e preciso — sem generalidades
- Inclua números, percentuais, valores típicos de mercado onde relevante
- Use terminologia técnica correta do setor (inglês técnico onde aplicável)
- Estruture com seções claras usando markdown (## subtítulo)
- Entre 900 e 1400 palavras
- Foco em aplicação prática para traders/exportadores brasileiros
- NÃO invente dados — use apenas informação consolidada do setor

TÍTULO: {title}

ARTIGO:"""

    _rpm_wait()
    resp = client.models.generate_content(
        model=MODEL_GEN,
        contents=full_prompt,
        config=gtypes.GenerateContentConfig(
            max_output_tokens=2000,
            temperature=0.2,
        ),
    )
    return (resp.text or "").strip()


def embed_text(text: str) -> bytes:
    """Gera embedding e retorna como bytes para armazenar no DB."""
    _rpm_wait()
    result = client.models.embed_content(
        model=MODEL_EMB,
        contents=text[:8000],  # limite do modelo
    )
    floats = result.embeddings[0].values
    return struct.pack(f"{len(floats)}f", *floats)


def save_to_db(title: str, category: str, content: str, embedding: bytes):
    """Salva artigo + embedding na CorporateKnowledge."""
    from models.database import get_session, CorporateKnowledge
    from datetime import datetime

    sess = get_session()
    try:
        # Verifica se já existe
        existing = sess.query(CorporateKnowledge)\
            .filter(CorporateKnowledge.document_name == f"KB_{category}_{title[:50]}")\
            .first()
        if existing:
            existing.content   = content
            existing.embedding = embedding
            existing.indexed_at = datetime.utcnow()
            print(f"    [update] {title[:60]}")
        else:
            chunk = CorporateKnowledge(
                document_name = f"KB_{category}_{title[:50]}",
                content       = content,
                embedding     = embedding,
                source_type   = "knowledge_base",
                indexed_at    = datetime.utcnow(),
            )
            sess.add(chunk)
            print(f"    [insert] {title[:60]}")
        sess.commit()
    finally:
        sess.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",  action="store_true", help="Só lista tópicos sem gerar")
    parser.add_argument("--category", default=None,        help="Gera apenas uma categoria")
    args = parser.parse_args()

    total = sum(len(cat["articles"]) for cat in KNOWLEDGE_TAXONOMY)
    print(f"=== SAMBA Knowledge Enrichment ===")
    print(f"Total de artigos a gerar: {total}")
    print(f"Modelo: {MODEL_GEN} | Embedding: {MODEL_EMB}")
    print()

    if args.dry_run:
        for cat in KNOWLEDGE_TAXONOMY:
            print(f"[{cat['category']}] — {len(cat['articles'])} artigos")
            for art in cat["articles"]:
                print(f"  · {art['title']}")
        return

    generated = 0
    errors    = 0

    for cat_data in KNOWLEDGE_TAXONOMY:
        category = cat_data["category"]

        if args.category and args.category.lower() not in category.lower():
            continue

        print(f"\n{'='*60}")
        print(f"CATEGORIA: {category}")
        print(f"{'='*60}")

        for art in cat_data["articles"]:
            title = art["title"]
            print(f"\n  Gerando: {title[:70]}...")

            try:
                # 1. Gera o artigo
                content = generate_article(title, art["prompt"])
                word_count = len(content.split())
                print(f"    {word_count} palavras geradas")

                # 2. Gera embedding
                print(f"    Gerando embedding...")
                emb = embed_text(content)

                # 3. Salva no DB
                save_to_db(title, category, content, emb)
                generated += 1

            except Exception as e:
                print(f"    ERRO: {e}")
                errors += 1
                time.sleep(5)  # pausa extra em erro

    print(f"\n{'='*60}")
    print(f"CONCLUÍDO: {generated} artigos gerados | {errors} erros")
    print(f"{'='*60}")

    # Verifica total na base
    from models.database import get_session, CorporateKnowledge
    from sqlalchemy import func
    sess = get_session()
    total_db = sess.query(func.count(CorporateKnowledge.id)).scalar()
    sess.close()
    print(f"Total chunks na CorporateKnowledge: {total_db}")


if __name__ == "__main__":
    main()
