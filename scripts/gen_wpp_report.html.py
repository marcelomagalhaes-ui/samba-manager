"""
Gera docs/SAMBA_WPP_AGENTS_REPORT.html
Abrir no Chrome → Ctrl+P → Salvar como PDF
"""
import pathlib, datetime

TODAY = datetime.date.today().strftime("%d de %B de %Y")
OUT   = pathlib.Path(__file__).parent.parent / "docs" / "SAMBA_WPP_AGENTS_REPORT.html"

HTML = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>Agentes WhatsApp — Samba Export</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: Arial, sans-serif;
    font-size: 10pt;
    color: #333;
    background: #fff;
    max-width: 900px;
    margin: 0 auto;
    padding: 32px 40px;
  }}

  /* ── CAPA ─────────────────────────────── */
  .cover {{ page-break-after: always; padding-bottom: 60px; }}
  .cover-bar {{
    background: #FA8200;
    color: #fff;
    padding: 18px 22px;
    border-radius: 3px;
    margin-bottom: 48px;
  }}
  .cover-bar h1 {{ font-size: 13pt; margin-bottom: 4px; }}
  .cover-bar p  {{ font-size: 9pt; color: #FFE8C0; }}
  .cover-title  {{ font-size: 32pt; font-weight: bold; color: #FA8200; margin-bottom: 8px; }}
  .cover-sub    {{ font-size: 14pt; color: #666; margin-bottom: 16px; }}
  .cover-phone  {{ font-size: 13pt; color: #FA8200; font-weight: bold; }}
  .cover-hr     {{ border: none; border-top: 3px solid #FA8200; margin: 28px 0; }}
  .meta-table   {{ width: 100%; border-collapse: collapse; margin-bottom: 32px; }}
  .meta-table td {{ padding: 4px 8px; font-size: 9pt; vertical-align: top; }}
  .meta-table td:first-child {{ font-weight: bold; color: #666; width: 160px; }}
  .toc-box {{
    border-left: 5px solid #FA8200;
    border-top: 2px solid #FA8200;
    border-bottom: 1px solid #FA8200;
    border-right: 1px solid #ddd;
    background: #FFF3E0;
    padding: 16px 18px;
    border-radius: 2px;
  }}
  .toc-box h3 {{ font-size: 10pt; color: #FA8200; margin-bottom: 10px; }}
  .toc-box ol {{ padding-left: 20px; }}
  .toc-box li {{ font-size: 9.5pt; color: #333; padding: 2px 0; }}

  /* ── SECOES ──────────────────────────── */
  .section {{ page-break-before: always; }}
  .section-title {{
    font-size: 14pt;
    font-weight: bold;
    color: #1A1A1A;
    border-bottom: 3px solid #FA8200;
    padding-bottom: 6px;
    margin: 24px 0 14px;
  }}
  .section-title span {{ color: #FA8200; }}
  h3 {{ font-size: 11pt; color: #C86400; margin: 16px 0 6px; }}
  h4 {{ font-size: 10pt; color: #666; margin: 10px 0 4px; }}

  /* ── CORPO ───────────────────────────── */
  p  {{ margin-bottom: 7px; line-height: 1.5; }}
  ul {{ padding-left: 22px; margin-bottom: 8px; }}
  li {{ padding: 2px 0; font-size: 9.5pt; }}
  .spacer {{ margin-bottom: 16px; }}

  /* ── TABELAS ─────────────────────────── */
  table.data {{ width: 100%; border-collapse: collapse; margin: 10px 0 16px; font-size: 9pt; }}
  table.data th {{
    background: #FA8200; color: #fff;
    padding: 7px 10px; text-align: left;
    border: 1px solid #FA8200;
  }}
  table.data td {{
    padding: 6px 10px;
    border: 1px solid #ddd;
    vertical-align: top;
  }}
  table.data tr:nth-child(even) td {{ background: #F7F7F7; }}
  .ok  {{ color: #2D7A2D; font-weight: bold; }}
  .err {{ color: #B03030; font-weight: bold; }}
  .wrn {{ color: #B07000; font-weight: bold; }}

  /* ── CODIGO ──────────────────────────── */
  pre {{
    background: #FFF8F0;
    border-left: 5px solid #FA8200;
    border-top: 2px solid #FA8200;
    border-bottom: 2px solid #FA8200;
    border-right: none;
    padding: 10px 14px;
    font-family: "Courier New", monospace;
    font-size: 8.5pt;
    color: #C86400;
    margin: 8px 0 14px;
    white-space: pre;
    overflow-x: auto;
  }}

  /* ── CARDS DE AGENTE ─────────────────── */
  .agent-card {{
    border-left: 6px solid #FA8200;
    border-top: 2px solid #FA8200;
    border-bottom: 1px solid #FA8200;
    border-right: 1px solid #ddd;
    background: #FFF3E0;
    padding: 12px 16px;
    margin: 10px 0 10px;
    border-radius: 2px;
  }}
  .agent-card .agent-name {{
    font-size: 12pt;
    font-weight: bold;
    color: #1A1A1A;
  }}
  .agent-card .agent-name span {{ color: #FA8200; }}
  .agent-card .agent-meta {{ font-size: 9pt; color: #555; margin-top: 4px; }}
  .agent-card .agent-send-ok  {{ color: #2D7A2D; font-weight: bold; }}
  .agent-card .agent-send-no  {{ color: #B03030; font-weight: bold; }}

  /* ── BOXES ───────────────────────────── */
  .box {{
    border-left: 5px solid #FA8200;
    border-top: 2px solid #FA8200;
    border-bottom: 1px solid #FA8200;
    border-right: 1px solid #ddd;
    background: #FFF3E0;
    padding: 12px 16px;
    margin: 12px 0;
    border-radius: 2px;
  }}
  .box.blue   {{ border-color: #1A4A8A; background: #EEF2F9; }}
  .box.amber  {{ border-color: #B07000; background: #FFF8E0; }}
  .box.green  {{ border-color: #2D7A2D; background: #EEF9EE; }}
  .box h4 {{ margin: 0 0 6px; font-size: 9.5pt; }}
  .box p  {{ margin: 0; font-size: 9.5pt; }}

  /* ── CHECKLIST ───────────────────────── */
  .fase {{ font-weight: bold; font-size: 10.5pt; border-bottom: 2px solid #C86400; padding-bottom: 4px; margin: 18px 0 8px; }}
  .check {{ display: flex; align-items: flex-start; gap: 8px; margin: 4px 0; font-size: 9.5pt; }}
  .check .box-chk {{
    font-family: "Courier New", monospace;
    font-size: 9pt;
    color: #666;
    white-space: nowrap;
    margin-top: 1px;
  }}

  /* ── RODAPE FINAL ────────────────────── */
  .doc-footer {{
    border-top: 2px solid #FA8200;
    margin-top: 40px;
    padding-top: 10px;
  }}
  .doc-footer p {{ font-size: 8pt; color: #666; }}

  /* ── IMPRESSAO ───────────────────────── */
  @media print {{
    body {{ max-width: 100%; padding: 20px 28px; }}
    .section {{ page-break-before: always; }}
    .cover   {{ page-break-after: always; }}
    pre {{ page-break-inside: avoid; }}
    .agent-card, .box {{ page-break-inside: avoid; }}
    table.data {{ page-break-inside: avoid; }}

    @page {{
      size: A4;
      margin: 1.5cm 1.5cm;
      @top-right   {{ content: "SAMBA EXPORT CONTROL DESK  |  Agentes WhatsApp"; font-size: 7pt; color: #FA8200; }}
      @bottom-right {{ content: "Pag. " counter(page); font-size: 7pt; color: #999; }}
    }}
  }}
</style>
</head>
<body>

<!-- ═══════════════════════════════════════════════════════════ CAPA -->
<div class="cover">
  <div class="cover-bar">
    <h1>SAMBA EXPORT CONTROL DESK</h1>
    <p>Sistema de Agentes de Inteligencia Comercial</p>
  </div>

  <div class="cover-title">Agentes WhatsApp</div>
  <div class="cover-sub">Arquitetura, Interacao e Configuracao Completa</div>
  <div style="font-size:11pt; color:#666; margin-bottom:4px;">Numero operacional:</div>
  <div class="cover-phone">+55 13 99140-5566</div>

  <hr class="cover-hr">

  <table class="meta-table">
    <tr><td>Data</td><td>{TODAY}</td></tr>
    <tr><td>Versao</td><td>1.0 — Circulacao interna</td></tr>
    <tr><td>Classificacao</td><td>Confidencial — Socios e Desenvolvedores</td></tr>
    <tr><td>Chip WhatsApp</td><td>+5513991405566 (numero unico — todos os agentes)</td></tr>
    <tr><td>Status</td><td>Codigo implantado — aguardando credenciais Twilio</td></tr>
    <tr><td>Repositorio</td><td>SAMBA_MANAGER / SAMBA_AGENTS</td></tr>
  </table>

  <div class="toc-box">
    <h3>INDICE DO DOCUMENTO</h3>
    <ol>
      <li>Arquitetura geral do sistema</li>
      <li>Os 5 agentes — funcoes e interacoes</li>
      <li>Os 3 grupos WhatsApp internos e roteamento</li>
      <li>Configuracao Twilio — passo a passo completo</li>
      <li>Configuracao Meta Business Manager</li>
      <li>Infraestrutura — servidor e tunel HTTPS</li>
      <li>Checklist de ativacao — na ordem certa</li>
      <li>Estado atual do sistema</li>
    </ol>
  </div>
</div>

<!-- ═══════════════════════════════════════════════════════════ 1. ARQUITETURA -->
<div class="section">
  <div class="section-title"><span>1.</span> ARQUITETURA GERAL DO SISTEMA</div>

  <p>O numero +5513991405566 e membro de todos os grupos operacionais da Samba Export. Cada mensagem recebida segue o pipeline abaixo antes de qualquer resposta ser enviada:</p>

<pre>WhatsApp Business (grupos clientes + grupos internos Samba)
      |
      |  Twilio entrega via HTTPS POST  (&lt; 15s SLA)
      v
   /webhook/twilio  (FastAPI — responde em &lt; 100ms)
      |
      +-- identifica canal: externo | mailbox | drive | tasks
      |
      +----------+----------+----------+
      v          v          v          v
  Extractor  FollowUp   Router IA  Audio ATA
  (Celery)   (resposta) (@mention) (Gemini)
      |
      v
  SQLite DB --&gt; Google Sheets --&gt; Google Drive</pre>

  <table class="data">
    <tr><th>Componente</th><th>Funcao</th><th>Tecnologia</th></tr>
    <tr><td>FastAPI</td><td>Webhook Twilio — porta 8000</td><td>Python / uvicorn</td></tr>
    <tr><td>Celery</td><td>Workers assincronos para os agentes</td><td>Python / Redis</td></tr>
    <tr><td>Redis</td><td>Broker de filas Celery + scheduler Beat</td><td>Redis 7+</td></tr>
    <tr><td>SQLite</td><td>Banco principal: deals, follow-ups, conversas</td><td>SQLAlchemy ORM</td></tr>
    <tr><td>Gemini API</td><td>Inteligencia dos agentes (Flash + Pro)</td><td>Google Cloud — billing ativo</td></tr>
    <tr><td>Sheets</td><td>Pipeline comercial e planilha de deals</td><td>Google Workspace</td></tr>
    <tr><td>Drive</td><td>Documentos RAG e arquivos corporativos</td><td>Google Workspace</td></tr>
    <tr><td>Twilio</td><td>API WhatsApp Business — envio e recepcao</td><td>Twilio Messaging API</td></tr>
  </table>
</div>

<!-- ═══════════════════════════════════════════════════════════ 2. OS 5 AGENTES -->
<div class="section">
  <div class="section-title"><span>2.</span> OS 5 AGENTES — FUNCOES E INTERACOES</div>

  <!-- Agente 1 -->
  <div class="agent-card">
    <div class="agent-name"><span>Agente 1:</span> Extractor &nbsp;&nbsp;<small style="font-weight:normal;color:#666;">+5513991405566</small></div>
    <div class="agent-meta">Role: EXTRACTOR &nbsp;&nbsp; Envio: <span class="agent-send-no">SOMENTE LEITURA — nunca envia</span></div>
  </div>
  <p>Minerador de dados comerciais. Opera em modo somente leitura — NUNCA envia mensagens. Captura qualquer mensagem e extrai:</p>
  <ul>
    <li>Commodity, volume (MT/sacas), preco (USD/MT), incoterm, origem, destino, parceiro</li>
    <li>Cria ou atualiza Deal no banco com stage "Lead Capturado"</li>
    <li>Sincroniza automaticamente com a planilha Google Sheets "todos andamento"</li>
    <li>Se campos criticos faltam: dispara alerta por email E WhatsApp ao socio responsavel</li>
  </ul>
<pre>Mensagem no grupo
  --&gt; persist_inbound_message()   --&gt; Message.id no banco
  --&gt; task_process_inbound_message(msg_id)  [Celery queue]
  --&gt; ExtractorAgent.process_single_message()
  --&gt; Deal criado/atualizado no SQLite
  --&gt; task_sync_spreadsheet_to_drive()  --&gt; Sheets atualizado</pre>

  <div class="spacer"></div>

  <!-- Agente 2 -->
  <div class="agent-card">
    <div class="agent-name"><span>Agente 2:</span> Follow-Up &nbsp;&nbsp;<small style="font-weight:normal;color:#666;">+5513991405566</small></div>
    <div class="agent-meta">Role: FOLLOWUP &nbsp;&nbsp; Envio: <span class="agent-send-ok">PODE ENVIAR MENSAGENS</span></div>
  </div>
  <p>O cobrador inteligente. Ciclo automatico a cada 15 minutos via Celery Beat. Cadencia de 3 tentativas com tom progressivamente mais firme:</p>

  <table class="data">
    <tr><th>Tentativa</th><th>Dias Vencido</th><th>Tom</th><th>Acao</th></tr>
    <tr><td>1a</td><td>0 a 2 dias</td><td>Casual — so checando, sem pressao</td><td>Envia direto via WhatsApp</td></tr>
    <tr><td>2a</td><td>3 a 6 dias</td><td>Firme — cita commodity e volume</td><td>Envia direto via WhatsApp</td></tr>
    <tr><td>3a</td><td>7+ dias</td><td>Critico — janela de preco fecha hoje</td><td class="wrn">PendingApproval — aguarda aprovacao humana</td></tr>
  </table>

  <p>Com <code>WHATSAPP_OFFLINE=true</code> (modo atual): envia email ao socio com a mensagem pronta para copiar/colar.<br>
     Com <code>WHATSAPP_OFFLINE=false</code> (producao): envia via Twilio diretamente ao parceiro.</p>

<pre>Parceiro responde pelo WhatsApp
  --&gt; webhook: match_followup_response(sender)
  --&gt; FollowUp.response_received = True  (banco)
  --&gt; Deal avanca para "Em Negociacao"
  --&gt; Alerta enviado ao grupo SAMBA AGENTS TASKS FUP</pre>

  <div class="spacer"></div>

  <!-- Agente 3 -->
  <div class="agent-card">
    <div class="agent-name"><span>Agente 3:</span> Manager &nbsp;&nbsp;<small style="font-weight:normal;color:#666;">+5513991405566</small></div>
    <div class="agent-meta">Role: MANAGER &nbsp;&nbsp; Envio: <span class="agent-send-ok">PODE ENVIAR MENSAGENS</span></div>
  </div>
  <p>O cerebro estrategico. Roda diariamente e envia briefing executivo para todos os socios:</p>
  <ul>
    <li>Classifica cada deal como COMPRA ou VENDA por heuristica de keywords + Gemini</li>
    <li>Cruza vendedores x compradores da mesma commodity — detecta oportunidades de arbitragem</li>
    <li>Persiste matches nas notas dos deals com tag [MATCH DD/MM/YYYY] — sem perdas entre ciclos</li>
    <li>Gera briefing executivo via Gemini Pro (pipeline + matches + alertas de risco)</li>
    <li>Envia o briefing por WhatsApp e email a todos os socios</li>
  </ul>
<pre>Deal #12 (vendedor soja) — campo notes apos match:

[MATCH 12/05/2026] SOJA spread USD 12,50/MT |
Venda 447,00 x Compra 459,50 | Contraparte: BRAKO Korea (VENDEDOR)</pre>

  <div class="spacer"></div>

  <!-- Agente 4 -->
  <div class="agent-card">
    <div class="agent-name"><span>Agente 4:</span> Intelligence Router (@mention) &nbsp;&nbsp;<small style="font-weight:normal;color:#666;">+5513991405566</small></div>
    <div class="agent-meta">Cascade 5 niveis &nbsp;&nbsp; Envio: <span class="agent-send-ok">PODE ENVIAR MENSAGENS</span></div>
  </div>
  <p>Ativado por <code>@samba</code>, <code>@agente</code>, <code>@ia</code> ou <code>@bot</code> em qualquer grupo. Responde no proprio grupo. Tem memoria das ultimas 4 trocas por usuario (ConversationHistory).</p>

  <table class="data">
    <tr><th>Nivel</th><th>Nome</th><th>Funcao</th><th>Custo API</th></tr>
    <tr><td>L0</td><td>Intent Parser</td><td>Gemini Flash classifica a intencao da pergunta</td><td>Minimo</td></tr>
    <tr><td>L1</td><td>DB Direct</td><td>SQL direto: deals, follow-ups, precos, atas</td><td>Zero — local</td></tr>
    <tr><td>L2</td><td>RAG Search</td><td>Busca vetorial nos documentos do Google Drive</td><td>Embedding API</td></tr>
    <tr><td>L3</td><td>Gemini Flash</td><td>Raciocinio com contexto do banco + historico</td><td>Baixo</td></tr>
    <tr><td>L4</td><td>Gemini Pro</td><td>Raciocinio profundo se Flash &lt; 75% de confianca</td><td>Medio</td></tr>
    <tr><td>L5</td><td>Honest Fallback</td><td>Nao encontrei — NUNCA inventa dados numericos</td><td>Zero</td></tr>
  </table>

  <div class="box blue">
    <h4 style="color:#1A4A8A;">REGRA FUNDAMENTAL — Facts Only</h4>
    <p>O router NUNCA inventa precos, datas, nomes, volumes ou qualquer dado numerico. Toda resposta e baseada exclusivamente no contexto recuperado do banco de dados ou dos documentos corporativos. Se a informacao nao existe no sistema, o agente responde honestamente que nao encontrou — nunca "chuta".</p>
  </div>

  <div class="spacer"></div>

  <!-- Agente 5 -->
  <div class="agent-card">
    <div class="agent-name"><span>Agente 5:</span> Documental e Enriquecimento &nbsp;&nbsp;<small style="font-weight:normal;color:#666;">+5513991405566</small></div>
    <div class="agent-meta">Role: DOCUMENTAL &nbsp;&nbsp; Envio: <span class="agent-send-ok">PODE ENVIAR MENSAGENS</span></div>
  </div>
  <ul>
    <li>Preenche celulas em branco na planilha "todos andamento" com dados das conversas</li>
    <li>Base de conhecimento dinamica: 21 JOBs estaticos + deals novos do banco automaticamente</li>
    <li>Regra absoluta: NUNCA sobrescreve celula com conteudo existente</li>
    <li>Audita documentos (LOI, ICPO, FCO, SPA) contra padrao ICC/UCP600</li>
  </ul>
</div>

<!-- ═══════════════════════════════════════════════════════════ 3. GRUPOS -->
<div class="section">
  <div class="section-title"><span>3.</span> OS 3 GRUPOS WHATSAPP INTERNOS</div>

  <p>O numero +5513991405566 e membro dos 3 grupos operacionais internos. O webhook identifica o grupo de origem e roteia cada mensagem de forma apropriada, com protecoes especificas para cada canal:</p>

  <table class="data">
    <tr><th>Grupo WhatsApp</th><th>Funcao Principal</th><th>Alertas recebidos</th></tr>
    <tr><td><strong>SAMBA AGENTS MAIL BOX</strong></td><td>Inbox / email / documentos</td><td>Documentos externos, alertas de email, notificacoes de inbox</td></tr>
    <tr><td><strong>SAMBA AGENTS GOOGLE DRIVE</strong></td><td>Drive e RAG</td><td>Novos arquivos indexados, atualizacoes da base de conhecimento</td></tr>
    <tr><td><strong>SAMBA AGENTS TASKS FUP</strong></td><td>Follow-ups e pipeline</td><td>Deals incompletos, follow-ups vencidos, respostas de parceiros, escalacoes</td></tr>
  </table>

  <h4>Roteamento automatico no webhook:</h4>
<pre>GroupName = 'SAMBA AGENTS MAIL BOX'      --&gt;  canal: mailbox
GroupName = 'SAMBA AGENTS GOOGLE DRIVE'  --&gt;  canal: drive
GroupName = 'SAMBA AGENTS TASKS FUP'     --&gt;  canal: tasks
qualquer outro grupo                      --&gt;  canal: external (grupo de cliente)</pre>

  <h4>Protecoes ativas para grupos internos:</h4>
  <ul>
    <li>Mensagens internas NAO disparam follow-up response matching (evita falsos positivos)</li>
    <li>Grupos internos NAO acionam mensagem de boas-vindas de novo grupo Samba x cliente</li>
    <li>O Extractor NAO cria deals a partir de mensagens dos grupos internos</li>
    <li>@mention em grupo interno responde no proprio grupo com contexto corporativo</li>
  </ul>
</div>

<!-- ═══════════════════════════════════════════════════════════ 4. TWILIO -->
<div class="section">
  <div class="section-title"><span>4.</span> CONFIGURACAO TWILIO — PASSO A PASSO</div>

  <h3>4.1 Obter credenciais Twilio</h3>
  <p>Acesse: <strong>https://console.twilio.com</strong></p>
<pre>Dashboard  --&gt;  Account Info
  Account SID  --&gt;  copiar  (comeca com AC...)
  Auth Token   --&gt;  clicar no olho para revelar e copiar</pre>
  <p>Inserir no arquivo <code>.env</code> na raiz do projeto:</p>
<pre>TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWILIO_AUTH_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx</pre>

  <h3>4.2 Habilitar WhatsApp no numero +5513991405566</h3>
<pre>Console  --&gt;  Messaging  --&gt;  Senders  --&gt;  WhatsApp Senders  --&gt;  Add Sender
  Tipo:    Business Profile  (NAO usar Sandbox — isso e producao)
  Numero:  +5513991405566</pre>
  <div class="box amber">
    <h4 style="color:#B07000;">ATENCAO — Conta WhatsApp Business obrigatoria</h4>
    <p>Se o numero +5513991405566 ainda nao tiver uma conta WhatsApp Business ativa, sera necessario registrar no Meta Business Manager primeiro (ver Secao 5). O Twilio usa a API oficial da Meta — nao funciona com contas pessoais.</p>
  </div>

  <h3>4.3 Registrar a URL do webhook</h3>
<pre>Console  --&gt;  Messaging  --&gt;  Senders  --&gt;  WhatsApp Senders
  --&gt;  selecione o numero +5513991405566
  --&gt;  campo 'A MESSAGE COMES IN':
         Webhook URL:  https://SEU-DOMINIO.COM.BR/webhook/twilio
         Metodo HTTP:  POST
  --&gt;  Salvar

# Inserir tambem no .env:
TWILIO_WEBHOOK_PUBLIC_URL=https://SEU-DOMINIO.COM.BR/webhook/twilio
TWILIO_VALIDATE_SIGNATURE=true</pre>

  <h3>4.4 Descobrir os IDs dos 3 grupos internos</h3>
  <p>Apos o webhook estar no ar, peca para qualquer membro de cada grupo enviar uma mensagem. Voce vera no log do servidor:</p>
<pre>webhook_twilio: group='SAMBA AGENTS TASKS FUP' channel=tasks
                From=whatsapp:+5513XXXXXXX-XXXXXXXXXX@g.us
                                |
                                +-- este e o ID do grupo (copiar sem 'whatsapp:')</pre>
  <p>Preencher no <code>.env</code>:</p>
<pre>WPP_GROUP_MAILBOX_ID=+XXXXXXXXX-XXXXXXXXXX@g.us     # SAMBA AGENTS MAIL BOX
WPP_GROUP_DRIVE_ID=+XXXXXXXXX-XXXXXXXXXX@g.us       # SAMBA AGENTS GOOGLE DRIVE
WPP_GROUP_TASKS_FUP_ID=+XXXXXXXXX-XXXXXXXXXX@g.us   # SAMBA AGENTS TASKS FUP</pre>

  <h3>4.5 Ativar envios reais e testar</h3>
<pre># 1. Mudar no .env:
WHATSAPP_OFFLINE=false

# 2. Diagnostico completo (nao envia nada):
python scripts/wpp_smoke_test.py

# 3. Teste de loopback — envia mensagem real para o proprio numero:
python scripts/wpp_smoke_test.py --send

# 4. Envia para numero especifico (ex: Leonardo):
python scripts/wpp_smoke_test.py --send --to +5513996259995</pre>

  <h4>Saida esperada do smoke test quando tudo OK:</h4>
<pre>[OK]  SAMBA_WPP_MAIN configurado  --&gt;  +5513991405566
[OK]  TWILIO_ACCOUNT_SID          --&gt;  ACxxxx***
[OK]  TWILIO_AUTH_TOKEN           --&gt;  ***xxxx
[!!]  WHATSAPP_OFFLINE            --&gt;  INATIVO (envios reais habilitados)
[OK]  twilio SDK instalado        --&gt;  v9.x.x
[OK]  Conta Twilio acessivel      --&gt;  Samba Export [active]
[OK]  Extractor    (AGENT_EXTRACTOR_PHONE)   --&gt;  +5513991405566
[OK]  Follow-Up    (AGENT_FOLLOWUP_PHONE)    --&gt;  +5513991405566
[OK]  Manager      (AGENT_MANAGER_PHONE)     --&gt;  +5513991405566
[OK]  Documental   (AGENT_DOCUMENTAL_PHONE)  --&gt;  +5513991405566
[OK]  Agenda       (AGENT_AGENDA_PHONE)      --&gt;  +5513991405566</pre>
</div>

<!-- ═══════════════════════════════════════════════════════════ 5. META -->
<div class="section">
  <div class="section-title"><span>5.</span> CONFIGURACAO META BUSINESS MANAGER</div>

  <div class="box blue">
    <h4 style="color:#1A4A8A;">Quando e necessario</h4>
    <p>Se o numero +5513991405566 ja estiver registrado como WhatsApp Business, a verificacao formal pode nao ser exigida para o Twilio Sandbox. Para producao com API oficial, a verificacao de negocio e obrigatoria.</p>
  </div>

  <h3>5.1 Verificar o negocio na Meta</h3>
  <ul>
    <li>Acesse: <strong>business.facebook.com</strong> → Configuracoes do Negocio → Verificacao do Negocio</li>
    <li>Documentos: CNPJ da Samba Export + comprovante de endereco comercial</li>
    <li>Prazo de aprovacao: 24 a 72 horas</li>
  </ul>

  <h3>5.2 Criar conta WhatsApp Business</h3>
<pre>Configuracoes do Negocio  --&gt;  Contas WhatsApp  --&gt;  Adicionar
  Nome de exibicao:  Samba Export
  Categoria:         Financas e Servicos Financeiros
  Descricao:         Trading de commodities agricolas</pre>

  <h3>5.3 Conectar o numero ao Twilio</h3>
  <ul>
    <li>Twilio Console → Messaging → Senders → WhatsApp Senders → Request Access</li>
    <li>Escolher: "Use your own number"</li>
    <li>O Twilio fornece um codigo de verificacao SMS — inserir no WhatsApp do +5513991405566</li>
    <li>Aguardar aprovacao da Meta (de minutos a 24h)</li>
  </ul>

  <h3>5.4 Templates de mensagens (apenas para 1o contato proativo)</h3>
  <p>Para mensagens onde a Samba toma a iniciativa com alguem que nunca escreveu antes, a Meta exige templates pre-aprovados. Para conversas em andamento (parceiro ja escreveu nas ultimas 24h), templates NAO sao necessarios.</p>
<pre>Meta Business Manager  --&gt;  Gerenciador do WhatsApp  --&gt;  Modelos de Mensagem
  --&gt;  Criar modelo
  Categoria:  Utility  (transacional — nao "Marketing")
  Nome:       samba_followup_v1
  Corpo:      "Ola {{{{1}}}}, estamos acompanhando a proposta de {{{{2}}}}.
               Poderia nos dar um retorno?"
  --&gt;  Enviar para aprovacao  (prazo: 24h)</pre>
</div>

<!-- ═══════════════════════════════════════════════════════════ 6. INFRA -->
<div class="section">
  <div class="section-title"><span>6.</span> INFRAESTRUTURA — SERVIDOR E TUNEL HTTPS</div>

  <h3>6.1 Desenvolvimento local — ngrok</h3>
<pre># Instalar: https://ngrok.com/download
ngrok http 8000

# URL gerada (ex): https://xxxx-xx-xx.ngrok.io
# Inserir no .env:
TWILIO_WEBHOOK_PUBLIC_URL=https://xxxx-xx-xx.ngrok.io/webhook/twilio</pre>
  <div class="box amber">
    <h4 style="color:#B07000;">Limitacao ngrok gratuito</h4>
    <p>A URL muda a cada reinicio no plano gratuito. Para uso continuo, usar ngrok pago (URL fixa) ou VPS com dominio proprio e Let's Encrypt.</p>
  </div>

  <h3>6.2 Producao — VPS recomendado</h3>
<pre>Sistema:  Ubuntu 22.04 LTS
Python:   3.11+
Redis:    7+
Nginx:    proxy reverso + HTTPS via Let's Encrypt (gratuito)
Processo: systemd ou Supervisor

# 3 servicos rodando simultaneamente:

# API webhook (FastAPI)
uvicorn api.webhook:app --host 0.0.0.0 --port 8000 --workers 2

# Worker Celery — processa as tasks dos agentes
celery -A core.celery_app worker -Q queue_extractor,queue_sync --loglevel=info

# Beat Celery — agendamentos automaticos (follow-ups 15min, briefing diario)
celery -A core.celery_app beat --loglevel=info</pre>

  <h3>6.3 Nginx — configuracao minima</h3>
<pre>server {{
    listen 443 ssl;
    server_name  seu-dominio.com.br;
    ssl_certificate      /etc/letsencrypt/live/seu-dominio/fullchain.pem;
    ssl_certificate_key  /etc/letsencrypt/live/seu-dominio/privkey.pem;

    location /webhook/twilio {{
        proxy_pass          http://127.0.0.1:8000;
        proxy_set_header    Host $host;
        proxy_set_header    X-Real-IP $remote_addr;
        proxy_read_timeout  30s;
    }}
    location /health {{
        proxy_pass  http://127.0.0.1:8000;
    }}
}}</pre>
</div>

<!-- ═══════════════════════════════════════════════════════════ 7. CHECKLIST -->
<div class="section">
  <div class="section-title"><span>7.</span> CHECKLIST DE ATIVACAO — NA ORDEM CERTA</div>

  <div class="box">
    <h4 style="color:#FA8200;">Siga exatamente esta ordem</h4>
    <p>Cada fase depende da anterior. Pular etapas causa erros de validacao de assinatura Twilio ou falhas de autenticacao dificeis de diagnosticar.</p>
  </div>

  <div class="fase">FASE 1 — Contas e aprovacoes</div>
  <div class="check"><span class="box-chk">[ ]</span><span>Criar conta Twilio em twilio.com/try-twilio (gratuito para comecar)</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Verificar negocio no Meta Business Manager com CNPJ da Samba Export</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Criar perfil WhatsApp Business para Samba Export no Meta</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Conectar numero +5513991405566 como WhatsApp Business no Twilio</span></div>

  <div class="fase">FASE 2 — Servidor e conectividade</div>
  <div class="check"><span class="box-chk">[ ]</span><span>Subir VPS (recomendado) ou abrir tunel ngrok local na porta 8000</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Configurar HTTPS — Let's Encrypt no VPS ou URL ngrok</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Testar acesso: curl https://SEU-DOMINIO/health → resposta "ok"</span></div>

  <div class="fase">FASE 3 — Configuracao Twilio</div>
  <div class="check"><span class="box-chk">[ ]</span><span>Copiar TWILIO_ACCOUNT_SID e TWILIO_AUTH_TOKEN para o .env</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Configurar webhook URL no Twilio Console para o numero +5513991405566</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Definir TWILIO_WEBHOOK_PUBLIC_URL no .env</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Rodar diagnostico: python scripts/wpp_smoke_test.py → todos OK</span></div>

  <div class="fase">FASE 4 — Grupos internos</div>
  <div class="check"><span class="box-chk">[ ]</span><span>Pedir mensagem no grupo SAMBA AGENTS MAIL BOX → copiar Group ID do log</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Pedir mensagem no grupo SAMBA AGENTS GOOGLE DRIVE → copiar Group ID</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Pedir mensagem no grupo SAMBA AGENTS TASKS FUP → copiar Group ID</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Preencher WPP_GROUP_*_ID no .env com os 3 IDs copiados</span></div>

  <div class="fase">FASE 5 — Ativacao e testes</div>
  <div class="check"><span class="box-chk">[ ]</span><span>Mudar WHATSAPP_OFFLINE=false no .env</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Reiniciar workers Celery e uvicorn</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Teste loopback: python scripts/wpp_smoke_test.py --send → mensagem chega</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Testar @mention: "@samba qual o status dos deals de hoje?" → resposta &lt; 5s</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Validar recebimento no grupo SAMBA AGENTS TASKS FUP</span></div>
  <div class="check"><span class="box-chk">[ ]</span><span>Criar template de follow-up proativo no Meta (opcional — so para 1o contato)</span></div>
</div>

<!-- ═══════════════════════════════════════════════════════════ 8. ESTADO ATUAL -->
<div class="section">
  <div class="section-title"><span>8.</span> ESTADO ATUAL DO SISTEMA</div>

  <p>Situacao em {TODAY} — referencia para socios e equipe de desenvolvimento:</p>

  <table class="data">
    <tr><th>Componente</th><th>Status</th><th>Acao Necessaria</th></tr>
    <tr><td>Numero WhatsApp +5513991405566</td><td class="ok">Ativo</td><td>Nenhuma</td></tr>
    <tr><td>Codigo dos 5 agentes</td><td class="ok">Ativo</td><td>Nenhuma</td></tr>
    <tr><td>Memoria de conversa</td><td class="ok">Ativo</td><td>Nenhuma</td></tr>
    <tr><td>Cadencia 3 tentativas + HITL</td><td class="ok">Ativo</td><td>Nenhuma</td></tr>
    <tr><td>Persistencia de matches</td><td class="ok">Ativo</td><td>Nenhuma</td></tr>
    <tr><td>KB dinamica Enrichment Agent</td><td class="ok">Ativo</td><td>Nenhuma</td></tr>
    <tr><td>Roteamento por grupo (3 grupos)</td><td class="ok">Ativo</td><td>Nenhuma</td></tr>
    <tr><td>TWILIO_ACCOUNT_SID</td><td class="err">Preencher no .env</td><td>Copiar do Console Twilio</td></tr>
    <tr><td>TWILIO_AUTH_TOKEN</td><td class="err">Preencher no .env</td><td>Copiar do Console Twilio</td></tr>
    <tr><td>Webhook URL publica</td><td class="err">Configurar servidor ou ngrok</td><td>VPS + nginx ou ngrok</td></tr>
    <tr><td>WHATSAPP_OFFLINE</td><td class="wrn">Mudar para false</td><td>Apos credenciais e webhook OK</td></tr>
    <tr><td>IDs dos 3 grupos internos</td><td class="wrn">Aguarda 1a mensagem de cada</td><td>Automatico apos webhook ativo</td></tr>
    <tr><td>Meta Business — verificacao</td><td class="wrn">Verificar status atual</td><td>business.facebook.com</td></tr>
  </table>

  <div class="box green">
    <h4 style="color:#2D7A2D;">PROXIMO PASSO IMEDIATO — 15 minutos para comecar</h4>
    <p>
      1. Criar conta Twilio gratuita em twilio.com/try-twilio<br>
      2. Copiar Account SID e Auth Token para o .env<br>
      3. Abrir tunel: <code>ngrok http 8000</code><br>
      4. Registrar a URL ngrok no Console Twilio como webhook do numero +5513991405566<br>
      Com isso o webhook ja comeca a receber mensagens e voce mapeia os IDs dos grupos internos.
    </p>
  </div>

  <div class="doc-footer">
    <p><strong style="color:#FA8200;">Samba Export Control Desk — Sistema de Agentes de Inteligencia Comercial</strong></p>
    <p>Gerado em {TODAY} &nbsp;|&nbsp; Confidencial — circulacao restrita a socios e desenvolvedores autorizados</p>
  </div>
</div>

</body>
</html>
"""

OUT.write_text(HTML, encoding="utf-8")
print(f"OK  {{OUT}}")
print(f"    {{OUT.stat().st_size // 1024}} KB")
