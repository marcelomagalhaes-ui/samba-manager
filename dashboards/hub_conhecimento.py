"""
Hub da Equipe — Samba Export
Módulo de conhecimento corporativo: scripts, FAQs, mapa de leads, glossário.
Espelha o index.html do ambiente Vercel (Teste-samba/index.html).
"""

import streamlit as st
from pathlib import Path

ROOT = Path(__file__).parent.parent


# ─── CSS Base ────────────────────────────────────────────────────────────────
_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800;900&display=swap');

/* ── reset / base ── */
.hub-wrap * { box-sizing: border-box; }
.hub-wrap {
    font-family: 'Montserrat', sans-serif;
    background: #f5f5f5;
    color: #2d2d2d;
    font-size: 13px;
    line-height: 1.5;
    padding-bottom: 40px;
}

/* ── page header ── */
.hub-page-header {
    background: #fff;
    border-bottom: 4px solid #FA8200;
    padding: 18px 0 14px;
    margin-bottom: 20px;
    display: flex;
    align-items: center;
    justify-content: space-between;
}
.hub-logo { font-size: 22px; font-weight: 900; color: #1a1a1a; letter-spacing: -0.5px; }
.hub-logo span { color: #FA8200; }
.hub-tag {
    background: #FFF3E0; color: #FA8200;
    font-size: 10px; font-weight: 700;
    padding: 4px 10px; border-radius: 20px; letter-spacing: 1px;
    margin-left: 10px;
}
.hub-back-btn {
    background: transparent; border: 2px solid #FA8200; color: #FA8200;
    border-radius: 8px; padding: 6px 14px; font-size: 11px; font-weight: 700;
    cursor: pointer; font-family: 'Montserrat', sans-serif; transition: all 0.2s;
}
.hub-back-btn:hover { background: #FA8200; color: #fff; }

/* ── page title ── */
.hub-page-title { font-size: 22px; font-weight: 900; color: #1a1a1a; margin-bottom: 4px; }
.hub-page-title span { color: #FA8200; }
.hub-page-sub {
    color: #666; font-size: 12px; margin-bottom: 24px;
    padding-bottom: 16px; border-bottom: 1px solid #e0e0e0;
}

/* ── cards ── */
.hub-card {
    background: #fff; border: 1px solid #e0e0e0; border-radius: 12px;
    padding: 18px 20px; margin-bottom: 14px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.04);
}
.hub-card-orange { border-left: 4px solid #FA8200; background: #FFF3E0; }
.hub-card-green  { border-left: 4px solid #2e7d32; background: #E8F5E9; }
.hub-card-red    { border-left: 4px solid #c62828; background: #FFEBEE; }
.hub-card-blue   { border-left: 4px solid #1565C0; background: #E3F2FD; }
.hub-card-yellow { border-left: 4px solid #F9A825; background: #FFFDE7; }

.hub-card-title { font-weight: 800; font-size: 14px; margin-bottom: 10px; color: #1a1a1a; }
.hub-card-title.orange { color: #FA8200; }
.hub-card-title.green  { color: #2e7d32; }
.hub-card-title.red    { color: #c62828; }
.hub-card-title.blue   { color: #1565C0; }

/* ── badges ── */
.hub-badge {
    display: inline-block; border-radius: 6px;
    padding: 3px 10px; font-size: 10px; font-weight: 800; letter-spacing: 0.5px;
}
.hub-badge-orange { background: #FA8200; color: #fff; }
.hub-badge-green  { background: #2e7d32; color: #fff; }
.hub-badge-red    { background: #c62828; color: #fff; }
.hub-badge-blue   { background: #1565C0; color: #fff; }
.hub-badge-gray   { background: #e0e0e0; color: #666; }

/* ── alert box ── */
.hub-alert {
    background: #FA8200; color: #fff; border-radius: 12px;
    padding: 18px 22px; margin-bottom: 20px;
}
.hub-alert strong { font-size: 14px; }

/* ── tip / warn / info / note ── */
.hub-tip  { background:#E8F5E9; border:1px solid #A5D6A7; border-radius:8px; padding:10px 14px; font-size:12px; margin-top:10px; color:#2e7d32; }
.hub-warn { background:#FFEBEE; border:1px solid #EF9A9A; border-radius:8px; padding:10px 14px; font-size:12px; margin-top:10px; color:#c62828; }
.hub-info { background:#E3F2FD; border:1px solid #90CAF9; border-radius:8px; padding:10px 14px; font-size:12px; margin-top:10px; color:#1565C0; }
.hub-note { background:#FFFDE7; border:1px solid #FFE082; border-radius:8px; padding:10px 14px; font-size:12px; margin-top:10px; color:#7B5800; }

/* ── script block ── */
.hub-script-en {
    background: #E8F5E9; border: 1px solid #A5D6A7; border-radius: 8px;
    padding: 14px; font-family: monospace; font-size: 12px;
    line-height: 1.8; color: #2d2d2d; white-space: pre-wrap;
}
.hub-script-pt {
    background: #E3F2FD; border: 1px solid #90CAF9; border-radius: 8px;
    padding: 14px; font-family: monospace; font-size: 12px;
    line-height: 1.8; color: #2d2d2d; white-space: pre-wrap;
}
.hub-lang-tag-en { display:inline-block; background:#2e7d32; color:#fff; border-radius:4px; padding:3px 10px; font-size:10px; font-weight:800; margin-bottom:8px; letter-spacing:1px; }
.hub-lang-tag-pt { display:inline-block; background:#1565C0; color:#fff; border-radius:4px; padding:3px 10px; font-size:10px; font-weight:800; margin-bottom:8px; letter-spacing:1px; }

/* ── funnel ── */
.hub-funnel-step {
    display: flex; align-items: stretch; margin-bottom: 3px;
}
.hub-funnel-num {
    min-width: 44px; display: flex; align-items: center; justify-content: center;
    font-weight: 900; font-size: 14px; color: #fff;
}
.hub-funnel-body {
    flex: 1; padding: 12px 18px;
    background: #fff; border: 1px solid #e0e0e0;
    display: flex; justify-content: space-between; align-items: center;
}

/* ── flow items ── */
.hub-flow-item {
    display: flex; gap: 12px; align-items: flex-start;
    padding: 10px 14px; background: #fff; border: 1px solid #e0e0e0;
    border-radius: 8px; margin-bottom: 6px;
}
.hub-flow-dot {
    width: 28px; height: 28px; border-radius: 50%;
    display: flex; align-items: center; justify-content: center;
    font-weight: 800; font-size: 12px; flex-shrink: 0;
    background: #FA8200; color: #fff;
}

/* ── rule item ── */
.hub-rule-item {
    display: flex; gap: 14px; padding: 12px 0;
    border-bottom: 1px solid #e0e0e0; align-items: flex-start;
}
.hub-rule-num {
    background: #FA8200; color: #fff; border-radius: 50%;
    width: 30px; height: 30px; display: flex; align-items: center;
    justify-content: center; font-weight: 800; font-size: 12px; flex-shrink: 0;
}

/* ── pills ── */
.hub-pill {
    display: inline-block; border-radius: 20px; padding: 4px 12px;
    font-size: 10px; font-weight: 700; margin: 3px; border: 1px solid;
}

/* ── glossary ── */
.hub-glossary-item { display:flex; gap:16px; padding:12px 0; border-bottom:1px solid #e0e0e0; }
.hub-glossary-term { font-weight:800; color:#FA8200; min-width:130px; font-size:12px; }
.hub-glossary-def  { font-size:12px; color:#2d2d2d; line-height:1.6; }

/* ── profile card ── */
.hub-profile-card {
    background: #fff; border: 2px solid #e0e0e0; border-radius: 12px;
    padding: 18px; text-align: center;
}
.hub-profile-avatar {
    width: 60px; height: 60px; border-radius: 50%; background: #FA8200;
    color: #fff; display: flex; align-items: center; justify-content: center;
    font-size: 22px; font-weight: 900; margin: 0 auto 10px;
}
.hub-profile-name   { font-weight: 800; font-size: 14px; color: #1a1a1a; margin-bottom: 4px; }
.hub-profile-role   { font-size: 11px; color: #FA8200; font-weight: 700; margin-bottom: 6px; }
.hub-profile-desc   { font-size: 11px; color: #666; line-height: 1.6; margin-bottom: 10px; }
.hub-profile-email  { background: #f5f5f5; border-radius: 6px; padding: 6px 10px; font-size: 11px; font-family: monospace; color: #FA8200; font-weight: 700; }

/* ── mini table ── */
.hub-table { width:100%; border-collapse:collapse; font-size:12px; }
.hub-table th { background:#FA8200; color:#fff; padding:8px 12px; text-align:left; font-weight:700; }
.hub-table td { padding:8px 12px; border-bottom:1px solid #e0e0e0; }
.hub-table tr:nth-child(even) td { background:#f5f5f5; }

/* ── section divider ── */
.hub-cat {
    font-size: 10px; font-weight: 800; letter-spacing: 1px; color: #FA8200;
    margin: 22px 0 10px; display: flex; align-items: center; gap: 8px;
}
.hub-cat::after { content: ''; flex: 1; height: 1px; background: #e0e0e0; }

/* ── filter question ── */
.hub-fq {
    background: #fff; border: 1px solid #e0e0e0; border-left: 4px solid #FA8200;
    border-radius: 8px; padding: 12px 16px; margin-bottom: 10px;
}

/* ── link button ── */
.hub-btn-orange {
    display: inline-block; background: #FA8200; color: #fff;
    border-radius: 8px; padding: 7px 16px; font-size: 11px; font-weight: 800;
    text-decoration: none; border: none; font-family: 'Montserrat', sans-serif; cursor: pointer;
}
.hub-btn-outline {
    display: inline-block; background: transparent; border: 2px solid #FA8200; color: #FA8200;
    border-radius: 8px; padding: 7px 16px; font-size: 11px; font-weight: 800;
    text-decoration: none; font-family: 'Montserrat', sans-serif; cursor: pointer;
}
</style>
"""


# ═══════════════════════════════════════════════════════════════════════════════
# TAB HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _card(content: str, color: str = "") -> str:
    cls = f"hub-card hub-card-{color}" if color else "hub-card"
    return f'<div class="{cls}">{content}</div>'

def _badge(text: str, color: str = "orange") -> str:
    return f'<span class="hub-badge hub-badge-{color}">{text}</span>'

def _tip(text: str)  -> str: return f'<div class="hub-tip">{text}</div>'
def _warn(text: str) -> str: return f'<div class="hub-warn">{text}</div>'
def _info(text: str) -> str: return f'<div class="hub-info">{text}</div>'
def _note(text: str) -> str: return f'<div class="hub-note">{text}</div>'


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1 — INÍCIO
# ═══════════════════════════════════════════════════════════════════════════════

def _tab_inicio():
    st.markdown("""
<div class="hub-alert">
<strong>📢 Mensagem dos sócios para a equipe</strong><br><br>
Vocês vão operar os Linkedins de Marcelo, Nívio e Leonardo. Vão ler mensagens, responder, qualificar leads e passar pra frente.<br><br>
<strong>Não precisam saber tudo sobre commodities. Precisam saber seguir o processo.</strong><br>
Este hub foi feito para isso. Qualquer dúvida: verifique aqui primeiro. Depois pergunte.
</div>
""", unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown(_card("""
<div class="hub-card-title orange">🎯 Seu único objetivo</div>
<p style="font-size:12px;line-height:1.8;color:#5d3a00;">Identificar se a pessoa é um <strong>comprador</strong> ou um <strong>potencial Finder</strong>, responder com o script correto e migrar para o WhatsApp. Só isso. O resto os sócios fazem.</p>
""", "orange"), unsafe_allow_html=True)

    with c2:
        st.markdown(_card("""
<div class="hub-card-title green">✅ O que você pode fazer</div>
<ul style="font-size:12px;line-height:1.8;color:#1b5e20;padding-left:16px;">
<li>Responder mensagens no LinkedIn</li>
<li>Usar os scripts deste hub</li>
<li>Qualificar o lead (perguntar 3 coisas)</li>
<li>Criar grupo no WhatsApp</li>
<li>Registrar no CRM</li>
</ul>
""", "green"), unsafe_allow_html=True)

    with c3:
        st.markdown(_card("""
<div class="hub-card-title red">⛔ O que você NÃO pode fazer</div>
<ul style="font-size:12px;line-height:1.8;color:#7f0000;padding-left:16px;">
<li>Inventar preços ou propostas</li>
<li>Mandar PDF sem qualificar primeiro</li>
<li>Avançar sem NCDA assinado</li>
<li>Improvisar scripts</li>
<li>Responder sobre condições técnicas complexas</li>
</ul>
""", "red"), unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(_card("""
<div class="hub-card-title">🧭 Como navegar este hub</div>
<div class="hub-flow-item"><div class="hub-flow-dot">1</div><div><strong>Recebeu uma mensagem?</strong><br><span style="color:#666;font-size:11px;">Vá para <strong>Mapa de Leads</strong> → identifique o tipo de pessoa</span></div></div>
<div class="hub-flow-item"><div class="hub-flow-dot">2</div><div><strong>Não sabe o que dizer?</strong><br><span style="color:#666;font-size:11px;">Vá para <strong>Scripts</strong> → copie e cole o correto</span></div></div>
<div class="hub-flow-item"><div class="hub-flow-dot">3</div><div><strong>Lead pediu preço de algo?</strong><br><span style="color:#666;font-size:11px;">Vá para <strong>Filtro Comprador</strong> → faça as 3 perguntas primeiro</span></div></div>
<div class="hub-flow-item"><div class="hub-flow-dot">4</div><div><strong>Não entende o que ele pediu?</strong><br><span style="color:#666;font-size:11px;">Vá para <strong>FAQ Commodities</strong> ou <strong>Glossário</strong></span></div></div>
<div class="hub-flow-item"><div class="hub-flow-dot">5</div><div><strong>Dúvida de vendas ou processo?</strong><br><span style="color:#666;font-size:11px;">Vá para <strong>FAQ Vendas</strong></span></div></div>
"""), unsafe_allow_html=True)

    with c2:
        st.markdown(_card("""
<div class="hub-card-title">⚡ Regras de ouro — decorar de cabeça</div>
<div style="display:flex;flex-direction:column;gap:8px;margin-top:6px;">
<div style="display:flex;gap:10px;align-items:flex-start;padding:8px;background:#f5f5f5;border-radius:6px;"><span style="font-size:18px;">1️⃣</span><span style="font-size:12px;"><strong>Nunca mandar PDF sem saber:</strong> qual commodity, qual volume, qual porto</span></div>
<div style="display:flex;gap:10px;align-items:flex-start;padding:8px;background:#f5f5f5;border-radius:6px;"><span style="font-size:18px;">2️⃣</span><span style="font-size:12px;"><strong>Nenhuma mensagem sem resposta</strong> por mais de 24 horas</span></div>
<div style="display:flex;gap:10px;align-items:flex-start;padding:8px;background:#f5f5f5;border-radius:6px;"><span style="font-size:18px;">3️⃣</span><span style="font-size:12px;"><strong>Toda mensagem termina com um próximo passo:</strong> "Would it be easier on WhatsApp?"</span></div>
<div style="display:flex;gap:10px;align-items:flex-start;padding:8px;background:#f5f5f5;border-radius:6px;"><span style="font-size:18px;">4️⃣</span><span style="font-size:12px;"><strong>Scripts em inglês são fixos</strong> — copiar e colar, nunca traduzir na cabeça</span></div>
<div style="display:flex;gap:10px;align-items:flex-start;padding:8px;background:#f5f5f5;border-radius:6px;"><span style="font-size:18px;">5️⃣</span><span style="font-size:12px;"><strong>Quem não é comprador</strong> → apresentar o Programa Finder antes de fechar</span></div>
</div>
"""), unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2 — MAPA DE LEADS
# ═══════════════════════════════════════════════════════════════════════════════

def _tab_mapa():
    st.markdown(_note("<strong>💡 O que é 'Conexão Pós-Ativa'?</strong><br>É quando <strong>NÓS pedimos conexão</strong> para alguém no LinkedIn e ele aceitou. Assim que ele aceitar → envie o Script A em até 24h. Não espere ele falar primeiro."), unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("""<div class="hub-card" style="border-top:4px solid #1565C0;">
<div class="hub-card-title blue">👤 PERFIL 1 — O ENGAJADOR</div>
<div style="font-size:12px;line-height:1.8;">
<strong>Quem é:</strong> Agrônomo, consultor, despachante, corretor de grãos<br>
<strong>Como chegou:</strong> Curtiu, comentou ou compartilhou um post<br>
<strong>Ele é o comprador?</strong> <span style="color:#c62828;font-weight:700;">NÃO.</span> Mas pode CONHECER o comprador<br><br>
<div style="background:#E3F2FD;border-radius:6px;padding:10px;margin-top:6px;">
<strong style="color:#1565C0;">→ OBJETIVO:</strong> Recrutá-lo para o Programa Finder<br>
<strong style="color:#1565C0;">→ Script a usar:</strong> Script G (Pitch Finder)
</div>
</div>
</div>""", unsafe_allow_html=True)

    with c2:
        st.markdown("""<div class="hub-card" style="border-top:4px solid #c62828;">
<div class="hub-card-title red">👤 PERFIL 2 — O COMPRADOR / MANDATÁRIO</div>
<div style="font-size:12px;line-height:1.8;">
<strong>Quem é:</strong> Importador direto, trader internacional, empresa que compra commodities<br>
<strong>Como chegou:</strong> Pediu conexão ou nós pedimos<br>
<strong>Ele é o comprador?</strong> <span style="color:#2e7d32;font-weight:700;">SIM (ou representa um)</span><br><br>
<div style="background:#FFEBEE;border-radius:6px;padding:10px;margin-top:6px;">
<strong style="color:#c62828;">→ OBJETIVO:</strong> Qualificar e migrar para WhatsApp<br>
<strong style="color:#c62828;">→ Scripts a usar:</strong> A, B, C, D, E
</div>
</div>
</div>""", unsafe_allow_html=True)

    st.markdown('<div style="font-size:17px;font-weight:900;color:#1a1a1a;margin:20px 0 12px;">Como o Lead Pode Chegar — 8 Situações</div>', unsafe_allow_html=True)

    situations = [
        ("CURTIU UM POST", "badge-blue", "PERFIL 1", "Aparece como 'curtiu' ou 'reagiu' na publicação", "→ Enviar convite de conexão + Script G depois"),
        ("COMENTOU OU COMPARTILHOU", "badge-blue", "PERFIL 1", "Engajamento maior = prioridade. Enviar conexão primeiro.", "→ Prioridade máxima. Conectar + Script G imediatamente"),
        ("PEDIU CONEXÃO PARA NÓS", "badge-red", "PERFIL 2", "Alguém nos encontrou e adicionou. Tem intenção.", "→ Aceitar e usar Script C. Responder em até 24h."),
        ("NÓS PEDIMOS CONEXÃO (pós-ativa)", "badge-red", "PERFIL 2", "Enviamos convite e ele aceitou → Conexão Pós-Ativa", "→ Script A imediatamente (não esperar ele falar)"),
        ("RESPONDEU COM INTERESSE", "badge-red", "PERFIL 2", "Pediu produto, preço ou mais informações", "→ Script D. Fazer 3 perguntas ANTES de qualquer PDF."),
        ("NÃO RESPONDEU (7+ dias)", "badge-gray", "QUALQUER", "Silêncio após primeiro contato", "→ Script F — 1 follow-up. Se não responder: FRIO."),
        ("RESPONDEU MAS NÃO COMPRA", "badge-blue", "PERFIL 1", "O produto não serve para ele diretamente", "→ Script G — Pitch Finder antes de encerrar"),
        ("QUALIFICADO ✅", "badge-green", "PERFIL 2", "Confirmou commodity + volume + porto de destino", "→ Script E — Migrar para WhatsApp + NCDA automático"),
    ]

    cols = st.columns(2)
    for i, (title, badge_cls, badge_txt, sub, action) in enumerate(situations):
        with cols[i % 2]:
            color_map = {"badge-blue": "#1565C0", "badge-red": "#c62828", "badge-gray": "#888", "badge-green": "#2e7d32"}
            bc = color_map.get(badge_cls, "#FA8200")
            st.markdown(f"""<div class="hub-card" style="border-left:3px solid {bc};">
<div style="display:flex;justify-content:space-between;margin-bottom:8px;">
<strong style="font-size:12px;">{title}</strong>
<span class="hub-badge hub-{badge_cls}">{badge_txt}</span>
</div>
<div style="font-size:11px;color:#666;margin-bottom:6px;">{sub}</div>
<div style="font-size:12px;"><strong style="color:#FA8200;">{action}</strong></div>
</div>""", unsafe_allow_html=True)

    st.markdown('<div style="font-size:17px;font-weight:900;color:#1a1a1a;margin:20px 0 14px;">Funil Completo — Do LinkedIn ao Contrato</div>', unsafe_allow_html=True)

    funnel_steps = [
        ("#1565C0", "01", "LinkedIn — Identificar e Qualificar", "Máximo 2-3 mensagens. Usar scripts."),
        ("#1565C0", "02", "Propor WhatsApp", 'Script E — "Could you share your number?"'),
        ("#FA8200", "03", "Criar Grupo WhatsApp", '"Samba x [Nome]" — Leonardo + Marcelo + Nívio'),
        ("#FA8200", "04", "NCDA via ZapSign (automático)", "Contrato enviado automaticamente. Lead assina pelo celular."),
        ("#2e7d32", "05", "NCDA Assinado ✅", "Sócios assumem. FCO → SPA → Instrumento financeiro."),
        ("#2e7d32", "06", "Negociação e Fechamento", "Leonardo + Nívio + Marcelo cuidam de tudo aqui."),
        ("#2e7d32", "07", "Comissão / Receita 💰", "Negócio fechado. Samba executa. Finder recebe."),
    ]

    bg_map = {"#1565C0": "#E3F2FD", "#FA8200": "#FFF3E0", "#2e7d32": "#E8F5E9"}
    for color, num, title, sub in funnel_steps:
        bg = bg_map.get(color, "#fff")
        st.markdown(f"""<div class="hub-funnel-step">
<div class="hub-funnel-num" style="background:{color};min-height:48px;">{num}</div>
<div class="hub-funnel-body" style="background:{bg};"><span style="font-weight:700;">{title}</span><span style="font-size:11px;color:#666;">{sub}</span></div>
</div>""", unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3 — FILTRO COMPRADOR
# ═══════════════════════════════════════════════════════════════════════════════

def _tab_filtro():
    questions = [
        ("01", "Qual é o nome da empresa e o site?", "Precisamos verificar se a empresa é real e qual é o porte."),
        ("02", "A empresa já importa atualmente?", "Quem já importa tem processo e banco. Quem nunca importou tem mais risco."),
        ("03", "Para onde vai a carga? (país + porto)", "Define tudo: certificações, documentação, preço e logística."),
        ("04", "O lead é comprador final ou intermediário?", "Comprador final fecha direto. Intermediário (mandatário) representa outra empresa."),
        ("05", "Qual commodity e especificação?", "Soja GMO ou Non-GMO? Açúcar ICUMSA 45 ou 150? Os detalhes mudam o preço."),
        ("06", "Qual volume? (toneladas ou sacas)", "Abaixo de 1.000 MT = pedido de teste. Acima de 5.000 MT = contrato anual."),
        ("07", "Condição: CIF ou FOB?", "CIF = Samba paga o frete. FOB = o comprador paga o frete."),
        ("08", "Como ele vai pagar? (instrumento financeiro)", "DLC MT700 ou SBLC MT760 = pagamento bancário garantido. Sem isso = sem negócio."),
        ("09", "Qual prazo de entrega esperado?", "Alinha expectativa com o calendário de safra e logística."),
        ("10", "Tem empresa verificável no LinkedIn/site?", "Empresa sem presença digital = sinal de alerta. Checar antes de avançar."),
    ]

    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<p style="font-size:12px;color:#666;margin-bottom:14px;">Use como checklist antes de qualquer proposta. Confirme o que já sabe pelo perfil LinkedIn.</p>', unsafe_allow_html=True)
        for num, q, hint in questions:
            st.markdown(f"""<div class="hub-fq">
<div style="display:flex;gap:10px;margin-bottom:4px;"><span class="hub-badge hub-badge-orange">{num}</span><strong style="font-size:12px;">{q}</strong></div>
<div style="font-size:11px;color:#666;">📌 {hint}</div>
</div>""", unsafe_allow_html=True)

    with c2:
        st.markdown(_card("""
<div class="hub-card-title red">⛔ Sinais de Alerta — Leads Problemáticos</div>
<div style="padding:10px;background:#fff;border-radius:8px;border:1px solid #EF9A9A;margin-bottom:8px;">
<span class="hub-badge hub-badge-red" style="margin-bottom:6px;display:inline-block;">ALTO RISCO</span><br>
<strong>Diz ter "mandato de governo"</strong> sem apresentar documento<br>
<span style="font-size:11px;color:#666;">→ Pedir carta de mandato ANTES de qualquer avanço</span>
</div>
<div style="padding:10px;background:#fff;border-radius:8px;border:1px solid #EF9A9A;margin-bottom:8px;">
<span class="hub-badge hub-badge-red" style="margin-bottom:6px;display:inline-block;">ALTO RISCO</span><br>
<strong>Quer fechar sem NCDA</strong> ou sem instrumento financeiro<br>
<span style="font-size:11px;color:#666;">→ Regra absoluta: sem NCDA assinado, sem negociação</span>
</div>
<div style="padding:10px;background:#fff;border-radius:8px;border:1px solid #EF9A9A;margin-bottom:8px;">
<span class="hub-badge hub-badge-red" style="margin-bottom:6px;display:inline-block;">ALTO RISCO</span><br>
<strong>Empresa sem site, sem CNPJ/registro verificável</strong><br>
<span style="font-size:11px;color:#666;">→ Não avançar. Due diligence primeiro. Passar para Nívio ou Marcelo.</span>
</div>
<div style="padding:10px;background:#fff;border-radius:8px;border:1px solid #FFB74D;margin-bottom:8px;">
<span class="hub-badge hub-badge-orange" style="margin-bottom:6px;display:inline-block;">MÉDIO RISCO</span><br>
<strong>Pede preço sem informar volume, porto ou condição</strong><br>
<span style="font-size:11px;color:#666;">→ Usar Script D. Pedir 3 informações antes de qualquer oferta.</span>
</div>
<div style="padding:10px;background:#fff;border-radius:8px;border:1px solid #FFB74D;">
<span class="hub-badge hub-badge-orange" style="margin-bottom:6px;display:inline-block;">MÉDIO RISCO</span><br>
<strong>Responde só "interested" sem detalhar</strong><br>
<span style="font-size:11px;color:#666;">→ Continuar qualificando com Script D. Não assumir interesse real.</span>
</div>
""", "red"), unsafe_allow_html=True)

        st.markdown(_card("""
<div class="hub-card-title">Finder vs. Comprador — A diferença em 30 segundos</div>
<table class="hub-table">
<tr><th>Finder (Engajador)</th><th>Comprador</th></tr>
<tr><td>Indica um contato</td><td>É o contato que compra</td></tr>
<tr><td>Não assina nada no negócio</td><td>Assina LOI e SPA</td></tr>
<tr><td>Não negocia preço</td><td>Recebe proposta e negocia</td></tr>
<tr><td>Recebe comissão (10-30%)</td><td>Paga pela carga</td></tr>
<tr><td>Permanece sigiloso</td><td>Entra no contrato principal</td></tr>
</table>
"""), unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4 — SCRIPTS
# ═══════════════════════════════════════════════════════════════════════════════

def _script_block(badge_label, title, when_note, en_text, pt_text, tip_text="", warn_text=""):
    note_html = _note(f"<strong>Quando usar:</strong> {when_note}") if when_note else ""
    tip_html  = _tip(f"<strong>💡 Dica:</strong> {tip_text}") if tip_text else ""
    warn_html = _warn(f"<strong>⛔</strong> {warn_text}") if warn_text else ""
    with st.expander(f"{badge_label}  {title}"):
        if note_html: st.markdown(note_html, unsafe_allow_html=True)
        if warn_text: st.markdown(warn_html, unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f'<div class="hub-lang-tag-en">🇺🇸 INGLÊS — ENVIAR ASSIM</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="hub-script-en">{en_text}</div>', unsafe_allow_html=True)
        with c2:
            st.markdown(f'<div class="hub-lang-tag-pt">🇧🇷 PORTUGUÊS — SÓ PARA ENTENDER</div>', unsafe_allow_html=True)
            st.markdown(f'<div class="hub-script-pt">{pt_text}</div>', unsafe_allow_html=True)
        if tip_html: st.markdown(tip_html, unsafe_allow_html=True)


def _tab_scripts():
    st.markdown(_card('<div class="hub-card-title blue">PERFIL 1 — ENGAJADOR / POTENCIAL FINDER</div>', "blue"), unsafe_allow_html=True)

    _script_block("Script G", "Pitch Finder — Para quem curtiu, comentou ou compartilhou",
        "Pessoa que interagiu com algum post. NÃO é comprador. Pode conhecer um comprador.",
        "Hi [Nome]! I noticed you engaged with our content — thank you for that.\nWe run a formal Finder Program: if you know serious active buyers in commodities, we pay 10–30% commission on closed deals, for up to 36 months.\nZero investment. Zero conflict of interest. Fully backed by legal contract.\nWould this be of interest to you?",
        "Olá [Nome]! Vi que você interagiu com nosso conteúdo — obrigado por isso.\nTemos um Programa de Finders formal: se você conhece compradores sérios ativos em commodities, pagamos comissão de 10% a 30% em negócios fechados, por até 36 meses.\nZero investimento. Zero conflito de interesses. Tudo formalizado em contrato.\nIsso seria de seu interesse?",
        "Nunca fale de produto para o Engajador. Ele não é o comprador. Ele pode INDICAR um comprador para nós. Essa é a diferença.")

    _script_block("Script H", "Arsenio Longo — Retomada (respondeu e não recebeu retorno)",
        "",
        "Hi Arsenio! Apologies for the delayed follow-up.\nI wanted to reconnect — your background in maritime analytics is exactly the kind of network that fits our Finder Program well.\nIf you know operators, importers or commodity buyers through your work, we pay formal commissions on any deal that closes.\nWorth a conversation?",
        "Olá Arsenio! Peço desculpas pela demora na resposta.\nQueria retomar o contato — sua atuação em analytics marítimo é exatamente o tipo de rede que se encaixa bem no nosso Programa de Finders.\nSe você conhece operadores, importadores ou compradores de commodities pelo seu trabalho, pagamos comissões formais em qualquer negócio fechado.\nVale uma conversa?")

    st.markdown(_card('<div class="hub-card-title red">PERFIL 2 — COMPRADOR / MANDATÁRIO</div>', "red"), unsafe_allow_html=True)

    _script_block("Script A", "Pós-Conexão Ativa — Nós pedimos, ele aceitou",
        "Você enviou o pedido de conexão, ele aceitou. Enviar em até 24h. Não esperar ele falar primeiro.",
        "Hi [Nome]! Thank you for connecting.\nWe operate as originators and exporters of Brazilian commodities on a large scale, with structured execution from origin to shipment.\nI'm seeking relationships with direct buyers and mandates who have active import demand.\nIf this is of interest to you, I'd be glad to have a quick conversation.",
        "Olá [Nome]! Obrigado por aceitar a conexão.\nSomos originadores e exportadores de commodities brasileiras em larga escala, com execução estruturada da origem até o embarque.\nBusco relações com compradores diretos e mandatários que tenham demanda ativa de importação.\nSe isso for de seu interesse, fico à disposição para uma conversa rápida.")

    _script_block("Script B", "Lead conecta e fala primeiro",
        "O lead aceitou e mandou uma mensagem, OU ele nos adicionou e falou primeiro.",
        "Hi [Nome]! Great to connect.\nWe are Brazilian commodity originators and exporters — large scale, full compliance, competitive CIF worldwide.\nAre you a buyer or do you represent import demand for any specific commodity?",
        "Olá [Nome]! Ótimo conectar.\nSomos originadores e exportadores brasileiros de commodities — larga escala, compliance completo, preços CIF competitivos para todo o mundo.\nVocê é comprador direto ou representa demanda de importação para alguma commodity específica?",
        "Sempre terminar com uma pergunta direta. O objetivo é identificar se é comprador ou intermediário.")

    _script_block("Script D ⚠️", "Lead demonstra interesse em produto — 3 perguntas OBRIGATÓRIAS",
        "Lead pediu preço, produto ou mandou mensagem demonstrando interesse.",
        "Perfect — we have active supply for [COMMODITY].\nTo prepare an indicative offer, I need just 3 quick details:\n1. Approximate volume? (MT or bags)\n2. Destination port?\n3. Preferred terms — CIF or FOB?\nWould it be easier to continue on WhatsApp?",
        "Perfeito — temos abastecimento ativo de [COMMODITY].\nPara preparar uma oferta indicativa, preciso de apenas 3 informações rápidas:\n1. Volume aproximado? (MT ou sacas)\n2. Porto de destino?\n3. Condição preferida — CIF ou FOB?\nSeria mais prático continuar pelo WhatsApp?",
        "",
        "NUNCA pule este script. Este é o mais importante. Antes de qualquer PDF, preço ou proposta — faça essas 3 perguntas. Esse foi o erro com Tejinder Singh (7 PDFs sem qualificação).")

    _script_block("Script E", "Migração para WhatsApp — Qualificado",
        "Lead confirmou interesse real. Respondeu às 3 perguntas do Script D.",
        "Great — let's move this forward on WhatsApp for faster communication.\nCould you share your number? I'll add you to our commercial group with our team.",
        "Ótimo — vamos acelerar no WhatsApp para comunicação mais rápida.\nPode compartilhar seu número? Vou te adicionar ao nosso grupo comercial com nossa equipe.",
        "Grupo WhatsApp: Criar como 'Samba x [Nome do Lead]'. Incluir Leonardo + Marcelo + Nívio. NCDA enviado automaticamente pelo sistema ZapSign.")

    _script_block("Script F", "Follow-up — Lead sumiu por 7+ dias",
        "Lead não respondeu em 7+ dias após primeiro contato.",
        "Hi [Nome] — just following up on our last conversation.\nWe still have active supply available. If the timing is right, happy to pick up where we left off.",
        "Olá [Nome] — retomando nossa conversa anterior.\nAinda temos abastecimento ativo disponível. Se o momento for oportuno, podemos continuar de onde paramos.",
        "",
        "Apenas UM follow-up. Se não responder, status no CRM: 'Frio — aguardar'. Não insistir mais.")

    _script_block("Script J", "Tejinder Singh — Retomada (erro cometido)",
        "Contexto: Tejinder pediu CDSO 30.000 MT CIF Mundra. Equipe mandou 7 PDFs sem qualificar. Conversa morreu.",
        "Hi Tejinder — checking back in.\nApologies for overloading you earlier — I should have asked first.\nIs the demand for CDSO 30,000 MT CIF Mundra/Chennai still active?\nIf so, I can have an indicative price ready within 24 hours.",
        "Olá Tejinder — retomando contato.\nPeço desculpas pelo excesso de materiais antes — deveria ter perguntado primeiro.\nA demanda de CDSO 30.000 MT CIF Mundra/Chennai ainda está ativa?\nSe sim, consigo ter um preço indicativo em até 24 horas.")


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5 — EXEMPLOS PRÁTICOS
# ═══════════════════════════════════════════════════════════════════════════════

def _tab_exemplos():
    st.markdown('<p style="font-size:12px;color:#666;margin-bottom:16px;">Como um comprador real escreve, o que ele quer dizer e como responder corretamente. Clique para abrir cada exemplo.</p>', unsafe_allow_html=True)

    with st.expander("🇧🇷 Açúcar ICUMSA 45 — Arábia Saudita"):
        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div style="font-weight:700;color:#c62828;font-size:12px;margin-bottom:8px;">📩 O que o lead escreveu (em inglês)</div>', unsafe_allow_html=True)
            st.code("We are looking for Sugar ICUMSA 45, 10,000 MT monthly, CIF Jeddah Saudi Arabia. Please send your best offer with full specs.", language=None)
            st.markdown('<div style="font-weight:700;color:#1565C0;font-size:12px;margin:10px 0 6px;">📩 Tradução — o que ele quer</div>', unsafe_allow_html=True)
            st.markdown(_info("Procuramos Açúcar ICUMSA 45, 10.000 toneladas por mês, com entrega CIF no porto de Jeddah, Arábia Saudita."), unsafe_allow_html=True)
        with c2:
            st.markdown('<div style="font-weight:700;color:#2e7d32;font-size:12px;margin-bottom:8px;">📋 Especificações do produto</div>', unsafe_allow_html=True)
            st.markdown(_tip("Polarização ≥ 99,80°Z<br>Umidade ≤ 0,04%<br>ICUMSA ≤ 45 UI<br>Sulfatos ≤ 2ppm<br>Origem: Brasil (Bonsucro/Halal certificado)"), unsafe_allow_html=True)
            st.markdown(_warn("<strong>⚠️ Filtro antes de responder:</strong> Confirmar se é comprador final ou mandatário. Perguntar instrumento de pagamento. Não mandar preço antes disso."), unsafe_allow_html=True)
        st.markdown('<div style="font-weight:700;font-size:12px;margin:14px 0 8px;color:#2e7d32;">✅ Resposta correta — usar assim:</div>', unsafe_allow_html=True)
        st.markdown("""<div class="hub-script-en">Thank you for reaching out.
We supply Sugar ICUMSA 45 from certified Brazilian mills — Bonsucro / Halal certified, available CIF ASWP.

Before preparing your indicative price, quick confirm:
- Are you the end buyer or a mandatary?
- Payment instrument: DLC MT700 or SBLC MT760?
- Monthly contract (12 months) or spot trial first?

Once confirmed, FCO ready within 24 hours.</div>""", unsafe_allow_html=True)

    with st.expander("🇮🇳 CDSO 30.000 MT — Índia (caso Tejinder Singh — erro cometido)"):
        st.markdown(_warn("<strong>Lição:</strong> Tejinder enviou interesse em CDSO. Equipe mandou 7 PDFs sem qualificar. Conversa morreu. Use o Script J para retomar e o Script D para novos leads similares."), unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div class="hub-lang-tag-en">🇺🇸 O que o lead pediu</div>', unsafe_allow_html=True)
            st.code("We are interested in CDSO (Crude Degummed Soybean Oil) 30,000 MT CIF Mundra/Chennai, India. Please advise.", language=None)
        with c2:
            st.markdown('<div class="hub-lang-tag-pt">🇧🇷 O que deveria ter sido feito</div>', unsafe_allow_html=True)
            st.markdown(_note("Usar Script D imediatamente: confirmar volume (30.000 MT ✅), porto (Mundra/Chennai ✅), instrumento de pagamento (❓ — faltou perguntar). <strong>Nunca mandar PDF antes disso.</strong>"), unsafe_allow_html=True)

    with st.expander("🌽 Soja / Milho GMO — China"):
        st.markdown(_tip("<strong>GACC obrigatório:</strong> A China exige registro GACC para importar alimentos. Samba tem esse registro — sempre confirmar quando lead perguntar."), unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            st.markdown('<div class="hub-lang-tag-en">🇺🇸 Exemplo de pedido típico</div>', unsafe_allow_html=True)
            st.code("We need Soybean GMO 50,000 MT, FOB Santos, GAFTA 100 contract. Do you have GACC registration?", language=None)
        with c2:
            st.markdown('<div class="hub-lang-tag-en">🇺🇸 Resposta correta (Script D adaptado)</div>', unsafe_allow_html=True)
            st.code("Yes, we are GACC registered and can supply Soybean GMO under GAFTA 100.\nTo prepare our FCO: confirm delivery period and payment instrument (DLC MT700 / SBLC MT760)?", language=None)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6 — FAQ VENDAS
# ═══════════════════════════════════════════════════════════════════════════════

def _tab_faq_vendas():
    faqs = [
        ("PROCESSO E SCRIPTS", [
            ("Por que preciso usar os scripts exatamente? Não posso adaptar?",
             "Porque os scripts foram testados e funcionam. Qualquer adaptação pode mudar o tom e perder o lead. Em inglês especialmente: copiar e colar direto. Se quiser adaptar algo, fale com Marcelo ou Nívio antes."),
            ("O que faço se o lead responder em português?",
             "Se for brasileiro, pode continuar em português. Se for estrangeiro escrevendo em português (ex: angolano, português), continue em português. Mas se for de Índia, China, Oriente Médio — responda em inglês mesmo. Os scripts em inglês são os que funcionam."),
            ("Posso falar em português com os leads?",
             "Depende do lead. Se ele for brasileiro, pode usar português. Se for estrangeiro, use inglês. Os nossos compradores são principalmente da Índia, China, Oriente Médio, Europa e África — então a grande maioria vai ser em inglês."),
            ("O que faço se o lead me mandar uma mensagem em outra língua (árabe, chinês, etc.)?",
             "Responda em inglês normalmente. A língua dos negócios internacionais é o inglês. Use o Google Translate para entender o que ele escreveu, mas responda sempre em inglês usando os scripts."),
        ]),
        ("SOBRE O PROGRAMA FINDER", [
            ("O que exatamente é um Finder? Explica de forma bem simples.",
             "Imagina que você tem um amigo que trabalha num banco que importa grãos. Você apresenta esse amigo para a Samba Export. A Samba fecha o negócio com esse banco. Você recebeu uma comissão sem fazer nada além da apresentação. Isso é um Finder. O Finder não vende, não negocia, não assina nada. Ele só apresenta. A Samba faz todo o resto."),
            ("Como o Finder recebe a comissão? Quanto?",
             "A comissão é de 10% a 30% sobre a margem da operação, por até 36 meses em negócios recorrentes.\n\nExemplos reais:\n• Soja 50.000 ton → Finder recebe de R$26k a R$78k por indicação\n• Milho 30.000 ton → R$45k a R$180k\n• Açúcar 10.000 ton → R$10k a R$50k\n\nTudo formalizado em contrato antes da apresentação."),
            ("Para quem devo fazer o pitch do Programa Finder?",
             "Para qualquer pessoa que não seja o comprador final: agrônomos, despachantes, corretores de grãos, agentes de carga, advogados de comex, traders bancários. Qualquer profissional que tenha uma rede de relacionamentos com importadores pode ser um Finder. Se alguém interagiu com nosso conteúdo mas claramente não é um comprador direto, apresente o Programa Finder."),
        ]),
        ("SOBRE O WHATSAPP E O NCDA", [
            ("O que é o NCDA e por que é tão importante?",
             "NCDA significa Non-Circumvention and Non-Disclosure Agreement — Acordo de Não Circunvenção e Confidencialidade. É um contrato que protege todo mundo:\n\n• Protege o Finder: garante que ele vai receber a comissão mesmo que tentem 'pular' ele\n• Protege a Samba: garante que o comprador não vai pegar os contatos e ir direto ao fornecedor\n• Protege o comprador: garante confidencialidade dos dados da operação\n\nSem NCDA assinado = nenhuma negociação avança. É regra não negociável."),
            ("Como o grupo WhatsApp funciona? O que eu faço depois de criar?",
             "Processo:\n1. Criar grupo: 'Samba x [Nome do Lead]'\n2. Adicionar: Leonardo + Marcelo + Nívio + você\n3. O sistema (Make.com + ZapSign) envia o NCDA automaticamente para o lead assinar\n4. Quando o lead assinar, avisar os sócios no grupo: 'NCDA assinado ✅'\n5. A partir daí os sócios assumem a negociação\n\nSua função no grupo depois disso é acompanhar e registrar atualizações no CRM."),
        ]),
        ("SOBRE O CRM E REGISTRO", [
            ("Por que preciso registrar tudo na planilha? Parece burocracia...",
             "Porque um lead que não está registrado pode ser abordado por dois membros da equipe ao mesmo tempo, causando confusão. Também porque leads 'frios' de hoje podem virar compradores em 6 meses — e sem registro, a gente perde esse histórico. Além disso, os sócios precisam saber exatamente em que estágio cada negócio está sem precisar perguntar toda hora."),
        ]),
    ]

    for cat, items in faqs:
        st.markdown(f'<div class="hub-cat">{cat}</div>', unsafe_allow_html=True)
        for q, a in items:
            with st.expander(q):
                st.markdown(f'<div style="font-size:12px;line-height:1.8;color:#2d2d2d;white-space:pre-wrap;">{a}</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 7 — FAQ COMMODITIES
# ═══════════════════════════════════════════════════════════════════════════════

def _tab_faq_commo():
    faqs = [
        ("O QUE É COMMODITY E O QUE A SAMBA VENDE", [
            ("O que é uma commodity? Explica como se eu tivesse 12 anos.",
             "Commodity é um produto que é igual em todo o lugar. Uma tonelada de soja brasileira e uma tonelada de soja argentina têm as mesmas características — são intercambiáveis. Por isso o preço é fixado no mercado global.\n\nExemplos: soja, milho, açúcar, café, petróleo, ouro. Tudo que você vê na natureza e pode medir em toneladas ou barris é basicamente uma commodity.\n\nÉ diferente de um produto manufaturado como um celular ou um carro, onde a marca faz diferença."),
            ("Quais são os principais produtos que a Samba vende?",
             "Os mais comuns nos pedidos de compra:\n\nAgro (os mais pedidos):\n• Soja (grão inteiro GMO)\n• Farelo de Soja (46-48% de proteína)\n• Açúcar ICUMSA 45 (branco refinado), ICUMSA 150 (cristal) e VHP (1200) (bruto)\n• Milho Amarelo GMO\n• Óleos vegetais: CDSO (óleo de soja bruto), RBD (refinado), Girassol, Palma, Canola\n• Arroz branco, feijão preto, carioca\n\nProteínas: Frango (peito, coxa, carcaça), Carne Bovina, Suíno\n\nBrazil Flavors: Café Arábica, Mel orgânico, Açaí, Cacau, Castanhas\n\nMinerais/Industrial: Minério de Ferro, Lítio, Etanol"),
            ("O que é ICUMSA? Qual a diferença entre 45, 150 e VHP?",
             "ICUMSA é uma medida de cor e pureza do açúcar. Quanto menor o número, mais branco e puro.\n\n• ICUMSA 45: Açúcar branco super refinado. O mais caro. Para consumo direto no Oriente Médio, Europa, Ásia.\n• ICUMSA 150: Açúcar cristal (granulado branco). Para indústria alimentícia.\n• VHP (Very High Polarization) — ICUMSA 1200: Açúcar bruto, amarelado. Para refinarias que processam o açúcar no destino. Mais barato.\n\nQuando o lead pede 'Sugar ICUMSA 45', ele quer o branco refinado premium."),
            ("O que é CDSO? E RBD? Eles aparecem bastante nos pedidos.",
             "CDSO = Crude Degummed Soybean Oil = Óleo de Soja Bruto. É o óleo de soja antes de ser refinado. Mais barato, usado pela indústria para refinar ou fazer biodiesel.\n\nRBD = Refined, Bleached and Deodorized = Óleo de Soja Refinado. É o óleo que vai para consumo direto — o que você compra no supermercado. Mais caro.\n\nTejinder Singh, por exemplo, pediu CDSO 30.000 MT — ou seja, óleo de soja bruto para a Índia."),
            ("O que é GMO? Todos os compradores aceitam?",
             "GMO = Genetically Modified Organism = Organismo Geneticamente Modificado. A maioria da soja e milho do Brasil é GMO. Isso é normal e aceito nos EUA, China, Oriente Médio e muitos outros países.\n\nMas alguns países e compradores preferem Non-GMO — especialmente Europa (alguns produtos) e compradores de alimentos orgânicos.\n\nQuando o lead pedir, confirme se ele aceita GMO ou precisa de Non-GMO. Se não souber, é uma das perguntas do Script D."),
        ]),
        ("LOGÍSTICA E TRANSPORTE", [
            ("Qual a diferença entre CIF e FOB? Quando uso cada um?",
             "CIF (Cost, Insurance and Freight): A Samba paga o frete marítimo e o seguro até o porto de destino do comprador. É o mais comum nos nossos contratos.\n\nFOB (Free On Board): A Samba entrega a mercadoria no porto de embarque (ex: Santos). O comprador paga o frete marítimo a partir daí.\n\nQuando o lead pede 'CIF Mundra', ele quer que a Samba entregue até a Índia pagando o frete. Quando pede 'FOB Santos', ele mesmo vai contratar o navio."),
            ("O que significa CIF ASWP?",
             "ASWP = Any Safe World Port = Qualquer porto seguro do mundo. Quando a Samba diz 'CIF ASWP', significa que pode entregar em qualquer porto do mundo, pagando o frete e o seguro. É a condição mais ampla e mostra capacidade global de entrega."),
        ]),
        ("CERTIFICAÇÕES E REGISTROS", [
            ("O que são GACC, DG SANTE e FDA? Por que importam?",
             "São registros obrigatórios para exportar alimentos para determinados países:\n\n• GACC = General Administration of Customs of China. Registro necessário para exportar alimentos para a China. Samba tem esse registro.\n• DG SANTE = Diretoria-Geral da União Europeia para Saúde e Alimentação. Registro para exportar para os 27 países da UE. Samba tem esse registro.\n• FDA = Food and Drug Administration. Registro para exportar para os EUA.\n\nPor que importa? Se um lead da China perguntar se temos registro GACC, a resposta é 'yes'. Se um europeu perguntar sobre DG SANTE, a resposta é 'yes'. Isso dá segurança ao comprador que a Samba pode efetivamente exportar."),
        ]),
    ]

    for cat, items in faqs:
        st.markdown(f'<div class="hub-cat">{cat}</div>', unsafe_allow_html=True)
        for q, a in items:
            with st.expander(q):
                st.markdown(f'<div style="font-size:12px;line-height:1.8;color:#2d2d2d;white-space:pre-wrap;">{a}</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 8 — GLOSSÁRIO
# ═══════════════════════════════════════════════════════════════════════════════

def _tab_glossario():
    terms = [
        ("ASWP", "Any Safe World Port — Qualquer porto seguro do mundo. Quando dizemos 'CIF ASWP', significa que entregamos em qualquer porto do mundo."),
        ("BL / B/L", "Bill of Lading — Conhecimento de Embarque. Documento que comprova que a mercadoria foi carregada no navio. É como o recibo de entrega do embarque."),
        ("CDSO", "Crude Degummed Soybean Oil — Óleo de Soja Bruto. Antes de ser refinado. Usado por indústrias e refinarias."),
        ("CIF", "Cost, Insurance and Freight — Custo, Seguro e Frete. O vendedor (Samba) paga tudo até o porto do comprador."),
        ("DLC / LC", "Documentary Letter of Credit — Carta de Crédito Documentária. Garantia de pagamento emitida pelo banco do comprador. Padrão SWIFT MT700."),
        ("DG SANTE", "Diretoria-Geral da União Europeia para Saúde e Alimentação. Registro obrigatório para exportar alimentos para os 27 países da UE. Samba tem esse registro."),
        ("FCO", "Full Corporate Offer — Oferta Corporativa Completa. Proposta formal da Samba com todos os detalhes: produto, specs, preço, prazo, condições. Emitida pelos sócios."),
        ("Finder", "Indicador remunerado. Profissional que usa sua rede para apresentar compradores à Samba Export, recebendo comissão de 10-30% por até 36 meses."),
        ("FOB", "Free On Board — O vendedor (Samba) entrega a mercadoria no porto de origem. O comprador é responsável pelo frete marítimo e seguro."),
        ("FOSFA 54", "Federation of Oils, Seeds and Fats Associations. Conjunto de regras internacionais para contratos de óleos e gorduras. Usado nos SPAs da Samba para óleos vegetais."),
        ("GACC", "General Administration of Customs of China — Registro necessário para exportar alimentos para a China. Samba tem esse registro."),
        ("GAFTA 100", "Grain and Feed Trade Association. Conjunto de regras internacionais para contratos de grãos. Usado nos SPAs da Samba para soja, milho, açúcar etc."),
        ("GMO", "Genetically Modified Organism — Organismo Geneticamente Modificado. A maioria da soja e milho do Brasil é GMO. Alguns compradores exigem Non-GMO."),
        ("ICPO", "Irrevocable Corporate Purchase Order — Ordem de Compra Corporativa Irrevogável. Documento formal do comprador confirmando pedido de compra."),
        ("ICUMSA", "International Commission for Uniform Methods of Sugar Analysis. Medida de cor e pureza do açúcar. ICUMSA 45 = branco refinado. VHP (1200) = bruto."),
        ("KYC / CIS", "Know Your Customer / Customer Information Sheet. Documentos de identificação do comprador. Due diligence básica antes de avançar."),
        ("LOI", "Letter of Intent — Carta de Intenção. Documento do comprador demonstrando interesse sério de compra. Precede o FCO e o SPA."),
        ("MT", "Metric Ton — Tonelada Métrica (1.000 kg). Unidade padrão para commodities internacionais."),
        ("Mandatário", "Intermediário que representa um comprador. Tem uma 'procuração' (mandato) para negociar em nome de outro. Não é o comprador final."),
        ("NCDA / IFPA", "Non-Circumvention, Non-Disclosure Agreement — Acordo de Não Circunvenção e Confidencialidade. Contrato obrigatório antes de qualquer negociação séria."),
        ("RBD", "Refined, Bleached and Deodorized — Óleo de Soja Refinado, Branqueado e Desodorizado. Óleo refinado para consumo humano direto."),
        ("SBLC", "Standby Letter of Credit — Carta de Crédito Standby. Garantia bancária de pagamento, padrão SWIFT MT760. Usada em contratos anuais de grande volume."),
        ("SGS / BV / Intertek", "Empresas de inspeção e certificação independentes. Verificam peso e qualidade da mercadoria no porto antes do embarque. Garantia para o comprador."),
        ("SPA", "Sales and Purchase Agreement — Contrato de Compra e Venda. Contrato principal assinado pela Samba e pelo comprador. Segue padrões GAFTA ou FOSFA."),
        ("TT", "Telegraphic Transfer — Transferência bancária internacional. Usado em pedidos de teste: 30% adiantado + 70% antes do embarque."),
        ("VHP", "Very High Polarization — Açúcar bruto com alta polarização (pureza). ICUMSA ~1200. Menos refinado que o ICUMSA 45. Usado por refinarias."),
    ]

    st.markdown('<div class="hub-card">', unsafe_allow_html=True)

    # Search box
    busca = st.text_input("🔍 Buscar termo...", placeholder="Digite um termo ou sigla...", key="hub_gloss_search")

    items_html = ""
    for term, definition in terms:
        if busca and busca.lower() not in term.lower() and busca.lower() not in definition.lower():
            continue
        items_html += f'<div class="hub-glossary-item"><div class="hub-glossary-term">{term}</div><div class="hub-glossary-def">{definition}</div></div>'

    st.markdown(f'<div>{items_html}</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 9 — REGRAS CRM
# ═══════════════════════════════════════════════════════════════════════════════

def _tab_regras_crm():
    st.markdown(_card("""
<div class="hub-card-title orange">Como Copiar o Link do LinkedIn — Passo a Passo</div>
<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:12px;margin-top:8px;">
<div style="background:#fff;border-radius:8px;padding:14px;">
<div style="color:#FA8200;font-weight:900;font-size:22px;margin-bottom:8px;">01</div>
<strong>Abrir no computador</strong>
<div style="font-size:11px;color:#666;margin-top:6px;line-height:1.6;">Ir ao perfil pelo navegador (Chrome, Edge ou Safari) — não pelo aplicativo de celular.</div>
</div>
<div style="background:#fff;border-radius:8px;padding:14px;">
<div style="color:#FA8200;font-weight:900;font-size:22px;margin-bottom:8px;">02</div>
<strong>Clicar "Mais" ou "..."</strong>
<div style="font-size:11px;color:#666;margin-top:6px;line-height:1.6;">Abaixo da foto de perfil → clicar em <strong>"Mais"</strong> → selecionar <strong>"Copiar link do perfil"</strong>.</div>
</div>
<div style="background:#fff;border-radius:8px;padding:14px;">
<div style="color:#FA8200;font-weight:900;font-size:22px;margin-bottom:8px;">03</div>
<strong>Colar no CRM</strong>
<div style="font-size:11px;color:#666;margin-top:6px;line-height:1.6;">Coluna <strong>"LinkedIn URL"</strong>. A URL correta começa com: <code>linkedin.com/in/nomedapessoa</code></div>
</div>
</div>
""", "orange"), unsafe_allow_html=True)

    c1, c2 = st.columns(2)
    with c1:
        st.markdown('<p style="font-weight:700;color:#FA8200;margin-bottom:12px;">Regras de preenchimento obrigatórias</p>', unsafe_allow_html=True)

        rules = [
            ("Registrar no mesmo dia do primeiro contato", "Nunca 'deixar para depois'. Se falou com o lead hoje, registra hoje."),
            ("Usar exatamente as 14 categorias", "Não inventar categorias. Se dúvida: categoria 14 (não qualificado) e revisar depois."),
            ("Registrar qual membro da equipe está tratando", "Colunas com email: lbd@ / nmd@ / cjs@ / jrt@ / pmn@ / marcelo.magalhaes@"),
            ("Nome do grupo WhatsApp sempre padronizado", '"Samba x [Primeiro Nome] [Empresa]" — Ex: "Samba x Tejinder MBB"'),
            ("Atualizar status a cada interação", "Nunca deixar desatualizado por mais de 48h. Status define prioridade."),
        ]
        for i, (rule, hint) in enumerate(rules, 1):
            st.markdown(f"""<div class="hub-rule-item">
<div class="hub-rule-num">{i}</div>
<div><strong style="font-size:12px;">{rule}</strong><br><span style="font-size:11px;color:#666;">{hint}</span></div>
</div>""", unsafe_allow_html=True)

    with c2:
        st.markdown('<p style="font-weight:700;color:#FA8200;margin-bottom:12px;">Status do Lead — Do início ao fechamento</p>', unsafe_allow_html=True)
        st.markdown("""<div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:16px;">
<span class="hub-pill" style="background:#E3F2FD;border-color:#90CAF9;color:#1565C0;">1. Novo</span>
<span class="hub-pill" style="background:#E3F2FD;border-color:#90CAF9;color:#1565C0;">2. Em Contato</span>
<span class="hub-pill" style="background:#FFF3E0;border-color:#FA8200;color:#FA8200;">3. Qualificado</span>
<span class="hub-pill" style="background:#FFF3E0;border-color:#FA8200;color:#FA8200;">4. WhatsApp</span>
<span class="hub-pill" style="background:#E8F5E9;border-color:#2e7d32;color:#2e7d32;">5. NCDA Enviado</span>
<span class="hub-pill" style="background:#E8F5E9;border-color:#2e7d32;color:#2e7d32;">6. NCDA Assinado</span>
<span class="hub-pill" style="background:#E8F5E9;border-color:#2e7d32;color:#2e7d32;">7. Em Negociação</span>
<span class="hub-pill" style="background:#E8F5E9;border-color:#2e7d32;color:#2e7d32;">8. FCO Enviado</span>
<span class="hub-pill" style="background:#1b5e20;border-color:#1b5e20;color:#fff;">9. Fechado ✅</span>
<span class="hub-pill" style="background:#f5f5f5;border-color:#ccc;color:#888;">10. Frio</span>
<span class="hub-pill" style="background:#f5f5f5;border-color:#ccc;color:#888;">11. Descartado</span>
</div>""", unsafe_allow_html=True)

        st.markdown(_card("""
<div class="hub-card-title red">As 14 Categorias — usar exatamente estas</div>
<div style="font-size:11px;line-height:2;color:#2d2d2d;">
01 — Comentou em post → Engajador → pitch Finder<br>
02 — Curtiu post → Engajador frio → convite de conexão<br>
03 — Compartilhou post → Engajador quente → prioridade<br>
04 — Visitou perfil sem conectar → enviar convite<br>
05 — Visitou Company Page → monitorar e conectar<br>
06 — Pediu conexão para nós → responder em 24h<br>
07 — Nós pedimos conexão → aguardar aceite<br>
08 — Respondeu com interesse → qualificar + WhatsApp<br>
09 — Respondeu sem interesse → pitch Finder<br>
10 — Não respondeu em 7 dias → follow-up único<br>
11 — Não respondeu ao follow-up → Frio<br>
12 — Qualificado → grupo WhatsApp + NCDA<br>
13 — Em negociação ativa → planilha em andamento<br>
14 — Não qualificado → arquivar
</div>
""", "red"), unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 10 — EQUIPE
# ═══════════════════════════════════════════════════════════════════════════════

def _tab_equipe():
    st.markdown('<p style="font-weight:700;color:#FA8200;margin-bottom:14px;">Os 3 Sócios — Autoridade total para fechar negócios</p>', unsafe_allow_html=True)

    socios = [
        ("LB", "Leonardo Brunelli", "Sócio — Eficiência Operacional", "Posts de conteúdo: chão de fábrica, processos, logística, números. Responde dúvidas operacionais e de execução.", "lbd@sambaexport.com.br", "https://www.linkedin.com/in/leonardobrunelli/"),
        ("ND", "Nívio Domingues", "Sócio — Comércio Exterior", "Posts de conteúdo: relações internacionais, destinos de exportação, humanizar a marca. Especialista em regulação e comex.", "nmd@sambaexport.com.br", "https://www.linkedin.com/in/niviodomingues/"),
        ("MM", "Marcelo Magalhães", "Sócio — Visão Estratégica", "Posts de conteúdo: geopolítica, tendências de mercado, agronegócio global. Posicionamento macro da empresa.", "marcelo.magalhaes@sambaexport.com.br", "https://www.linkedin.com/in/marcelomagalhaesn/"),
    ]

    cols = st.columns(3)
    for i, (initials, name, role, desc, email, linkedin) in enumerate(socios):
        with cols[i]:
            st.markdown(f"""<div class="hub-profile-card">
<div class="hub-profile-avatar">{initials}</div>
<div class="hub-profile-name">{name}</div>
<div class="hub-profile-role">{role}</div>
<div class="hub-profile-desc">{desc}</div>
<div class="hub-profile-email">{email}</div>
<br>
<a href="{linkedin}" target="_blank" class="hub-btn-outline" style="font-size:11px;">Ver LinkedIn →</a>
</div>""", unsafe_allow_html=True)

    st.markdown('<br><p style="font-weight:700;color:#FA8200;margin-bottom:12px;">Equipe Operacional — Respondem os Linkedins e operam o CRM</p>', unsafe_allow_html=True)

    c1, c2, c3 = st.columns(3)
    for col, (name, email) in zip([c1, c2, c3], [
        ("Equipe CJS", "cjs@sambaexport.com.br"),
        ("Equipe JRT", "jrt@sambaexport.com.br"),
        ("Equipe PMN", "pmn@sambaexport.com.br"),
    ]):
        with col:
            st.markdown(f'<div class="hub-card"><div style="font-weight:700;font-size:13px;margin-bottom:6px;">{name}</div><div class="hub-profile-email">{email}</div></div>', unsafe_allow_html=True)

    st.markdown('<br><p style="font-weight:700;color:#FA8200;margin-bottom:12px;">Links das Apresentações — Compartilhar no momento certo</p>', unsafe_allow_html=True)

    links = [
        ("Portfolio Global — Todos os produtos", "Compartilhar com compradores qualificados APÓS assinatura do NCDA", "https://www.sambaexport.com.br"),
        ("Programa de Finders", "Compartilhar com Engajadores após pitch (Script G)", "https://www.sambaexport.com.br/finder/"),
        ("CRM + Playbook no Notion", "Hub central da equipe — registrar TODOS os leads aqui", "https://notion.so/34b448b387d38185a8d6f7f179e3e705"),
        ("LinkedIn Company Page — Samba Export", "Página oficial da empresa — monitorar quem visita", "https://www.linkedin.com/company/samba-export/"),
    ]
    for title, sub, url in links:
        st.markdown(f"""<div class="hub-card" style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
<div><strong style="font-size:13px;">{title}</strong><div style="font-size:11px;color:#666;margin-top:2px;">{sub}</div></div>
<a href="{url}" target="_blank" class="hub-btn-orange">ABRIR →</a>
</div>""", unsafe_allow_html=True)

    st.markdown(_card("""
<div class="hub-card-title green">💡 Quem faz o quê — Resumo final</div>
<table class="hub-table">
<tr><th>Quem</th><th>Faz o quê</th></tr>
<tr><td><strong>Equipe (todos)</strong></td><td>Identifica lead, classifica no CRM, envia scripts, filtra perguntas, cria grupo WhatsApp</td></tr>
<tr><td><strong>Leonardo + Nívio + Marcelo</strong></td><td>Confirmam preços, emitem FCO, assinam contratos, fecham negócios</td></tr>
<tr><td><strong>Claude + ChatGPT</strong></td><td>Ajudam a entender o que o lead pediu, sugerem resposta, traduzem — mas nunca definem preço final</td></tr>
<tr><td><strong>ZapSign (sistema)</strong></td><td>Envia NCDA automaticamente quando lead migra para WhatsApp</td></tr>
</table>
""", "green"), unsafe_allow_html=True)

    st.markdown(_card("""
<div class="hub-card-title">Contatos Oficiais Samba Export</div>
<div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-top:8px;">
<div style="background:#f5f5f5;border-radius:6px;padding:10px;"><div style="font-size:10px;color:#666;">Email Comercial</div><div style="font-weight:700;font-family:monospace;font-size:12px;color:#FA8200;">commercial@sambaexport.com.br</div></div>
<div style="background:#f5f5f5;border-radius:6px;padding:10px;"><div style="font-size:10px;color:#666;">WhatsApp</div><div style="font-weight:700;font-family:monospace;font-size:12px;color:#FA8200;">+55 13 991405566</div></div>
<div style="background:#f5f5f5;border-radius:6px;padding:10px;"><div style="font-size:10px;color:#666;">WeChat</div><div style="font-weight:700;font-family:monospace;font-size:12px;color:#FA8200;">SambaExport</div></div>
<div style="background:#f5f5f5;border-radius:6px;padding:10px;"><div style="font-size:10px;color:#666;">CNPJ</div><div style="font-weight:700;font-family:monospace;font-size:12px;color:#FA8200;">60.280.015/0001-82</div></div>
<div style="background:#f5f5f5;border-radius:6px;padding:10px;"><div style="font-size:10px;color:#666;">Endereço</div><div style="font-weight:700;font-size:11px;color:#FA8200;">Av. Faria Lima 1811, Sala 115, SP</div></div>
<div style="background:#f5f5f5;border-radius:6px;padding:10px;"><div style="font-size:10px;color:#666;">Site</div><div style="font-weight:700;font-family:monospace;font-size:12px;color:#FA8200;">www.sambaexport.com.br</div></div>
</div>
"""), unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ═══════════════════════════════════════════════════════════════════════════════

def render_hub_conhecimento():
    import base64 as _b64
    _css_compact = "\n".join(ln for ln in _CSS.splitlines() if ln.strip())
    st.markdown(_css_compact, unsafe_allow_html=True)
    st.markdown('<div class="hub-wrap">', unsafe_allow_html=True)

    # ── Logo real (PNG) ──────────────────────────────────────────────────────
    _logo_tag = ""
    _logo_path = ROOT / "assets" / "logo.png"
    if _logo_path.exists():
        _logo_b64 = _b64.b64encode(_logo_path.read_bytes()).decode()
        _logo_tag = (f'<img src="data:image/png;base64,{_logo_b64}" '
                     f'style="height:40px;width:auto;flex-shrink:0;display:block">')
    else:
        _logo_tag = '<span class="hub-logo"><span>samba</span>EXPORT</span>'

    # ── Page header ──
    c_logo, c_back = st.columns([6, 1])
    with c_logo:
        st.markdown(f"""<div class="hub-page-header">
<div style="display:flex;align-items:center;gap:14px">
{_logo_tag}
<div>
<span class="hub-tag">HUB DA EQUIPE</span>
<span class="hub-tag">v3.0</span>
</div>
</div>
</div>""", unsafe_allow_html=True)
    with c_back:
        if st.button("← Portal", key="hub_back_portal", use_container_width=True):
            st.session_state.current_view = "portal"
            st.rerun()

    # ── Tab navigation ──
    tabs = st.tabs([
        "🏠 Início",
        "🗺️ Mapa de Leads",
        "🔍 Filtro Comprador",
        "💬 Scripts",
        "📦 Exemplos Práticos",
        "❓ FAQ Vendas",
        "🌾 FAQ Commodities",
        "📖 Glossário",
        "📋 Regras CRM",
        "👥 Equipe",
    ])

    with tabs[0]:
        st.markdown('<div class="hub-page-title">Bem-vindo ao Hub da <span>Equipe Samba Export</span></div>', unsafe_allow_html=True)
        st.markdown('<div class="hub-page-sub">Tudo que você precisa para trabalhar no LinkedIn como se fosse o Marcelo, o Nívio ou o Leonardo. Do zero.</div>', unsafe_allow_html=True)
        _tab_inicio()

    with tabs[1]:
        st.markdown('<div class="hub-page-title">🗺️ Mapa de <span>Leads</span></div>', unsafe_allow_html=True)
        st.markdown('<div class="hub-page-sub">Como identificar quem é a pessoa e o que fazer com ela. Simples e direto.</div>', unsafe_allow_html=True)
        _tab_mapa()

    with tabs[2]:
        st.markdown('<div class="hub-page-title">🔍 Filtro do <span>Comprador</span></div>', unsafe_allow_html=True)
        st.markdown('<div class="hub-page-sub">10 perguntas que você precisa responder ANTES de qualquer proposta. Algumas você descobre pelo perfil LinkedIn do lead.</div>', unsafe_allow_html=True)
        _tab_filtro()

    with tabs[3]:
        st.markdown('<div class="hub-page-title">💬 <span>Scripts</span> — Inglês e Português</div>', unsafe_allow_html=True)
        st.markdown('<div class="hub-page-sub">Todos os textos prontos. Clique para abrir. Copiar e colar — nunca improvisar. O português é só para você entender o que está enviando.</div>', unsafe_allow_html=True)
        _tab_scripts()

    with tabs[4]:
        st.markdown('<div class="hub-page-title">📦 Exemplos <span>Práticos</span></div>', unsafe_allow_html=True)
        st.markdown('<div class="hub-page-sub">Como um comprador real escreve, o que ele quer dizer e como responder corretamente.</div>', unsafe_allow_html=True)
        _tab_exemplos()

    with tabs[5]:
        st.markdown('<div class="hub-page-title">❓ FAQ — <span>Vendas</span></div>', unsafe_allow_html=True)
        st.markdown('<div class="hub-page-sub">Dúvidas mais comuns sobre processo, scripts, Programa Finder, WhatsApp e CRM.</div>', unsafe_allow_html=True)
        _tab_faq_vendas()

    with tabs[6]:
        st.markdown('<div class="hub-page-title">🌾 FAQ — <span>Commodities</span></div>', unsafe_allow_html=True)
        st.markdown('<div class="hub-page-sub">O básico que você precisa saber para entender o que os compradores estão pedindo. Sem tecnicismo desnecessário.</div>', unsafe_allow_html=True)
        _tab_faq_commo()

    with tabs[7]:
        st.markdown('<div class="hub-page-title">📖 <span>Glossário</span></div>', unsafe_allow_html=True)
        st.markdown('<div class="hub-page-sub">Todas as siglas e termos que você vai encontrar nas mensagens dos leads. Em ordem alfabética.</div>', unsafe_allow_html=True)
        _tab_glossario()

    with tabs[8]:
        st.markdown('<div class="hub-page-title">📋 Regras do <span>CRM</span></div>', unsafe_allow_html=True)
        st.markdown('<div class="hub-page-sub">Como registrar tudo certo na planilha e no Notion. Sem isso o processo perde qualidade.</div>', unsafe_allow_html=True)
        _tab_regras_crm()

    with tabs[9]:
        st.markdown('<div class="hub-page-title">👥 <span>Equipe</span> e Links</div>', unsafe_allow_html=True)
        st.markdown('<div class="hub-page-sub">Quem é quem, qual é o papel de cada um e onde estão os materiais que você vai precisar.</div>', unsafe_allow_html=True)
        _tab_equipe()

    st.markdown('</div>', unsafe_allow_html=True)
