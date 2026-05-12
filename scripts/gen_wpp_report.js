/**
 * gen_wpp_report.js
 * Gera o relatório "WhatsApp Agents — Configuração e Arquitetura"
 * no padrão visual Samba Export para circulação com sócios e devs.
 *
 * Uso: node scripts/gen_wpp_report.js
 * Saída: docs/SAMBA_WPP_AGENTS_REPORT.docx
 */
"use strict";

const fs   = require("fs");
const path = require("path");

// Resolve global docx package (instalado com npm install -g docx)
const DOCX_PATH = require("path").join(
  process.env.APPDATA || "",
  "npm", "node_modules", "docx"
);
const {
  Document, Packer, Paragraph, TextRun, Table, TableRow, TableCell,
  Header, Footer, PageNumber, AlignmentType, HeadingLevel,
  BorderStyle, WidthType, ShadingType, VerticalAlign,
  PageBreak, LevelFormat, ExternalHyperlink,
  TabStopType, TabStopPosition,
} = require(DOCX_PATH);

// ── Paleta Samba Export ───────────────────────────────────────────────────────
const C = {
  BLACK:       "0D0D10",   // fundo escuro (usado em headers de tabela)
  DARK_CARD:   "14141A",   // cards escuros
  ORANGE:      "FA8200",   // laranja Samba (accent principal)
  ORANGE_MID:  "C86800",   // laranja escuro (subtítulos)
  WHITE:       "F5F5F7",   // texto principal sobre escuro
  SILVER:      "C0C0C8",   // texto secundário
  GRAY_DARK:   "1E1E28",   // linhas alternadas de tabela
  GRAY_MID:    "2E2E3A",   // bordas e separadores
  GRAY_LIGHT:  "3E3E50",   // cabeçalhos de seção
  GREEN:       "2E8B3A",   // status OK
  RED:         "C83232",   // status erro
  AMBER:       "D4A017",   // status aviso
  BLUE_STEEL:  "1E3A5F",   // destaque técnico
  ORANGE_BG:   "2A1800",   // fundo de blocos de código (laranja muito escuro)
};

// ── Helpers de formatação ─────────────────────────────────────────────────────
const px = (pt) => pt * 20;          // pt → half-points (twips)
const dxa = (inch) => inch * 1440;   // inches → DXA

// Borda fina padrão
const borderThin = (color = C.GRAY_MID) => ({
  style: BorderStyle.SINGLE, size: 4, color,
});
const noBorder = () => ({
  style: BorderStyle.NONE, size: 0, color: "FFFFFF",
});
const allBorders = (color = C.GRAY_MID) => ({
  top: borderThin(color), bottom: borderThin(color),
  left: borderThin(color), right: borderThin(color),
});
const noBorders = () => ({
  top: noBorder(), bottom: noBorder(), left: noBorder(), right: noBorder(),
});

// TextRun helper
function tr(text, opts = {}) {
  return new TextRun({
    text,
    font:  opts.font  || "Arial",
    size:  opts.size  || px(10),
    bold:  opts.bold  || false,
    color: opts.color || C.BLACK,
    italics: opts.italic || false,
    ...opts,
  });
}

// Parágrafo simples
function p(runs, opts = {}) {
  const children = Array.isArray(runs) ? runs : [tr(runs, opts)];
  return new Paragraph({
    children,
    spacing:   opts.spacing   || { before: 0, after: 80 },
    alignment: opts.alignment || AlignmentType.LEFT,
    ...opts,
  });
}

// Parágrafo em branco
const blank = (pts = 4) => p("", { spacing: { before: 0, after: px(pts) } });

// ── Seção de título (cover) ───────────────────────────────────────────────────
function coverTitle(text) {
  return new Paragraph({
    children: [tr(text, { size: px(28), bold: true, color: C.ORANGE })],
    spacing:  { before: px(8), after: px(6) },
    alignment: AlignmentType.LEFT,
  });
}

function coverSub(text) {
  return new Paragraph({
    children: [tr(text, { size: px(13), color: C.SILVER, italic: true })],
    spacing:  { before: 0, after: px(4) },
  });
}

// ── Cabeçalhos de seção ───────────────────────────────────────────────────────
function h1(text, number) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_1,
    children: [
      tr(`${number}. `, { size: px(14), bold: true, color: C.ORANGE }),
      tr(text,          { size: px(14), bold: true, color: C.WHITE }),
    ],
    spacing:  { before: px(16), after: px(6) },
    border: {
      bottom: { style: BorderStyle.SINGLE, size: 6, color: C.ORANGE, space: 2 },
    },
  });
}

function h2(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_2,
    children: [tr(text, { size: px(11.5), bold: true, color: C.ORANGE_MID })],
    spacing:  { before: px(10), after: px(3) },
  });
}

function h3(text) {
  return new Paragraph({
    heading: HeadingLevel.HEADING_3,
    children: [tr(text, { size: px(10.5), bold: true, color: C.SILVER })],
    spacing:  { before: px(6), after: px(2) },
  });
}

// ── Corpo de texto ────────────────────────────────────────────────────────────
function body(text, color = "000000") {
  return new Paragraph({
    children: [tr(text, { size: px(10), color })],
    spacing: { before: 0, after: px(3) },
  });
}

function bodyMixed(runs) {
  return new Paragraph({
    children: runs,
    spacing:  { before: 0, after: px(3) },
  });
}

// ── Bullet (sem emoji unicode) ────────────────────────────────────────────────
function bullet(text, level = 0, color = "000000") {
  return new Paragraph({
    numbering: { reference: "bullets", level },
    children:  [tr(text, { size: px(10), color })],
    spacing:   { before: 0, after: px(2) },
  });
}

// ── Bloco de código / monospace ───────────────────────────────────────────────
function code(text) {
  return new Paragraph({
    children: [tr(text, { size: px(8.5), font: "Courier New", color: C.ORANGE })],
    spacing:  { before: 0, after: 0 },
    indent:   { left: dxa(0.15) },
  });
}

function codeBlock(lines) {
  const rows = lines.map(line => new TableRow({
    children: [new TableCell({
      borders: noBorders(),
      shading: { fill: "1A1000", type: ShadingType.CLEAR },
      margins: { top: 40, bottom: 0, left: 160, right: 80 },
      width:   { size: 9360, type: WidthType.DXA },
      children: [new Paragraph({
        children: [tr(line || " ", { size: px(8.5), font: "Courier New", color: C.ORANGE })],
        spacing: { before: 0, after: 0 },
      })],
    })],
  }));

  return new Table({
    width: { size: 9360, type: WidthType.DXA },
    columnWidths: [9360],
    borders: {
      top:    { style: BorderStyle.SINGLE, size: 4, color: C.ORANGE_MID },
      bottom: { style: BorderStyle.SINGLE, size: 4, color: C.ORANGE_MID },
      left:   { style: BorderStyle.SINGLE, size: 8, color: C.ORANGE },
      right:  { style: BorderStyle.NONE,   size: 0, color: "FFFFFF" },
    },
    rows,
  });
}

// ── Tabela de dados ───────────────────────────────────────────────────────────
function dataTable(headers, rows, colWidths) {
  const total = colWidths.reduce((a, b) => a + b, 0);

  const headerRow = new TableRow({
    tableHeader: true,
    children: headers.map((h, i) => new TableCell({
      width:   { size: colWidths[i], type: WidthType.DXA },
      borders: allBorders(C.ORANGE_MID),
      shading: { fill: C.BLACK, type: ShadingType.CLEAR },
      margins: { top: 80, bottom: 80, left: 140, right: 80 },
      verticalAlign: VerticalAlign.CENTER,
      children: [new Paragraph({
        children:  [tr(h, { size: px(9), bold: true, color: C.ORANGE })],
        alignment: AlignmentType.CENTER,
        spacing:   { before: 0, after: 0 },
      })],
    })),
  });

  const dataRows = rows.map((row, ri) => new TableRow({
    children: row.map((cell, ci) => {
      const isStatus = typeof cell === "string" && (
        cell.startsWith("OK") || cell.startsWith("Ativo")
      );
      const isError = typeof cell === "string" && (
        cell.startsWith("Preencher") || cell.startsWith("Configurar")
      );
      const isWarn = typeof cell === "string" && cell.startsWith("Mudar");
      const color = isStatus ? C.GREEN : isError ? C.RED : isWarn ? C.AMBER : "111111";
      const fillColor = ri % 2 === 0 ? "F9F6F0" : "FFFFFF";

      return new TableCell({
        width:   { size: colWidths[ci], type: WidthType.DXA },
        borders: allBorders(C.GRAY_MID),
        shading: { fill: fillColor, type: ShadingType.CLEAR },
        margins: { top: 60, bottom: 60, left: 140, right: 80 },
        verticalAlign: VerticalAlign.CENTER,
        children: [new Paragraph({
          children:  [tr(String(cell), { size: px(9), bold: isStatus || isError, color })],
          alignment: ci > 0 ? AlignmentType.CENTER : AlignmentType.LEFT,
          spacing:   { before: 0, after: 0 },
        })],
      });
    }),
  }));

  return new Table({
    width: { size: total, type: WidthType.DXA },
    columnWidths: colWidths,
    rows: [headerRow, ...dataRows],
  });
}

// ── Caixa de destaque (info / aviso / perigo) ─────────────────────────────────
function alertBox(label, text, accentColor = C.ORANGE) {
  return new Table({
    width: { size: 9360, type: WidthType.DXA },
    columnWidths: [9360],
    borders: {
      top:    { style: BorderStyle.SINGLE, size: 4, color: accentColor },
      bottom: { style: BorderStyle.SINGLE, size: 4, color: accentColor },
      left:   { style: BorderStyle.SINGLE, size: 12, color: accentColor },
      right:  { style: BorderStyle.NONE,   size: 0,  color: "FFFFFF" },
    },
    rows: [new TableRow({ children: [new TableCell({
      borders: noBorders(),
      shading: { fill: "FFF8F0", type: ShadingType.CLEAR },
      margins: { top: 80, bottom: 80, left: 160, right: 80 },
      width:   { size: 9360, type: WidthType.DXA },
      children: [
        new Paragraph({
          children: [tr(label, { size: px(9), bold: true, color: accentColor })],
          spacing: { before: 0, after: px(2) },
        }),
        new Paragraph({
          children: [tr(text, { size: px(9.5), color: "333333" })],
          spacing: { before: 0, after: 0 },
        }),
      ],
    })]})] ,
  });
}

// ── Separador de agente (card laranja) ───────────────────────────────────────
function agentCard(emoji, name, number, role, canReply) {
  return new Table({
    width: { size: 9360, type: WidthType.DXA },
    columnWidths: [9360],
    rows: [new TableRow({ children: [new TableCell({
      borders: {
        top:    borderThin(C.ORANGE),
        bottom: borderThin(C.ORANGE),
        left:   { style: BorderStyle.SINGLE, size: 16, color: C.ORANGE },
        right:  borderThin(C.ORANGE),
      },
      shading: { fill: "1A0E00", type: ShadingType.CLEAR },
      margins: { top: 100, bottom: 100, left: 200, right: 100 },
      width:   { size: 9360, type: WidthType.DXA },
      children: [
        new Paragraph({
          children: [
            tr(`${emoji}  ${name}  `, { size: px(12), bold: true, color: C.ORANGE }),
            tr(`(${number})`, { size: px(10), bold: false, color: C.SILVER }),
          ],
          spacing: { before: 0, after: px(2) },
        }),
        new Paragraph({
          children: [
            tr(`Role: `, { size: px(9), bold: true, color: C.SILVER }),
            tr(role, { size: px(9), color: C.WHITE }),
            tr("   |   Envio: ", { size: px(9), bold: true, color: C.SILVER }),
            tr(canReply ? "SIM (can_reply=True)" : "NAO — apenas leitura", {
              size: px(9), bold: true, color: canReply ? C.GREEN : C.RED,
            }),
          ],
          spacing: { before: 0, after: 0 },
        }),
      ],
    })]})] ,
  });
}

// ── Checklist ─────────────────────────────────────────────────────────────────
function checkItem(done, text) {
  const box  = done ? "[OK]" : "[ ]";
  const col  = done ? C.GREEN : "333333";
  const bold = done;
  return new Paragraph({
    numbering: { reference: "checklist", level: 0 },
    children: [
      tr(`${box}  `, { size: px(9), bold: true, color: col, font: "Courier New" }),
      tr(text, { size: px(9.5), bold, color: done ? "111111" : "333333" }),
    ],
    spacing: { before: 0, after: px(3) },
  });
}

// ── Linha de status (para tabela de estado atual) ─────────────────────────────
function statusRow(label, status, detail) {
  const isOk   = status.startsWith("OK") || status.startsWith("Ativo");
  const isErr  = status.startsWith("Preencher") || status.startsWith("Nao") || status.startsWith("Nenhum");
  const isWarn = status.startsWith("Mudar") || status.startsWith("Aguarda") || status.startsWith("Verificar");
  const icon   = isOk ? "  OK  " : isErr ? "  XX  " : "  !!  ";
  const col    = isOk ? C.GREEN  : isErr ? C.RED    : C.AMBER;

  return [label, icon + status, detail || ""];
}

// =============================================================================
// CONTEÚDO DO DOCUMENTO
// =============================================================================

const PAGE_W  = 12240;  // 8.5" (Letter)
const PAGE_H  = 15840;  // 11"
const MARGIN  = 1080;   // 0.75" margens (mais espaço para conteúdo técnico)
const CONTENT = PAGE_W - 2 * MARGIN; // 9360 DXA

const TODAY = new Date().toLocaleDateString("pt-BR", {
  day: "2-digit", month: "long", year: "numeric",
});

// =============================================================================
const doc = new Document({
  // ── Estilos globais ──────────────────────────────────────────────────────
  styles: {
    default: {
      document: { run: { font: "Arial", size: px(10), color: "111111" } },
    },
    paragraphStyles: [
      {
        id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Normal",
        run:       { size: px(14), bold: true, font: "Arial", color: C.WHITE },
        paragraph: { spacing: { before: px(16), after: px(6) }, outlineLevel: 0 },
      },
      {
        id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Normal",
        run:       { size: px(11.5), bold: true, font: "Arial", color: C.ORANGE_MID },
        paragraph: { spacing: { before: px(10), after: px(3) }, outlineLevel: 1 },
      },
      {
        id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Normal",
        run:       { size: px(10.5), bold: true, font: "Arial", color: C.SILVER },
        paragraph: { spacing: { before: px(6), after: px(2) }, outlineLevel: 2 },
      },
    ],
  },

  // ── Listas ───────────────────────────────────────────────────────────────
  numbering: {
    config: [
      {
        reference: "bullets",
        levels: [{
          level: 0, format: LevelFormat.BULLET, text: "•",
          alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: dxa(0.3), hanging: dxa(0.15) } } },
        }, {
          level: 1, format: LevelFormat.BULLET, text: "◦",
          alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: dxa(0.55), hanging: dxa(0.15) } } },
        }],
      },
      {
        reference: "numbered",
        levels: [{
          level: 0, format: LevelFormat.DECIMAL, text: "%1.",
          alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: dxa(0.35), hanging: dxa(0.2) } } },
        }],
      },
      {
        reference: "checklist",
        levels: [{
          level: 0, format: LevelFormat.BULLET, text: "",
          alignment: AlignmentType.LEFT,
          style: { paragraph: { indent: { left: dxa(0.1), hanging: 0 } } },
        }],
      },
    ],
  },

  // ── Seções do documento ──────────────────────────────────────────────────
  sections: [

    // ===================================================================
    // CAPA
    // ===================================================================
    {
      properties: {
        page: {
          size:   { width: PAGE_W, height: PAGE_H },
          margin: { top: MARGIN, right: MARGIN, bottom: MARGIN, left: MARGIN },
        },
      },
      headers: {
        default: new Header({ children: [
          new Paragraph({
            children: [
              tr("SAMBA EXPORT CONTROL DESK", { size: px(8), bold: true, color: C.ORANGE }),
              tr("   |   Documento Interno   |   Confidencial", { size: px(8), color: C.SILVER }),
            ],
            border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: C.ORANGE, space: 2 } },
            spacing: { before: 0, after: px(4) },
          }),
        ]}),
      },
      footers: {
        default: new Footer({ children: [
          new Paragraph({
            children: [
              tr("Samba Export — Documento Interno Confidencial   |   Pagina ", { size: px(8), color: C.SILVER }),
              new TextRun({ children: [PageNumber.CURRENT], size: px(8), color: C.SILVER }),
              tr(" de ", { size: px(8), color: C.SILVER }),
              new TextRun({ children: [PageNumber.TOTAL_PAGES], size: px(8), color: C.SILVER }),
            ],
            border: { top: { style: BorderStyle.SINGLE, size: 4, color: C.ORANGE, space: 2 } },
            spacing: { before: px(4), after: 0 },
            alignment: AlignmentType.RIGHT,
          }),
        ]}),
      },
      children: [

        // Faixa laranja de topo da capa
        new Table({
          width: { size: CONTENT, type: WidthType.DXA },
          columnWidths: [CONTENT],
          rows: [new TableRow({ children: [new TableCell({
            width: { size: CONTENT, type: WidthType.DXA },
            borders: noBorders(),
            shading: { fill: C.ORANGE, type: ShadingType.CLEAR },
            margins: { top: 200, bottom: 200, left: 300, right: 200 },
            children: [
              new Paragraph({
                children: [tr("SAMBA EXPORT CONTROL DESK", { size: px(11), bold: true, color: "FFFFFF" })],
                spacing: { before: 0, after: px(2) },
              }),
              new Paragraph({
                children: [tr("Sistema de Agentes de Inteligencia Comercial", { size: px(9), color: "FFE0B0" })],
                spacing: { before: 0, after: 0 },
              }),
            ],
          })]})] ,
        }),

        blank(20),

        // Titulo principal
        new Paragraph({
          children: [tr("Agentes WhatsApp", { size: px(36), bold: true, color: C.ORANGE })],
          spacing:  { before: 0, after: px(4) },
        }),
        new Paragraph({
          children: [tr("Arquitetura, Configuracao e Ativacao", { size: px(20), bold: false, color: "333333" })],
          spacing:  { before: 0, after: px(6) },
        }),
        new Paragraph({
          children: [tr("Numero operacional: ", { size: px(14), color: "555555" }),
                     tr("+55 13 99140-5566", { size: px(14), bold: true, color: C.ORANGE })],
          spacing:  { before: 0, after: px(24) },
        }),

        // Linha divisoria
        new Paragraph({
          border: { bottom: { style: BorderStyle.SINGLE, size: 6, color: C.ORANGE, space: 1 } },
          spacing: { before: 0, after: px(18) },
          children: [],
        }),

        // Metadados
        new Table({
          width: { size: CONTENT, type: WidthType.DXA },
          columnWidths: [2000, 7360],
          rows: [
            ...[
              ["Data:", TODAY],
              ["Versao:", "1.0 — Documento de circulacao interna"],
              ["Classificacao:", "Confidencial — Socios e Desenvolvedores"],
              ["Numero WPP:", "+5513991405566 (chip unico consolidado)"],
              ["Status:", "Sistema implantado — aguardando credenciais Twilio"],
              ["Repositorio:", "SAMBA_MANAGER / SAMBA_AGENTS"],
            ].map(([k, v]) => new TableRow({ children: [
              new TableCell({
                width: { size: 2000, type: WidthType.DXA }, borders: noBorders(),
                margins: { top: 40, bottom: 40, left: 0, right: 60 },
                children: [new Paragraph({ children: [tr(k, { size: px(9.5), bold: true, color: "555555" })], spacing: { before: 0, after: px(2) } })],
              }),
              new TableCell({
                width: { size: 7360, type: WidthType.DXA }, borders: noBorders(),
                margins: { top: 40, bottom: 40, left: 60, right: 0 },
                children: [new Paragraph({ children: [tr(v, { size: px(9.5), color: "111111" })], spacing: { before: 0, after: px(2) } })],
              }),
            ]})),
          ],
        }),

        blank(18),

        // Sumario rapido
        new Table({
          width: { size: CONTENT, type: WidthType.DXA },
          columnWidths: [CONTENT],
          rows: [new TableRow({ children: [new TableCell({
            borders: allBorders(C.ORANGE_MID),
            shading: { fill: "FFF8F0", type: ShadingType.CLEAR },
            margins: { top: 120, bottom: 120, left: 200, right: 200 },
            width:   { size: CONTENT, type: WidthType.DXA },
            children: [
              new Paragraph({ children: [tr("SUMARIO EXECUTIVO", { size: px(10), bold: true, color: C.ORANGE })], spacing: { before: 0, after: px(6) } }),
              ...[
                "1. Arquitetura geral do sistema",
                "2. Os 5 agentes e como interagem",
                "3. Os 3 grupos WhatsApp internos e roteamento",
                "4. Configuracao Twilio — passo a passo completo",
                "5. Configuracao Meta Business Manager",
                "6. Infraestrutura — servidor e tunel HTTPS",
                "7. Checklist de ativacao — na ordem certa",
                "8. Estado atual do sistema",
              ].map(line => new Paragraph({
                children: [tr(line, { size: px(9.5), color: "333333" })],
                spacing:  { before: 0, after: px(3) },
              })),
            ],
          })]})] ,
        }),

        // Quebra de pagina para secao 1
        new Paragraph({ children: [new PageBreak()] }),
      ],
    },

    // ===================================================================
    // CONTEUDO PRINCIPAL
    // ===================================================================
    {
      properties: {
        page: {
          size:   { width: PAGE_W, height: PAGE_H },
          margin: { top: MARGIN, right: MARGIN, bottom: MARGIN, left: MARGIN },
        },
      },
      headers: {
        default: new Header({ children: [
          new Paragraph({
            children: [
              tr("SAMBA EXPORT — Agentes WhatsApp   |   Confidencial", { size: px(8), bold: true, color: C.ORANGE }),
            ],
            border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: C.ORANGE, space: 2 } },
            spacing: { before: 0, after: px(4) },
            tabStops: [{ type: TabStopType.RIGHT, position: TabStopPosition.MAX }],
          }),
        ]}),
      },
      footers: {
        default: new Footer({ children: [
          new Paragraph({
            children: [
              tr("Samba Export — Documento Interno Confidencial   |   Pagina ", { size: px(8), color: C.SILVER }),
              new TextRun({ children: [PageNumber.CURRENT], size: px(8), color: C.SILVER }),
              tr(" de ", { size: px(8), color: C.SILVER }),
              new TextRun({ children: [PageNumber.TOTAL_PAGES], size: px(8), color: C.SILVER }),
            ],
            border: { top: { style: BorderStyle.SINGLE, size: 4, color: C.ORANGE, space: 2 } },
            spacing: { before: px(4), after: 0 },
            alignment: AlignmentType.RIGHT,
          }),
        ]}),
      },
      children: [

        // =================================================================
        // SECAO 1 — ARQUITETURA GERAL
        // =================================================================
        h1("ARQUITETURA GERAL DO SISTEMA", "1"),
        blank(2),
        body("O numero +5513991405566 e membro de todos os grupos operacionais da Samba Export — tanto grupos de clientes/parceiros quanto os 3 grupos internos de operacao. Cada mensagem recebida segue o seguinte pipeline:"),
        blank(4),

        codeBlock([
          "WhatsApp Business (grupos clientes + grupos internos Samba)",
          "          |",
          "          |  mensagem recebida (Twilio entrega via HTTPS POST)",
          "          v",
          "    /webhook/twilio  (FastAPI — responde em < 100ms)",
          "          |",
          "     identifica canal: cliente | mailbox | drive | tasks",
          "          |",
          "    +------+----------+----------+",
          "    v      v          v          v",
          " Extractor  FollowUp  Router IA  Audio ATA",
          " (Celery)   (resposta)(@mention)  (Gemini)",
          "    |",
          "    v",
          " SQLite DB --> Google Sheets --> Google Drive",
        ]),

        blank(6),
        h3("Componentes de infraestrutura"),
        blank(2),
        dataTable(
          ["Componente", "Funcao", "Tecnologia"],
          [
            ["FastAPI", "Webhook Twilio — porta 8000", "Python / uvicorn"],
            ["Celery", "Workers assincronos para agentes", "Python / Redis"],
            ["Redis", "Broker de filas Celery + Beat", "Redis 7+"],
            ["SQLite", "Banco principal (deals, follow-ups, conversas)", "SQLAlchemy ORM"],
            ["Gemini API", "Inteligencia dos agentes (Flash + Pro)", "Google Cloud / Billing ativo"],
            ["Google Sheets", "Pipeline comercial e planilha de deals", "Google Workspace"],
            ["Google Drive", "Documentos RAG e arquivos corporativos", "Google Workspace"],
            ["Twilio", "API WhatsApp Business (envio e recepcao)", "Twilio Messaging API"],
          ],
          [2400, 3480, 3480],
        ),

        blank(8),

        // =================================================================
        // SECAO 2 — OS 5 AGENTES
        // =================================================================
        h1("OS 5 AGENTES — FUNCOES E INTERACOES", "2"),
        blank(4),

        // --- Extractor ---
        agentCard("1.", "Agente Extractor", "+5513991405566", "AgentRole.EXTRACTOR", false),
        blank(3),
        body("O Extractor e o minerador de dados comerciais. Opera em modo somente-leitura — NUNCA envia mensagens. Captura qualquer mensagem de qualquer grupo e extrai inteligencia comercial:"),
        blank(2),
        bullet("Commodity, volume (MT/sacas), preco (USD/MT), incoterm, origem, destino, parceiro"),
        bullet("Cria ou atualiza Deal no banco com stage = 'Lead Capturado'"),
        bullet("Sincroniza automaticamente para a planilha Google Sheets 'todos andamento'"),
        bullet("Se campos criticos faltam: dispara alerta por email E WhatsApp ao socio responsavel"),
        blank(4),
        h3("Fluxo de ativacao:"),
        blank(2),
        codeBlock([
          "Mensagem chega no grupo",
          "  --> persist_inbound_message()  -->  Message.id no banco",
          "  --> task_process_inbound_message(msg_id)    [Celery]",
          "  --> task_extract_message()  -->  ExtractorAgent.process_single_message()",
          "  --> Deal criado/atualizado no SQLite",
          "  --> task_sync_spreadsheet_to_drive()  -->  Google Sheets atualizado",
        ]),
        blank(8),

        // --- Follow-Up ---
        agentCard("2.", "Agente Follow-Up", "+5513991405566", "AgentRole.FOLLOWUP", true),
        blank(3),
        body("O cobrador inteligente da Samba. Ciclo automatico a cada 15 minutos via Celery Beat. Implementa uma cadencia de 3 tentativas com tom progressivamente mais firme:"),
        blank(4),

        dataTable(
          ["Tentativa", "Dias Vencido", "Tom", "Acao do Sistema"],
          [
            ["1a tentativa", "0 a 2 dias", "Casual — so checando", "Envia direto ao parceiro"],
            ["2a tentativa", "3 a 6 dias", "Firme — cita commodity e volume", "Envia direto ao parceiro"],
            ["3a tentativa", "7+ dias", "Critico — janela fecha hoje", "Cria PendingApproval — aguarda aprovacao humana"],
          ],
          [1600, 1600, 3000, 3160],
        ),

        blank(4),
        body("Com WHATSAPP_OFFLINE=true (modo atual): email ao socio responsavel com a mensagem pronta para copiar e colar no WhatsApp manualmente."),
        body("Com WHATSAPP_OFFLINE=false (producao): envia via Twilio diretamente ao parceiro externo."),
        blank(4),
        h3("Fluxo de resposta recebida:"),
        blank(2),
        codeBlock([
          "Parceiro responde pelo WhatsApp",
          "  --> webhook detecta: match_followup_response(sender)",
          "  --> FollowUp.response_received = True  (banco)",
          "  --> task_process_followup_response()  [Celery]",
          "  --> Deal avanca para 'Em Negociacao'",
          "  --> Alerta enviado ao grupo SAMBA AGENTS TASKS FUP",
        ]),
        blank(8),

        // --- Manager ---
        agentCard("3.", "Agente Manager", "+5513991405566", "AgentRole.MANAGER", true),
        blank(3),
        body("O cerebro estrategico. Roda diariamente e envia briefing executivo para todos os socios:"),
        blank(2),
        bullet("Le todos os deals ativos e classifica como COMPRA ou VENDA"),
        bullet("Cruza vendedores x compradores da mesma commodity — detecta arbitragem"),
        bullet("Persiste as oportunidades de arbitragem nas notas dos deals com tag [MATCH DD/MM/YYYY]"),
        bullet("Gera briefing executivo via Gemini Pro (pipeline + matches + alertas de risco)"),
        bullet("Envia o briefing aos socios via WhatsApp e email"),
        blank(4),
        h3("Exemplo de match detectado e persistido:"),
        blank(2),
        codeBlock([
          "Deal #12 (vendedor soja) — campo notes atualizado:",
          "",
          "[MATCH 12/05/2026] SOJA spread USD 12,50/MT |",
          "Venda 447,00 x Compra 459,50 | Contraparte: BRAKO Korea (VENDEDOR)",
        ]),
        blank(8),

        // --- Router IA ---
        agentCard("4.", "Intelligence Router (@mention)", "+5513991405566", "Cascade 5 niveis", true),
        blank(3),
        body("Ativado por qualquer mensagem contendo @samba, @agente, @ia ou @bot em qualquer grupo. Responde no proprio grupo onde a mencao foi feita. Possui memoria das ultimas 4 trocas por usuario."),
        blank(4),
        dataTable(
          ["Nivel", "Nome", "Funcao", "Custo API"],
          [
            ["L0", "Intent Parser", "Gemini Flash classifica a intencao da pergunta", "Minimo"],
            ["L1", "DB Direct", "SQL direto no SQLite — deals, follow-ups, precos, atas", "Zero (local)"],
            ["L2", "RAG Search", "Busca vetorial nos documentos do Drive", "Embedding API"],
            ["L3", "Gemini Flash", "Raciocinio com contexto + historico de conversa", "Baixo"],
            ["L4", "Gemini Pro", "Raciocinio profundo se Flash < 75% de confianca", "Medio"],
            ["L5", "Honest Fallback", "Nao encontrei — NUNCA inventa dados", "Zero"],
          ],
          [700, 1800, 4360, 2500],
        ),
        blank(4),
        alertBox(
          "REGRA FUNDAMENTAL — Facts Only",
          "O router NUNCA inventa precos, datas, nomes, volumes ou qualquer dado numerico. " +
          "Toda resposta e baseada exclusivamente no contexto recuperado do banco de dados ou documentos. " +
          "Se a informacao nao existe no sistema, o agente responde honestamente que nao encontrou.",
          C.BLUE_STEEL,
        ),
        blank(8),

        // --- Documental / Enrichment ---
        agentCard("5.", "Agente Documental e Enriquecimento", "+5513991405566", "AgentRole.DOCUMENTAL", true),
        blank(3),
        bullet("Preenche celulas em branco na planilha 'todos andamento' com dados extraidos das conversas"),
        bullet("Base de conhecimento dinamica: 21 JOBs estaticos + deals novos lidos automaticamente do banco"),
        bullet("Regra absoluta: NUNCA sobrescreve celula com conteudo existente"),
        bullet("Tambem audita documentos (LOI, ICPO, FCO, SPA) contra padrao ICC/UCP600"),
        blank(12),

        // =================================================================
        // SECAO 3 — GRUPOS INTERNOS
        // =================================================================
        h1("OS 3 GRUPOS WHATSAPP INTERNOS", "3"),
        blank(4),
        body("O numero +5513991405566 e membro dos 3 grupos operacionais internos. O webhook identifica automaticamente de qual grupo vem cada mensagem e roteia de forma apropriada:"),
        blank(6),

        dataTable(
          ["Grupo", "Funcao", "Alertas recebidos neste grupo"],
          [
            ["SAMBA AGENTS MAIL BOX", "Inbox / email / documentos externos", "Documentos recebidos, emails importantes, alertas de inbox"],
            ["SAMBA AGENTS GOOGLE DRIVE", "Drive RAG e documentos", "Novos arquivos indexados, atualizacoes de base de conhecimento"],
            ["SAMBA AGENTS TASKS FUP", "Follow-ups e pipeline", "Deals incompletos, follow-ups vencidos, respostas recebidas, escalacoes, alertas intraday"],
          ],
          [2800, 2560, 4000],
        ),

        blank(6),
        h3("Como o sistema roteia as mensagens:"),
        blank(2),
        codeBlock([
          "GroupName = 'SAMBA AGENTS MAIL BOX'      -->  channel = 'mailbox'",
          "GroupName = 'SAMBA AGENTS GOOGLE DRIVE'  -->  channel = 'drive'",
          "GroupName = 'SAMBA AGENTS TASKS FUP'     -->  channel = 'tasks'",
          "qualquer outro grupo                      -->  channel = 'external' (cliente)",
        ]),
        blank(4),
        h3("Protecoes ativas para grupos internos:"),
        blank(2),
        bullet("Mensagens de grupos internos NAO disparam follow-up response matching"),
        bullet("Grupos internos NAO disparam mensagem de boas-vindas de 'Samba x Cliente'"),
        bullet("O Extractor NAO cria deals a partir de mensagens internas"),
        bullet("@mention em grupos internos responde no proprio grupo (contexto interno)"),
        blank(12),

        // =================================================================
        // SECAO 4 — CONFIGURACAO TWILIO
        // =================================================================
        h1("CONFIGURACAO TWILIO — PASSO A PASSO", "4"),
        blank(4),

        h2("4.1  Obter credenciais"),
        blank(2),
        body("Acesse: https://console.twilio.com  ->  Dashboard principal"),
        blank(2),
        codeBlock([
          "Account SID  -->  copiar (comeca com AC...)",
          "Auth Token   -->  clicar no olho para revelar e copiar",
        ]),
        blank(3),
        body("Inserir no arquivo .env:"),
        blank(2),
        codeBlock([
          "TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
          "TWILIO_AUTH_TOKEN=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        ]),
        blank(6),

        h2("4.2  Habilitar WhatsApp no numero"),
        blank(2),
        body("No Twilio Console:"),
        blank(2),
        codeBlock([
          "Messaging  -->  Senders  -->  WhatsApp Senders  -->  Add Sender",
          "Tipo:   Business Profile  (NAO usar Sandbox — e producao)",
          "Numero: +5513991405566",
        ]),
        blank(4),
        alertBox(
          "ATENCAO — Conta WhatsApp Business obrigatoria",
          "Se o numero +5513991405566 ainda nao for uma conta WhatsApp Business, " +
          "e necessario registrar via Meta Business Manager antes de conectar ao Twilio. " +
          "Ver Secao 5 deste documento.",
          C.AMBER,
        ),
        blank(6),

        h2("4.3  Configurar o Webhook"),
        blank(2),
        codeBlock([
          "Console  -->  Messaging  -->  Senders  -->  WhatsApp Senders",
          "  -->  selecione o numero +5513991405566",
          "  -->  campo 'A MESSAGE COMES IN':",
          "       Webhook URL:  https://SEU-DOMINIO/webhook/twilio",
          "       Metodo:       HTTP POST",
          "  -->  Salvar",
        ]),
        blank(3),
        body("Inserir no .env:"),
        blank(2),
        codeBlock([
          "TWILIO_WEBHOOK_PUBLIC_URL=https://SEU-DOMINIO/webhook/twilio",
          "TWILIO_VALIDATE_SIGNATURE=true",
        ]),
        blank(6),

        h2("4.4  Descobrir os IDs dos grupos internos"),
        blank(2),
        body("Apos o webhook estar configurado, peca a qualquer membro dos 3 grupos internos que envie qualquer mensagem. O Twilio enviara o payload para o webhook com o ID do grupo no campo 'From':"),
        blank(2),
        codeBlock([
          "No log do servidor, voce vera:",
          "",
          "webhook_twilio: group='SAMBA AGENTS TASKS FUP' channel=tasks",
          "                sender=+5513XXXXXXX-XXXXXXXXXX@g.us",
          "",
          "Ou no Twilio Console:  Monitor  -->  Logs  -->  Messaging",
          "  campo 'From': whatsapp:+XXXXXXXXX-XXXXXXXXXX@g.us",
          "  (o valor apos 'whatsapp:' e o ID do grupo)",
        ]),
        blank(3),
        body("Preencher no .env com os IDs descobertos:"),
        blank(2),
        codeBlock([
          "WPP_GROUP_MAILBOX_ID=+XXXXXXXXX-XXXXXXXXXX@g.us",
          "WPP_GROUP_DRIVE_ID=+XXXXXXXXX-XXXXXXXXXX@g.us",
          "WPP_GROUP_TASKS_FUP_ID=+XXXXXXXXX-XXXXXXXXXX@g.us",
        ]),
        blank(6),

        h2("4.5  Ativar envios reais e testar"),
        blank(2),
        codeBlock([
          "# Mudar no .env:",
          "WHATSAPP_OFFLINE=false",
          "",
          "# Rodar diagnostico completo (nao envia nada):",
          "python scripts/wpp_smoke_test.py",
          "",
          "# Teste de loopback — envia mensagem real para o proprio numero:",
          "python scripts/wpp_smoke_test.py --send",
          "",
          "# Envia para numero especifico (ex: Leonardo):",
          "python scripts/wpp_smoke_test.py --send --to +5513996259995",
        ]),
        blank(4),
        h3("Saida esperada do smoke test quando tudo estiver OK:"),
        blank(2),
        codeBlock([
          "[OK]  SAMBA_WPP_MAIN configurado  -->  +5513991405566",
          "[OK]  TWILIO_ACCOUNT_SID          -->  ACxxxx***",
          "[OK]  TWILIO_AUTH_TOKEN           -->  ***xxxx",
          "[!!]  WHATSAPP_OFFLINE            -->  INATIVO — envios reais habilitados",
          "[OK]  twilio SDK instalado        -->  v9.x.x",
          "[OK]  Conta Twilio acessivel      -->  Samba Export [active]",
          "[OK]  Extractor    (AGENT_EXTRACTOR_PHONE)   -->  +5513991405566",
          "[OK]  Follow-Up    (AGENT_FOLLOWUP_PHONE)    -->  +5513991405566",
          "[OK]  Manager      (AGENT_MANAGER_PHONE)     -->  +5513991405566",
          "[OK]  Documental   (AGENT_DOCUMENTAL_PHONE)  -->  +5513991405566",
          "[OK]  Agenda       (AGENT_AGENDA_PHONE)      -->  +5513991405566",
        ]),
        blank(12),

        // =================================================================
        // SECAO 5 — META BUSINESS MANAGER
        // =================================================================
        h1("CONFIGURACAO META BUSINESS MANAGER", "5"),
        blank(4),
        alertBox(
          "APENAS se o numero ainda nao for WhatsApp Business",
          "Se +5513991405566 ja esta registrado como WhatsApp Business e tem o app WA Business instalado, " +
          "a verificacao da Meta pode nao ser necessaria para o Twilio Sandbox. Para producao completa, " +
          "a verificacao e obrigatoria.",
          C.BLUE_STEEL,
        ),
        blank(6),

        h2("5.1  Verificar o negocio"),
        blank(2),
        bullet("Acesse business.facebook.com  -->  Configuracoes do Negocio  -->  Verificacao do Negocio"),
        bullet("Enviar: CNPJ da Samba Export, comprovante de enderco comercial"),
        bullet("Prazo: 24 a 72 horas para aprovacao"),
        blank(6),

        h2("5.2  Criar conta WhatsApp Business"),
        blank(2),
        codeBlock([
          "Configuracoes do Negocio  -->  Contas WhatsApp  -->  Adicionar",
          "Nome de exibicao:  Samba Export",
          "Categoria:         Financas e Servicos Financeiros (ou B2B)",
          "Descricao:         Trading de commodities agricolas",
        ]),
        blank(6),

        h2("5.3  Conectar o numero ao Twilio"),
        blank(2),
        bullet("No Twilio Console: Messaging  -->  Senders  -->  WhatsApp Senders  -->  Request Access"),
        bullet("Escolher: 'Use your own number'"),
        bullet("O Twilio fornece um codigo de verificacao — inserir no WhatsApp do +5513991405566"),
        bullet("Aguardar aprovacao da Meta (de minutos a 24h)"),
        blank(6),

        h2("5.4  Templates de mensagens (para primeiro contato proativo)"),
        blank(2),
        body("Para mensagens proativas (quando a Samba toma a iniciativa de contato), a Meta exige templates pre-aprovados. Para follow-ups em conversas onde o parceiro ja escreveu nas ultimas 24h, templates NAO sao necessarios."),
        blank(4),
        codeBlock([
          "Meta Business Manager  -->  Gerenciador do WhatsApp  -->  Modelos de Mensagem",
          "  -->  Criar modelo",
          "  Categoria:  Utility (transacional)",
          "  Nome:       samba_followup_v1",
          "  Corpo:      'Ola {{1}}, estamos acompanhando a proposta de {{2}}.",
          "               Poderia nos dar um retorno?'",
          "  Enviar para aprovacao (prazo: 24h)",
        ]),
        blank(12),

        // =================================================================
        // SECAO 6 — INFRAESTRUTURA
        // =================================================================
        h1("INFRAESTRUTURA — SERVIDOR E TUNEL HTTPS", "6"),
        blank(4),

        h2("6.1  Para desenvolvimento local (ngrok)"),
        blank(2),
        codeBlock([
          "# Instalar ngrok: https://ngrok.com/download",
          "ngrok http 8000",
          "",
          "# Copiar a URL gerada, ex:",
          "# https://xxxx-xx-xx.ngrok.io",
          "",
          "# Inserir no .env:",
          "TWILIO_WEBHOOK_PUBLIC_URL=https://xxxx-xx-xx.ngrok.io/webhook/twilio",
        ]),
        blank(4),
        alertBox(
          "Limitacao do ngrok gratuito",
          "A URL do ngrok muda a cada reinicializacao no plano gratuito. " +
          "Para uso continuo, usar ngrok pago (URL fixa) ou migrar para VPS com dominio proprio.",
          C.AMBER,
        ),
        blank(6),

        h2("6.2  Para producao (VPS recomendado)"),
        blank(2),
        body("Stack minimo recomendado:"),
        blank(2),
        codeBlock([
          "Sistema:  Ubuntu 22.04 LTS",
          "Python:   3.11+",
          "Redis:    7+",
          "Nginx:    proxy reverso com HTTPS (Let's Encrypt gratuito)",
          "Processo: Supervisor ou systemd",
          "",
          "Servicos a rodar simultaneamente:",
          "",
          "# 1. API webhook (FastAPI)",
          "uvicorn api.webhook:app --host 0.0.0.0 --port 8000 --workers 2",
          "",
          "# 2. Worker Celery (extractor + sync)",
          "celery -A core.celery_app worker -Q queue_extractor,queue_sync --loglevel=info",
          "",
          "# 3. Beat Celery (schedules automaticos)",
          "celery -A core.celery_app beat --loglevel=info",
        ]),
        blank(6),

        h2("6.3  Nginx — configuracao minima"),
        blank(2),
        codeBlock([
          "server {",
          "    listen 443 ssl;",
          "    server_name seu-dominio.com.br;",
          "    ssl_certificate     /etc/letsencrypt/live/seu-dominio/fullchain.pem;",
          "    ssl_certificate_key /etc/letsencrypt/live/seu-dominio/privkey.pem;",
          "",
          "    location /webhook/twilio {",
          "        proxy_pass         http://127.0.0.1:8000;",
          "        proxy_set_header   Host $host;",
          "        proxy_set_header   X-Real-IP $remote_addr;",
          "        proxy_read_timeout 30s;",
          "    }",
          "    location /health {",
          "        proxy_pass http://127.0.0.1:8000;",
          "    }",
          "}",
        ]),
        blank(12),

        // =================================================================
        // SECAO 7 — CHECKLIST
        // =================================================================
        h1("CHECKLIST DE ATIVACAO — NA ORDEM CERTA", "7"),
        blank(4),
        alertBox(
          "Siga exatamente esta ordem",
          "Cada etapa depende da anterior. Pular etapas causa falhas de validacao " +
          "de assinatura Twilio ou erros de autenticacao que sao dificeis de diagnosticar.",
          C.ORANGE,
        ),
        blank(6),

        new Paragraph({ children: [tr("FASE 1 — Contas e aprovacoes", { size: px(10), bold: true, color: C.ORANGE })], spacing: { before: 0, after: px(4) } }),
        checkItem(false, "Criar conta Twilio em twilio.com/try-twilio (gratuito para comecar)"),
        checkItem(false, "Verificar negocio no Meta Business Manager com CNPJ da Samba Export"),
        checkItem(false, "Criar perfil WhatsApp Business para Samba Export no Meta"),
        checkItem(false, "Conectar +5513991405566 como WhatsApp Business no Twilio (Request Access)"),
        blank(4),

        new Paragraph({ children: [tr("FASE 2 — Servidor e conectividade", { size: px(10), bold: true, color: C.ORANGE })], spacing: { before: 0, after: px(4) } }),
        checkItem(false, "Subir VPS (recomendado) ou abrir tunel ngrok local"),
        checkItem(false, "Configurar HTTPS (Let's Encrypt no VPS ou URL ngrok)"),
        checkItem(false, "Testar acesso: curl https://SEU-DOMINIO/health  -->  resposta 'ok'"),
        blank(4),

        new Paragraph({ children: [tr("FASE 3 — Configuracao Twilio", { size: px(10), bold: true, color: C.ORANGE })], spacing: { before: 0, after: px(4) } }),
        checkItem(false, "Copiar TWILIO_ACCOUNT_SID e TWILIO_AUTH_TOKEN para o .env"),
        checkItem(false, "Configurar webhook URL no Twilio Console para o numero +5513991405566"),
        checkItem(false, "Definir TWILIO_WEBHOOK_PUBLIC_URL no .env"),
        checkItem(false, "Rodar diagnostico: python scripts/wpp_smoke_test.py  -->  todos OK"),
        blank(4),

        new Paragraph({ children: [tr("FASE 4 — Grupos internos", { size: px(10), bold: true, color: C.ORANGE })], spacing: { before: 0, after: px(4) } }),
        checkItem(false, "Pedir mensagem em SAMBA AGENTS MAIL BOX  -->  copiar Group ID do log"),
        checkItem(false, "Pedir mensagem em SAMBA AGENTS GOOGLE DRIVE  -->  copiar Group ID"),
        checkItem(false, "Pedir mensagem em SAMBA AGENTS TASKS FUP  -->  copiar Group ID"),
        checkItem(false, "Preencher WPP_GROUP_MAILBOX_ID, WPP_GROUP_DRIVE_ID, WPP_GROUP_TASKS_FUP_ID no .env"),
        blank(4),

        new Paragraph({ children: [tr("FASE 5 — Ativacao e testes", { size: px(10), bold: true, color: C.ORANGE })], spacing: { before: 0, after: px(4) } }),
        checkItem(false, "Mudar WHATSAPP_OFFLINE=false no .env"),
        checkItem(false, "Reiniciar workers Celery e uvicorn"),
        checkItem(false, "Teste de loopback: python scripts/wpp_smoke_test.py --send  -->  mensagem chega"),
        checkItem(false, "Testar @mention: digitar '@samba qual o status dos deals?' em um grupo interno"),
        checkItem(false, "Verificar resposta do router em < 5 segundos"),
        checkItem(false, "Criar template de follow-up proativo no Meta (opcional — apenas para 1o contato)"),
        blank(12),

        // =================================================================
        // SECAO 8 — ESTADO ATUAL
        // =================================================================
        h1("ESTADO ATUAL DO SISTEMA", "8"),
        blank(4),
        body("Situacao em " + TODAY + " — referencia para socios e equipe de desenvolvimento:"),
        blank(6),

        dataTable(
          ["Componente", "Status", "Acao necessaria"],
          [
            ["Numero WhatsApp +5513991405566", "Ativo no device", "Nenhuma"],
            ["Codigo dos 5 agentes", "Ativo (commitado)", "Nenhuma"],
            ["Memoria de conversa (ConversationHistory)", "Ativo", "Nenhuma"],
            ["Cadencia 3 tentativas + HITL", "Ativo", "Nenhuma"],
            ["Persistencia de matches arbitragem", "Ativo", "Nenhuma"],
            ["KB dinamica (Enrichment Agent)", "Ativo", "Nenhuma"],
            ["Roteamento por grupo (3 grupos)", "Ativo", "Nenhuma"],
            ["TWILIO_ACCOUNT_SID", "Preencher no .env", "Copiar do Console Twilio"],
            ["TWILIO_AUTH_TOKEN", "Preencher no .env", "Copiar do Console Twilio"],
            ["Webhook URL publica", "Configurar servidor/ngrok", "VPS ou ngrok + nginx"],
            ["WHATSAPP_OFFLINE", "Mudar para false", "Apos credenciais OK"],
            ["IDs dos 3 grupos internos", "Aguarda 1a mensagem de cada grupo", "Automatico apos webhook ativo"],
            ["Meta Business — verificacao", "Verificar status atual", "business.facebook.com"],
          ],
          [3200, 2400, 3760],
        ),

        blank(8),
        alertBox(
          "PROXIMO PASSO IMEDIATO",
          "A acao mais rapida para comecar: criar conta Twilio (gratuito), " +
          "copiar Account SID e Auth Token para o .env, abrir tunel ngrok na porta 8000 " +
          "e registrar a URL no Console Twilio. Com isso o webhook ja comeca a receber mensagens " +
          "e voce pode mapear os IDs dos grupos internos.",
          C.GREEN,
        ),

        blank(8),

        // Nota final
        new Paragraph({
          border: { top: { style: BorderStyle.SINGLE, size: 4, color: C.ORANGE, space: 2 } },
          spacing: { before: px(12), after: px(4) },
          children: [],
        }),
        new Paragraph({
          children: [
            tr("Samba Export Control Desk  —  Sistema de Agentes de Inteligencia Comercial", { size: px(8), bold: true, color: C.ORANGE }),
          ],
          spacing: { before: 0, after: px(2) },
        }),
        new Paragraph({
          children: [
            tr("Documento gerado automaticamente em " + TODAY + "  |  Confidencial — circulacao restrita a socios e desenvolvedores autorizados", { size: px(8), color: C.SILVER, italic: true }),
          ],
          spacing: { before: 0, after: 0 },
        }),
      ],
    },
  ],
});

// =============================================================================
// GERAR ARQUIVO
// =============================================================================
const outDir  = path.join(__dirname, "..", "docs");
const outFile = path.join(outDir, "SAMBA_WPP_AGENTS_REPORT.docx");

if (!fs.existsSync(outDir)) {
  fs.mkdirSync(outDir, { recursive: true });
}

Packer.toBuffer(doc).then(buffer => {
  fs.writeFileSync(outFile, buffer);
  console.log("OK  Documento gerado: " + outFile);
  console.log("    Tamanho: " + (buffer.length / 1024).toFixed(1) + " KB");
}).catch(err => {
  console.error("ERRO ao gerar documento:", err.message);
  process.exit(1);
});
