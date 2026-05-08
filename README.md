# 🎷 SAMBA EXPORT — Control Desk
## Plataforma de Agentes de IA para Gestão de Commodities

### Marco Zero — Estrutura Inicial do Projeto

```
 C:\SAMBA_MANAGER\SAMBA_AGENTS
│
├── main.py                    # Entry point da aplicação
├── requirements.txt           # Dependências Python
├── .env                       # Variáveis de ambiente (NÃO commitar)
├── .env.example               # Template do .env
├── README.md                  # Este arquivo
│
├── config/
│   ├── __init__.py
│   └── settings.py            # Configurações centralizadas
│
├── parsers/
│   ├── __init__.py
│   └── whatsapp_parser.py     # Parser de conversas WhatsApp (.txt)
│
├── models/
│   ├── __init__.py
│   ├── database.py            # Conexão SQLite (dev) / PostgreSQL (prod)
│   └── schemas.py             # Modelos de dados (Message, Deal, Quote, etc.)
│
├── agents/
│   ├── __init__.py
│   ├── base_agent.py          # Classe base de todos os agentes
│   ├── extractor_agent.py     # Agente 2: Minerador (parser de cotações)
│   ├── manager_agent.py       # Agente 1: Gerente Geral
│   ├── followup_agent.py      # Agente 3: Follow-Up / Cobrador
│   ├── documental_agent.py    # Agente 4: Documental / Risco
│   └── agenda_agent.py        # Agente 5: Agenda / Secretariado
│
├── services/
│   ├── __init__.py
│   ├── google_drive.py        # Integração Google Drive API
│   ├── google_sheets.py       # Integração Google Sheets API
│   ├── whatsapp_api.py        # Integração Twilio / WhatsApp Business
│   ├── esalq_scraper.py       # Scraper ESALQ/CEPEA
│   └── claude_api.py          # Integração Claude API (Anthropic)
│
├── dashboards/
│   ├── __init__.py
│   └── streamlit_app.py       # Dashboard Streamlit (Control Desk)
│
├── data/
│   ├── raw/                   # Arquivos brutos (chats WhatsApp .txt)
│   └── processed/             # Dados processados (JSON, CSV)
│
├── templates/                 # Templates de documentos (SPA, FCO, etc.)
│
└── tests/
    ├── __init__.py
    └── test_parser.py         # Testes do parser
```

### Setup Rápido (Windows)

```bash
# 1. Criar pasta do projeto
mkdir  C:\SAMBA_MANAGER>
cd  C:\SAMBA_MANAGER>

# 2. Criar ambiente virtual
python -m venv venv
venv\Scripts\activate

# 3. Instalar dependências
pip install -r requirements.txt

# 4. Copiar .env.example para .env e preencher
copy .env.example .env

# 5. Rodar o parser nos chats de WhatsApp
python main.py parse --input data/raw/

# 6. Rodar o dashboard
streamlit run dashboards/streamlit_app.py
```
