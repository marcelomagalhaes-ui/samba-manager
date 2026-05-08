"""Configurações centralizadas do Samba Export Control Desk."""
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
TEMPLATES_DIR = BASE_DIR / "templates"

# Ensure dirs exist
for d in [RAW_DIR, PROCESSED_DIR, TEMPLATES_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Claude API
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL_FAST = "claude-sonnet-4-20250514"
MODEL_DEEP = "claude-opus-4-20250514"

# Google
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "0AOllQoxhuNj4Uk9PVA")
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "config/google_credentials.json")

# Twilio
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "")

# Database
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{DATA_DIR / 'samba_control.db'}")

# Sócios
SOCIOS = [
    {"name": os.getenv("SOCIO_1_NAME", "Leonardo"), "phone": os.getenv("SOCIO_1_PHONE", "")},
    {"name": os.getenv("SOCIO_2_NAME", "Nivio"), "phone": os.getenv("SOCIO_2_PHONE", "")},
    {"name": os.getenv("SOCIO_3_NAME", "Marcelo"), "phone": os.getenv("SOCIO_3_PHONE", "")},
]

# Portfolio Samba Export (base de conhecimento dos agentes)
COMMODITIES = {
    "agri": ["soja", "soy", "soybeans", "açúcar", "sugar", "icumsa", "milho", "corn",
             "trigo", "wheat", "arroz", "rice", "paddy", "feijão", "beans",
             "amendoim", "peanuts", "sorgo", "sorghum", "gergelim", "sesame",
             "lentilha", "lentils", "grão de bico", "chickpea", "óleo vegetal", "veg oil"],
    "proteins": ["frango", "chicken", "boi", "beef", "porco", "pork", "tilápia",
                 "tilapia", "atum", "tuna", "ovo", "egg", "pé de frango", "chicken paw"],
    "flavors": ["café", "coffee", "arábica", "arabica", "conilon", "cacau", "cocoa",
                "açaí", "acai", "mel", "honey", "cachaça", "vinho", "wine"],
    "industrial": ["algodão", "cotton", "etanol", "ethanol", "biodiesel",
                   "madeira", "timber", "eucalipto", "eucalyptus", "pinus", "pine",
                   "ferro", "iron", "lítio", "lithium", "manganês", "manganese",
                   "glicerina", "glycerin", "lecitina", "lecithin"],
}

# Incoterms reconhecidos
INCOTERMS = ["FOB", "CIF", "FAS", "CFR", "CIP", "DAP", "DDP", "FCA", "EXW", "ASWP"]

# Moedas
CURRENCIES = ["USD", "BRL", "EUR", "R$", "US$"]
