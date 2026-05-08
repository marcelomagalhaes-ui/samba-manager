# =====================================================================
# Dockerfile - SAMBA CORE ENGINE
# ---------------------------------------------------------------------
# Imagem unica usada por API, worker e beat (apenas o CMD muda em
# docker-compose.yml). Mantem o build simples e reutilizavel.
# =====================================================================

FROM python:3.11-slim

# Reduz tamanho de cache pip e impede que python escreva .pyc no host
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencias do SO necessarias para alguns pacotes Python
# (ex.: cryptography para Twilio, mysqlclient se trocar de DB).
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Instala dependencias Python primeiro (camada cacheavel).
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copia o codigo da aplicacao.
COPY . .

# Cria diretorio data/ para o SQLite (pode ser substituido por volume).
RUN mkdir -p /app/data

# Healthcheck do FastAPI (override em outros services do compose).
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:8000/health || exit 1

EXPOSE 8000

# Default = API. Worker e Beat sobrescrevem via `command:` no compose.
CMD ["uvicorn", "api.webhook:app", "--host", "0.0.0.0", "--port", "8000"]
