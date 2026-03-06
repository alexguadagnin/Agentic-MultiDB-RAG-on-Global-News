FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libpq-dev gcc curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia PRIMA solo i requirements (per sfruttare la cache di Docker)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia il resto del codice (Docker ignorerà la cartella 'data' grazie al file .dockerignore)
COPY . .

ENV PYTHONPATH=/app/src

CMD ["uvicorn", "Hybrid_RAG.api.main:app", "--host", "0.0.0.0", "--port", "8000"]