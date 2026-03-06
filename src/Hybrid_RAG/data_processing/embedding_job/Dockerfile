# Usa la tua immagine base
FROM runpod/pytorch:2.8.0-py3.11-cuda12.8.1-cudnn-devel-ubuntu22.04
WORKDIR /app

# Installa dipendenze di sistema
RUN apt-get update && apt-get install -y git

# Copia e installa le dipendenze Python
COPY requirements.txt .

# Approccio "Scorched Earth": ignora i pacchetti pre-installati
# e forza la reinstallazione delle nostre versioni.
RUN pip install --no-cache-dir --ignore-installed --force-reinstall -r requirements.txt

# Copia il codice sorgente (il main.py v12 è corretto)
COPY ./src /app/src

# Imposta il CMD
CMD ["python", "/app/src/main.py"]