import os

# --- Percorsi (Architettura Robusta) ---
# Assumiamo che il volume sia montato su /data
VOLUME_MOUNT_PATH = "/data"
INPUT_DIR = os.path.join(VOLUME_MOUNT_PATH, "input")
OUTPUT_DIR = os.path.join(VOLUME_MOUNT_PATH, "output_embeddings")
PROCESSED_DIR = os.path.join(VOLUME_MOUNT_PATH, "processed_parquet")
CHECKPOINT_DIR = os.path.join(VOLUME_MOUNT_PATH, "checkpoints") # Il nostro "taccuino"

# --- Modello e Chunking ---
MODEL_NAME = "Qwen/Qwen3-Embedding-0.6B"
CHUNK_SIZE = 1000 # Manteniamo chunk grandi per un buon contesto
CHUNK_OVERLAP = 100

# --- Efficienza e Checkpoint ---
# Quanti articoli processare prima di salvare checkpoint e log
CHECKPOINT_EVERY_N_ROWS = 1000 
EMBEDDING_BATCH_SIZE = 64 # Satura la GPU con batch più grandi

# --- Colonne Parquet ---
# Il nome corretto della colonna di testo
TEXT_COLUMN = "text"