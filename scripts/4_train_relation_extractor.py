import json
import pandas as pd
from setfit import SetFitModel, SetFitTrainer
from sklearn.model_selection import train_test_split
from datasets import Dataset
import os
import torch
import gc
from sentence_transformers.losses import CosineSimilarityLoss

# --- 1. GESTIONE MEMORIA ---
# Questo è fondamentale per le schede grandi per evitare frammentazione
os.environ["PYTORCH_CUDA_ALLOC_CONF"] = "expandable_segments:True"

gc.collect()
if torch.cuda.is_available():
    torch.cuda.empty_cache()

# --- 2. CONFIGURAZIONE ---
TRAINING_DATA_FILE = "training_data_pure_text.jsonl"
MODEL_OUTPUT_DIR = "./my_relation_extractor_SOTA_Final"
BASE_MODEL_NAME = "Qwen/Qwen3-Embedding-0.6B"

# --- 3. PREPARAZIONE DATI ---
print(f"--> Caricamento dati da {TRAINING_DATA_FILE}...")
data = []

try:
    with open(TRAINING_DATA_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            item = json.loads(line)
            text_input = f"{item['subject']} [SEP] {item['object']} [SEP] {item['context_text']}"
            label = item['relation']
            data.append({"text": text_input, "label_str": label})
except Exception as e:
    print(f"❌ Errore lettura: {e}"); exit(1)

df = pd.DataFrame(data)
label_list = sorted(df['label_str'].unique().tolist())
label2id = {label: i for i, label in enumerate(label_list)}
id2label = {i: label for i, label in enumerate(label_list)}
df['label'] = df['label_str'].map(label2id)
df_clean = df[['text', 'label']]

train_df, test_df = train_test_split(df_clean, test_size=0.15, random_state=42)
train_dataset = Dataset.from_pandas(train_df)
test_dataset = Dataset.from_pandas(test_df)

# --- 4. ADDESTRAMENTO (Configurazione Smart-Accumulation) ---
print(f"\n--> Avvio training su GPU: {torch.cuda.get_device_name(0)}")

model = SetFitModel.from_pretrained(BASE_MODEL_NAME, trust_remote_code=True)

print("--> Configurazione Trainer SOTA (con Accumulo)...")
trainer = SetFitTrainer(
    model=model,
    train_dataset=train_dataset,
    eval_dataset=test_dataset,
    loss_class=CosineSimilarityLoss,
    metric="accuracy",
    
    batch_size=8,        
    num_iterations=20,    
    num_epochs=1,         
    learning_rate=2e-5,

    use_amp=True
)

print("--> Inizio Addestramento...")
trainer.train()

# --- 5. VALUTAZIONE E SALVATAGGIO ---
print("\n--> Valutazione...")
metrics = trainer.evaluate()
print(f"\n🎯 RISULTATI ACCURACY: {metrics['accuracy']:.4f}")

print(f"\n--> Salvataggio modello in '{MODEL_OUTPUT_DIR}'...")
model.save_pretrained(MODEL_OUTPUT_DIR)
with open(os.path.join(MODEL_OUTPUT_DIR, "label_map.json"), "w") as f:
    json.dump({"label2id": label2id, "id2label": id2label}, f)

print("\n✅ COMPLETATO. Scarica la cartella!")