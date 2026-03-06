import os
import logging
from typing import List, Dict, Any
from qdrant_client import QdrantClient
from elasticsearch import Elasticsearch
from langchain_community.embeddings import HuggingFaceEmbeddings
from sentence_transformers import CrossEncoder
import torch
import numpy as np 
import math

# Configurazione Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VECTOR_TOOL")

class HybridRetrievalTool:
    def __init__(self):
        
        # ***** TEST *****
        # --- LOGICA DINAMICA PER I TEST ---
        self.test_size = os.getenv("GDELT_TEST_SIZE", "").lower()
        
        if self.test_size == "full":
            print(f"🧪 VECTOR TOOL: Using PRODUCTION Collections (Full Data)")
            target_collection = "gdelt_articles"
            target_index = "news_chunks"
        elif self.test_size:
            print(f"🧪 TEST MODE DETECTED: SIZE={self.test_size.upper()}")
            target_collection = f"gdelt_articles_{self.test_size}"
            target_index = f"news_chunks_{self.test_size}"
        else:
            target_collection = "gdelt_articles"
            target_index = "news_chunks"

        self.collection_name = target_collection
        # ***** FINE TEST *****

        print("🛠️ Initializing HybridRetrievalTool v4.0 (High Precision Mode)...")
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"🖥️ Hardware detected: {self.device.upper()}")

        # --- DB SETUP ---
        qdrant_host = os.getenv("QDRANT_HOST", "qdrant-db")
        qdrant_port = int(os.getenv("QDRANT_PORT", 6333))
        try:
            self.qdrant = QdrantClient(host=qdrant_host, port=qdrant_port)
            #self.collection_name = "gdelt_articles"
            target_collection # <--- ASSEGNAZIONE DINAMICA
        except: 
            self.qdrant = None
            print("⚠️ Qdrant connection failed.")
        
        es_host = os.getenv("ELASTIC_HOST", "elasticsearch-db")
        es_port = int(os.getenv("ELASTIC_PORT", 9200))
        try:
            self.es = Elasticsearch(f"http://{es_host}:{es_port}", request_timeout=30)
            #self.es_index = "news_chunks"
            self.es_index = target_index # <--- ASSEGNAZIONE DINAMICA
        except: 
            self.es = None
            print("⚠️ Elasticsearch connection failed.")

        # --- MODELS SETUP ---
        # Embedding (Per Qdrant)
        print("📥 Loading Embeddings: Qwen/Qwen3-Embedding-0.6B...")
        try:
            self.embeddings = HuggingFaceEmbeddings(
                model_name="Qwen/Qwen3-Embedding-0.6B",
                model_kwargs={'device': self.device, 'trust_remote_code': True, 'model_kwargs': {"torch_dtype": torch.float16} if self.device == "cuda" else {}},
                encode_kwargs={'normalize_embeddings': True}
            )
        except Exception as e:
            print(f"❌ Embedding Model Error: {e}")
            self.embeddings = None

        # Reranker (Per il giudizio finale)
        # Nota: max_length aumentato a 1024 per leggere chunk lunghi senza tagliare la fine
        print("📥 Loading Reranker: BAAI/bge-reranker-v2-m3...")
        try:
            self.reranker = CrossEncoder("BAAI/bge-reranker-v2-m3", max_length=1024, device=self.device)
        except Exception as e:
            print(f"❌ Reranker Model Error: {e}")
            self.reranker = None
    
    def _adaptive_cutoff(self, scored_docs: List[tuple], limit: int) -> List[str]:
        """
        LOGICA DI FILTRAGGIO V4.0 (HIGH PRECISION):
        Obiettivo: Eliminare il rumore (documenti con score 0.01) mantenendo la recall.
        
        Parametri:
        - MIN_SCORE (0.25): Soglia minima assoluta. Sotto questo, è spazzatura.
        - CATASTROPHIC_DROP (0.45): Se la qualità crolla del 45% tra due documenti, STOP.
        - ADAPTIVE_THRESHOLD (0.20): Per la coda, se c'è un calo moderato, STOP.
        """
        if not scored_docs:
            return []

        # 1. Convertiamo logits in probabilità (0.0 - 1.0)
        # BGE restituisce logits grezzi, la sigmoide li rende leggibili
        probs = []
        for doc, score in scored_docs:
            try:
                prob = 1 / (1 + math.exp(-score))
            except OverflowError:
                prob = 0.0 if score < 0 else 1.0
            probs.append(prob)

        final_docs = []
        
        # --- TUNING PRECISIONE ---
        SAFE_KEEP = 3           # Tenta di tenere i primi 3...
        MIN_SCORE = 0.25        # ...MA SOLO SE superano questa soglia minima.
        CATASTROPHIC_DROP = 0.45 # Stop se crollo verticale.
        ADAPTIVE_THRESHOLD = 0.20 # Stop se calo graduale nella coda.

        for i in range(len(probs)):
            current_prob = probs[i]
            
            # --- CRITERIO 1: HARD FLOOR (Il "Buttafuori") ---
            # Se il documento ha meno del 25% di probabilità, fermati subito.
            # Questo elimina i casi dove tenevi 10 documenti con score 0.01.
            if current_prob < MIN_SCORE:
                print(f"✂️ HARD FLOOR: Taglio al doc #{i+1} (Score {current_prob:.4f} insufficiente)")
                break

            # Calcolo delta rispetto al precedente
            delta = 0
            if i > 0:
                prev_prob = probs[i-1]
                delta = prev_prob - current_prob

            # --- ZONA SALVAGENTE (Primi 3) ---
            if i < SAFE_KEEP:
                # Anche nei primi 3, se c'è un crollo disastroso, ci fermiamo.
                if i > 0 and delta > CATASTROPHIC_DROP:
                    print(f"✂️ CATASTROPHIC CUTOFF: Taglio brutale al doc #{i+1} (Delta: {delta:.4f})")
                    break
                
                final_docs.append(scored_docs[i][0])
                continue

            # --- ZONA CODA (Dal 4° in poi) ---
            if delta > ADAPTIVE_THRESHOLD:
                print(f"✂️ ADAPTIVE CUTOFF: Taglio al doc #{i+1} (Delta: {delta:.4f})")
                break
            
            final_docs.append(scored_docs[i][0])
            
            if len(final_docs) >= limit:
                break
        
        return final_docs

    def run_hybrid_search(self, query: str, limit: int = 10) -> Dict[str, Any]:
        print(f"🔍 VECTOR SEARCH execution for: '{query}'")
        
        # Aumentiamo i candidati per dare più scelta al Reranker
        SIZE_QDRANT = 100
        SIZE_ELASTIC = 100
        candidates = [] 
        
        # 1. Qdrant Retrieval (Semantico)
        if self.qdrant and self.embeddings:
            try:
                v = self.embeddings.embed_query(query)
                q_res = self.qdrant.search(self.collection_name, v, limit=SIZE_QDRANT)
                for h in q_res:
                    p = h.payload or {}
                    c = p.get("chunk_text") or p.get("content") or ""
                    if c: candidates.append(f"[Vector] {c}")
            except Exception as e:
                print(f"⚠️ Qdrant Error: {e}")

        # 2. Elasticsearch Retrieval (Keyword - Robust)
        if self.es:
            try:
                # Query semplificata per evitare errori 400
                res = self.es.search(index=self.es_index, body = {
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "match": {
                                        "chunk_text": {
                                            "query": query,
                                            # Usiamo OR per massimizzare la Recall (trovare tutto)
                                            # Il Reranker pulirà il rumore dopo.
                                            "operator": "or" 
                                        }
                                    }
                                }
                            ]
                        }
                    },
                    "size": SIZE_ELASTIC
                })

                for h in res['hits']['hits']:
                    c = h['_source'].get('chunk_text',"")
                    if c: candidates.append(f"[Keyword] {c}")
            except Exception as e:
                print(f"⚠️ Elasticsearch Error: {e}")

        # Unione e deduplica
        unique_candidates = list(set(candidates))
        print(f"📊 Candidates for Reranking: {len(unique_candidates)}")

        # 3. Reranking & Filtering
        final_results = []
        if self.reranker and unique_candidates:
            try:
                pairs = [[query, doc] for doc in unique_candidates]
                scores = self.reranker.predict(pairs)
                # Ordina per score decrescente
                scored_docs = sorted(zip(unique_candidates, scores), key=lambda x: x[1], reverse=True)
                
                print("\n🏆 --- TOP 5 BGE RERANKED ---")
                for i, (doc, score) in enumerate(scored_docs[:5]):
                    # Calcolo probabilità per display
                    prob = 1 / (1 + math.exp(-score))
                    print(f"#{i+1} [Prob: {prob:.4f} | Logit: {score:.2f}] {doc[:100]}...")
                print("-------------------------------\n")
                
                # Applicazione del filtro V4.0
                final_results = self._adaptive_cutoff(scored_docs, limit=limit)
            except Exception as e:
                print(f"⚠️ Reranker Error: {e}")
                final_results = unique_candidates[:limit]
        else:
            final_results = unique_candidates[:limit]

        print(f"📉 Adaptive Filter: Tenuti {len(final_results)} su {limit} possibili.")
        return {"results": final_results, "debug_info": f"Top {len(final_results)}"}

    def get_chunks_by_ids(self, chunk_ids: List[str]) -> List[str]:
        if not self.es or not chunk_ids: return []
        clean_ids = list(set([str(cid) for cid in chunk_ids if cid]))
        try:
            res = self.es.mget(index=self.es_index, body={"ids": clean_ids})
            return [d['_source'].get('chunk_text',"") for d in res.get('docs',[]) if d.get('found')]
        except: return []