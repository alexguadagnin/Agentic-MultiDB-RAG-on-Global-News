from pathlib import Path

# __file__ è il percorso di questo file (constants.py)
# .parent ci fa salire di una cartella.
# Lo facciamo 3 volte per arrivare alla radice del progetto.
#
# .../src/HYBRID_RAG/constants.py  <- __file__
# .../src/HYBRID_RAG/             <- .parent
# .../src/                         <- .parent.parent
# .../progetto-rag-gdelt/          <- .parent.parent.parent (RADICE)
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()

# Definiamo tutti gli altri percorsi partendo dalla radice
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR_EVENT = DATA_DIR / "gdelt_event"
RAW_DATA_DIR_NGRAMS = DATA_DIR / "gdelt_ngrams"
DATA_DIR_NGRAMS = RAW_DATA_DIR_NGRAMS / "parquet_data_locale"
PROCESSED_DATA_DIR_EVENT = DATA_DIR / "03_processed"