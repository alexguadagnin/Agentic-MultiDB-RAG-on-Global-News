import os
import glob
import html
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
from typing import Set, Optional
import warnings

# --- 1. Import Costanti ---
try:
    from Hybrid_RAG.constants import RAW_DATA_DIR_EVENT
except ImportError:
    print("Attenzione: Impossibile importare 'RAW_DATA_DIR_EVENT'.")
    print("Imposto un valore di default './raw_data' per questo script.")
    RAW_DATA_DIR_EVENT = r"D:\progetto-rag-gdelt\data\gdelt_event" # Aggiornato al tuo path di test

# --- 2. Definizione Path e Schemi ---

OUTPUT_DATA_DIR = os.path.join(os.path.dirname(RAW_DATA_DIR_EVENT), "processed_parquet")
GKG_OUTPUT_DIR = os.path.join(OUTPUT_DATA_DIR, "gkg")
EXPORT_OUTPUT_DIR = os.path.join(OUTPUT_DATA_DIR, "export")
MENTIONS_OUTPUT_DIR = os.path.join(OUTPUT_DATA_DIR, "mentions")

# === DEFINIZIONE COMPLETA DELLE COLONNE (DAL TUO INPUT) ===

COLS_EXPORT = [
    "GlobalEventID", "Day", "MonthYear", "Year", "FractionDate", "Actor1Code", "Actor1Name", "Actor1CountryCode", 
    "Actor1KnownGroupCode", "Actor1EthnicCode", "Actor1Religion1Code", "Actor1Religion2Code", "Actor1Type1Code", 
    "Actor1Type2Code", "Actor1Type3Code", "Actor2Code", "Actor2Name", "Actor2CountryCode", "Actor2KnownGroupCode", 
    "Actor2EthnicCode", "Actor2Religion1Code", "Actor2Religion2Code", "Actor2Type1Code", "Actor2Type2Code", 
    "Actor2Type3Code", "IsRootEvent", "EventCode", "EventBaseCode", "EventRootCode", "QuadClass", "GoldsteinScale", 
    "NumMentions", "NumSources", "NumArticles", "AvgTone", "Actor1Geo_Type", "Actor1Geo_Fullname", "Actor1Geo_CountryCode", 
    "Actor1Geo_ADM1Code", "Actor1Geo_ADM2Code", "Actor1Geo_Lat", "Actor1Geo_Lon", "Actor1Geo_FeatureID", "Actor2Geo_Type", 
    "Actor2Geo_Fullname", "Actor2Geo_CountryCode", "Actor2Geo_ADM1Code", "Actor2Geo_ADM2Code", "Actor2Geo_Lat", "Actor2Geo_Lon", 
    "Actor2Geo_FeatureID", "ActionGeo_Type", "ActionGeo_Fullname", "ActionGeo_CountryCode", "ActionGeo_ADM1Code", 
    "ActionGeo_ADM2Code", "ActionGeo_Lat", "ActionGeo_Lon", "ActionGeo_FeatureID", "DATEADDED", "SOURCEURL"
]

COLS_GKG = [
    "GKGRECORDID", "V2.1DATE", "V2.1SourceCollectionIdentifier", "V2.1SourceCommonName", "V2.1DocumentIdentifier", 
    "V2.1Counts", "V2.1V2Counts", "V2.1Themes", "V2.1EnhancedThemes", "V2.1Locations", "V2.1EnhancedLocations", 
    "V2.1Persons", "V2.1EnhancedPersons", "V2.1Organizations", "V2.1EnhancedOrganizations", "V2.1Tone", 
    "V2.1EnhancedDates", "V2.1GCAM", "V2.1SharingImage", "V2.1RelatedImages", "V2.1SocialImageEmbeds", 
    "V2.1SocialVideoEmbeds", "V2.1Quotations", "V2.1AllNames", "V2.1Amounts", "V2.1TranslationInfo", "V2.1Extras"
]

COLS_MENTIONS = [
    "GlobalEventID", "EventTimeDate", "MentionTimeDate", "MentionType", "MentionSourceName", "MentionIdentifier", 
    "SentenceID", "Actor1CharOffset", "Actor2CharOffset", "ActionCharOffset", "InRawText", "Confidence", 
    "MentionDocLen", "MentionDocTone", "MentionDocTranslationInfo", "Extras"
]

# === COLONNE DA TENERE (BASATE SULLE NOSTRE DISCUSSIONI) ===

USECOLS_EXPORT_NAMES = [
    "GlobalEventID", "Day", "Actor1Name", "Actor2Name", "IsRootEvent", 
    "EventRootCode", "GoldsteinScale", "NumMentions", "AvgTone", 
    "ActionGeo_Fullname", "SOURCEURL"
]

USECOLS_GKG_NAMES = [
    "GKGRECORDID", "V2.1DATE", "V2.1Tone", "V2.1GCAM", "V2.1AllNames", 
    "V2.1EnhancedThemes", "V2.1EnhancedLocations", "V2.1SourceCommonName", 
    "V2.1DocumentIdentifier", "V2.1Extras"
]

USECOLS_MENTIONS_NAMES = [
    "GlobalEventID", "MentionTimeDate", "MentionSourceName", 
    "MentionIdentifier", "Confidence"
]

# === DTYPES PER LE COLONNE CHE TENIAMO (PER NOME) ===

DTYPES_EXPORT = {
    'GlobalEventID': 'int64',
    'Day': 'str', 
    'Actor1Name': 'str',
    'Actor2Name': 'str',
    'IsRootEvent': 'str', 
    'EventRootCode': 'str', # FONDAMENTALE per '020'
    'GoldsteinScale': 'float64',
    'NumMentions': 'int64',
    'AvgTone': 'float64',
    'ActionGeo_Fullname': 'str',
    'SOURCEURL': 'str'
}

DTYPES_GKG = {
    # Leggiamo tutto come stringa, è più sicuro e convertiamo dopo
    col: 'str' for col in USECOLS_GKG_NAMES
}

DTYPES_MENTIONS = {
    'GlobalEventID': 'int64',
    'MentionTimeDate': 'str', 
    'MentionSourceName': 'str',
    'MentionIdentifier': 'str',
    'Confidence': 'float64'
}


# --- 3. Funzioni di Pulizia (Helper Functions) ---
# (Queste sono corrette, ma le aggiorno per usare i nomi corretti delle colonne)

def to_lower_safe(s: str) -> Optional[str]:
    if isinstance(s, str):
        return s.lower()
    return None

def extract_gdelt_tone(v21tone_string: str) -> Optional[float]:
    if not isinstance(v21tone_string, str):
        return None
    try:
        parts = v21tone_string.split(',')
        return float(parts[0])
    except (ValueError, IndexError):
        return None

def clean_html_extras(content: str) -> Optional[str]:
    if pd.isna(content):
        return None
    try:
        return html.unescape(content)
    except:
        return content 

def clean_and_deduplicate_field(raw_string: str, 
                                field_separator: str = ';', 
                                offset_separator: str = ',') -> Optional[str]:
    if pd.isna(raw_string):
        return None 
    
    unique_items = {} 
    entities = raw_string.split(field_separator)
    
    for item in entities:
        if not item:
            continue
        try:
            name = item.split(offset_separator)[0]
            if name not in unique_items:
                unique_items[name] = True
        except IndexError:
            if item not in unique_items:
                unique_items[item] = True
    
    return field_separator.join(unique_items.keys())

def extract_location_names(raw_string: str) -> Optional[str]:
    if pd.isna(raw_string):
        return None
    
    nomi_unici: Set[str] = set()
    luoghi_lista = raw_string.split(';')
    
    for item in luoghi_lista:
        try:
            parti = item.split('#')
            if len(parti) > 1 and parti[1]: 
                nomi_unici.add(parti[1])
        except Exception:
            continue 
            
    return ';'.join(sorted(list(nomi_unici)))

def merge_actors(row: pd.Series) -> Optional[str]:
    actor1 = row.get('Actor1Name')
    actor2 = row.get('Actor2Name')
    
    actors: Set[str] = set()
    if isinstance(actor1, str) and actor1:
        actors.add(actor1)
    if isinstance(actor2, str) and actor2:
        actors.add(actor2)
        
    if not actors:
        return None
    
    return ';'.join(sorted(list(actors)))


# --- 4. Funzioni di Elaborazione per Dask (Process Partitions) ---

def process_gkg_partition(df: pd.DataFrame) -> pd.DataFrame:
    # N.B. Nessuna rinomina necessaria, Dask ha già letto i nomi corretti
    
    # 1. Applicare trasformazioni
    # Assicuriamo che la colonna V2.1Tone sia stringa prima di 'split'
    df['Tone'] = df['V2.1Tone'].astype(str).apply(extract_gdelt_tone)
    df['Extras_Cleaned'] = df['V2.1Extras'].astype(str).apply(clean_html_extras)
    
    df['AllNames_Cleaned'] = df['V2.1AllNames'].astype(str).apply(
        lambda x: clean_and_deduplicate_field(x, ';', ',')
    )
    
    # *** ECCEZIONE MINUSCOLO ***
    # N.B. Ho aggiornato allo spelling corretto 'V2.1EnhancedThemes'
    df['EnhancedThemes_Cleaned'] = df['V2.1EnhancedThemes'].astype(str).apply(
        lambda x: clean_and_deduplicate_field(x, ';', ',')
    )
    
    # N.B. Ho aggiornato allo spelling corretto 'V2.1EnhancedLocations'
    df['Locations_Cleaned'] = df['V2.1EnhancedLocations'].astype(str).apply(
        extract_location_names
    )
    
    # 2. Normalizzazione minuscolo (dove richiesto)
    df['Extras_Cleaned'] = df['Extras_Cleaned'].apply(to_lower_safe)
    df['AllNames_Cleaned'] = df['AllNames_Cleaned'].apply(to_lower_safe)
    df['Locations_Cleaned'] = df['Locations_Cleaned'].apply(to_lower_safe)
    df['V2.1SourceCommonName'] = df['V2.1SourceCommonName'].apply(to_lower_safe)
    df['V2.1DocumentIdentifier'] = df['V2.1DocumentIdentifier'].apply(to_lower_safe)
    
    # 3. Seleziona e rinomina le colonne finali
    final_columns_map = {
        'GKGRECORDID': 'GKGRECORDID',
        'V2.1DATE': 'Date',
        'V2.1GCAM': 'GCAM',
        'V2.1SourceCommonName': 'Source',
        'V2.1DocumentIdentifier': 'DocumentURL',
        'Tone': 'Tone',
        'Extras_Cleaned': 'Extras',
        'AllNames_Cleaned': 'AllNames',
        'EnhancedThemes_Cleaned': 'EnhancedThemes',
        'Locations_Cleaned': 'Locations'
    }
    
    # Seleziona solo le colonne che abbiamo trasformato o che erano già pronte
    final_df = df[list(final_columns_map.keys())]
    return final_df.rename(columns=final_columns_map)

def process_export_partition(df: pd.DataFrame) -> pd.DataFrame:
    # N.B. Nessuna rinomina necessaria
    
    # Applica la conversione in minuscolo direttamente
    df['Actor1Name'] = df['Actor1Name'].apply(to_lower_safe)
    df['Actor2Name'] = df['Actor2Name'].apply(to_lower_safe)
    df['ActionGeo_Fullname'] = df['ActionGeo_Fullname'].apply(to_lower_safe)
    df['SOURCEURL'] = df['SOURCEURL'].apply(to_lower_safe)
    
    # Le colonne finali sono quelle che abbiamo letto
    final_columns = [
        'GlobalEventID', 'Day', 'Actor1Name', 'Actor2Name', 'IsRootEvent', 
        'EventRootCode', 'GoldsteinScale', 'NumMentions', 'AvgTone', 
        'ActionGeo_Fullname', 'SOURCEURL'
    ]
    
    # Assicuriamo l'ordine corretto e restituiamo
    return df[final_columns]

def process_mentions_partition(df: pd.DataFrame) -> pd.DataFrame:
    # N.B. Nessuna rinomina necessaria
    
    df['MentionSourceName'] = df['MentionSourceName'].apply(to_lower_safe)
    df['MentionIdentifier'] = df['MentionIdentifier'].apply(to_lower_safe)
    
    # Le colonne lette (USECOLS_MENTIONS_NAMES) sono già quelle finali
    return df


# --- 5. Funzione Main (Esecuzione) ---

def main():
    warnings.filterwarnings("ignore", category=FutureWarning)
    
    print("Avvio cluster Dask...")
    cluster = LocalCluster(
        n_workers=3,                # Numero di worker
        threads_per_worker=1,       # Va bene 1
        memory_limit='4GB'        # Limite di RAM per OGNI worker
    )
    client = Client(cluster)
    print(f"Dashboard Dask disponibile a: {client.dashboard_link}")
    print(f"Lettura dati da: {RAW_DATA_DIR_EVENT}")
    print(f"Salvataggio dati in: {OUTPUT_DATA_DIR}\n")
    
    # --- 5.1. Processo GKG ---
    print("Avvio elaborazione GKG...")
    gkg_files = glob.glob(os.path.join(RAW_DATA_DIR_EVENT, "*.gkg.csv"))
    if gkg_files:
        meta_gkg = pd.DataFrame({
            'GKGRECORDID': pd.Series(dtype='str'),
            'Date': pd.Series(dtype='str'),
            'GCAM': pd.Series(dtype='str'),
            'Source': pd.Series(dtype='str'),
            'DocumentURL': pd.Series(dtype='str'),
            'Tone': pd.Series(dtype='float64'),
            'Extras': pd.Series(dtype='object'), # object è più sicuro per stringhe miste
            'AllNames': pd.Series(dtype='object'),
            'EnhancedThemes': pd.Series(dtype='object'),
            'Locations': pd.Series(dtype='object'),
        })
        
        ddf_gkg = dd.read_csv(
            gkg_files,
            sep='\t',
            header=None, # FONDAMENTALE: No header
            names=COLS_GKG, # FONDAMENTALE: Assegna nomi dall'elenco completo
            usecols=USECOLS_GKG_NAMES, # FONDAMENTALE: Tieni solo quelli che servono
            dtype=DTYPES_GKG,
            on_bad_lines='skip', 
            encoding='latin-1', # Corretto dall'ultimo test
            engine='python'
        )
        
        processed_gkg = ddf_gkg.map_partitions(process_gkg_partition, meta=meta_gkg)
        
        processed_gkg.to_parquet(
            GKG_OUTPUT_DIR,
            engine='pyarrow',
            compression='snappy',
            overwrite=True 
        )
        print(f"Elaborazione GKG completata. Output in {GKG_OUTPUT_DIR}")
    else:
        print("Nessun file GKG trovato.")

    # --- 5.2. Processo EXPORT ---
    print("\nAvvio elaborazione EXPORT...")
    export_files = glob.glob(os.path.join(RAW_DATA_DIR_EVENT, "*.export.csv"))
    if export_files:
        meta_export = pd.DataFrame({
            'GlobalEventID': pd.Series(dtype='int64'),
            'Day': pd.Series(dtype='str'),
            'Actor1Name': pd.Series(dtype='object'), # Aggiunto
            'Actor2Name': pd.Series(dtype='object'), # Aggiunto
            'IsRootEvent': pd.Series(dtype='str'),
            'EventRootCode': pd.Series(dtype='str'),
            'GoldsteinScale': pd.Series(dtype='float64'),
            'NumMentions': pd.Series(dtype='int64'),
            'AvgTone': pd.Series(dtype='float64'),
            'ActionGeo_Fullname': pd.Series(dtype='object'),
            'SOURCEURL': pd.Series(dtype='object'),
        })

        ddf_export = dd.read_csv(
            export_files,
            sep='\t',
            header=None,
            names=COLS_EXPORT, # FONDAMENTALE: Assegna nomi
            usecols=USECOLS_EXPORT_NAMES, # FONDAMENTALE: Tieni solo quelli che servono
            dtype=DTYPES_EXPORT,
            on_bad_lines='skip',
            encoding='latin-1',
            engine='python'
        )

        processed_export = ddf_export.map_partitions(process_export_partition, meta=meta_export)

        processed_export.to_parquet(
            EXPORT_OUTPUT_DIR,
            engine='pyarrow',
            compression='snappy',
            overwrite=True 
        )
        print(f"Elaborazione EXPORT completata. Output in {EXPORT_OUTPUT_DIR}")
    else:
        print("Nessun file EXPORT trovato.")
        
    # --- 5.3. Processo MENTIONS ---
    print("\nAvvio elaborazione MENTIONS...")
    mentions_files = glob.glob(os.path.join(RAW_DATA_DIR_EVENT, "*.mentions.csv"))
    if mentions_files:
        meta_mentions = pd.DataFrame({
            'GlobalEventID': pd.Series(dtype='int64'),
            'MentionTimeDate': pd.Series(dtype='str'),
            'MentionSourceName': pd.Series(dtype='object'),
            'MentionIdentifier': pd.Series(dtype='object'),
            'Confidence': pd.Series(dtype='float64'),
        })

        ddf_mentions = dd.read_csv(
            mentions_files,
            sep='\t',
            header=None,
            names=COLS_MENTIONS, # FONDAMENTALE: Assegna nomi
            usecols=USECOLS_MENTIONS_NAMES, # FONDAMENTALE: Tieni solo quelli che servono
            dtype=DTYPES_MENTIONS,
            on_bad_lines='skip',
            encoding='latin-1',
            engine='python'
        )

        processed_mentions = ddf_mentions.map_partitions(process_mentions_partition, meta=meta_mentions)

        processed_mentions.to_parquet(
            MENTIONS_OUTPUT_DIR,
            engine='pyarrow',
            compression='snappy',
            overwrite=True 
        )
        print(f"Elaborazione MENTIONS completata. Output in {MENTIONS_OUTPUT_DIR}")
    else:
        print("Nessun file MENTIONS trovato.")

    # --- 6. Chiusura ---
    print("\nElaborazione completata. Chiusura cluster Dask.")
    client.close()
    cluster.close()

if __name__ == "__main__":
    os.makedirs(GKG_OUTPUT_DIR, exist_ok=True)
    os.makedirs(EXPORT_OUTPUT_DIR, exist_ok=True)
    os.makedirs(MENTIONS_OUTPUT_DIR, exist_ok=True)
    
    print("=== AVVIO SCRIPT DI ELABORAZIONE GDELT ===")
    print("ATTENZIONE: Esecuzione su D:\progetto-rag-gdelt\data\gdelt_event")
    
    main()