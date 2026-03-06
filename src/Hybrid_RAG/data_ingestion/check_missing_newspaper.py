import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import pandas as pd
from pathlib import Path
import re
import os
from tqdm import tqdm # Import tqdm
from collections import Counter # Import Counter for header check

# Importa le costanti ESATTE dal tuo file
try:
    from Hybrid_RAG.constants import RAW_DATA_DIR_NGRAMS, DATA_DIR_NGRAMS, RAW_DATA_DIR_EVENT
    print(f"Path Cartella Genitore Ngrams (da constants): {RAW_DATA_DIR_NGRAMS}")
    print(f"Path Cartella Originale Parquet (da constants): {DATA_DIR_NGRAMS}")
    print(f"Path Cartella Event (da constants): {RAW_DATA_DIR_EVENT}")

except ImportError:
    print("ATTENZIONE: Impossibile importare da Hybrid_RAG.constants.")
    PROJECT_ROOT_FALLBACK = Path("D:/progetto-rag-gdelt")
    DATA_DIR_FALLBACK = PROJECT_ROOT_FALLBACK / "data"
    RAW_DATA_DIR_NGRAMS = DATA_DIR_FALLBACK / "gdelt_ngrams"
    DATA_DIR_NGRAMS = RAW_DATA_DIR_NGRAMS / "parquet_data_locale" # Cartella originale Parquet
    RAW_DATA_DIR_EVENT = DATA_DIR_FALLBACK / "gdelt_event"
    print(f"Uso path di fallback: RAW_DATA_DIR_NGRAMS={RAW_DATA_DIR_NGRAMS}, DATA_DIR_NGRAMS={DATA_DIR_NGRAMS}, RAW_DATA_DIR_EVENT={RAW_DATA_DIR_EVENT}")


# --- 1. CONFIGURAZIONE DEI PATH ---

# Path alla cartella locale ORIGINALE (definita in constants come DATA_DIR_NGRAMS)
PATH_ORIGINALE = DATA_DIR_NGRAMS

# Path alla cartella locale con i file Parquet "delta" (FIX)
PATH_FIX = RAW_DATA_DIR_NGRAMS / "parquet_fix_locale"

# --- Lista ESPLICITA dei file Parquet ---
LISTA_FILE_PARQUET = []
found_original = False
found_fix = False

print("\nRicerca file Parquet...")
# Cerca file nella cartella originale
if PATH_ORIGINALE.is_dir():
    files_originali = list(PATH_ORIGINALE.glob('*.parquet'))
    if files_originali:
        LISTA_FILE_PARQUET.extend(files_originali)
        found_original = True
        print(f" -> Trovati {len(files_originali)} file in: {PATH_ORIGINALE}")
    else:
        print(f" -> ATTENZIONE: Nessun file .parquet trovato in: {PATH_ORIGINALE}")
else:
    print(f"ATTENZIONE: Cartella originale NON trovata in: {PATH_ORIGINALE}")

# Cerca file nella cartella FIX
if PATH_FIX.is_dir():
    files_fix = list(PATH_FIX.glob('*.parquet'))
    if files_fix:
        LISTA_FILE_PARQUET.extend(files_fix)
        found_fix = True
        print(f" -> Trovati {len(files_fix)} file in: {PATH_FIX}")
    else:
        print(f" -> ATTENZIONE: Nessun file .parquet trovato in: {PATH_FIX}")
else:
    print(f"ATTENZIONE: Cartella FIX NON trovata in: {PATH_FIX}")


if not LISTA_FILE_PARQUET:
     print("\nERRORE CRITICO: Nessun file .parquet valido trovato nelle cartelle specificate! Controlla i percorsi.")
     exit() # Esce dallo script
else:
     print(f"\nTotale file Parquet che verranno letti: {len(LISTA_FILE_PARQUET)}")

# Path ai dati GDELT Event
GDELT_EVENT_DATA_PATH = RAW_DATA_DIR_EVENT
if not GDELT_EVENT_DATA_PATH.is_dir():
    print(f"ERRORE: La cartella GDELT Event non esiste: {GDELT_EVENT_DATA_PATH}")
    print("Uso la cartella corrente come fallback.")
    GDELT_EVENT_DATA_PATH = Path.cwd()

# --- 2. HEADER E DOMINI ---
siti_autorevoli = {
    "New York Times": "nytimes.com", "Washington Post": "washingtonpost.com",
    "Wall Street Journal": "wsj.com", "Los Angeles Times": "latimes.com",
    "USA Today": "usatoday.com", "Bloomberg": "bloomberg.com", "BBC": "bbc.com",
    "The Guardian": "theguardian.com", "The Times": "thetimes.co.uk", "Financial Times": "ft.com",
    "The Independent": "independent.co.uk", "The Telegraph": "telegraph.co.uk",
    "Le Monde": "lemonde.fr", "Le Figaro": "lefigaro.fr", "Libération": "liberation.fr",
    "Der Spiegel": "spiegel.de", "Frankfurter Allgemeine Zeitung": "faz.net", "Die Zeit": "zeit.de",
    "Süddeutsche Zeitung": "sueddeutsche.de", "Corriere della Sera": "corriere.it",
    "La Repubblica": "repubblica.it", "Il Sole 24 Ore": "ilsole24ore.com", "La Stampa": "lastampa.it",
    "Il Fatto Quotidiano": "ilfattoquotidiano.it", "El Pais": "elpais.com", "El Mundo": "elmundo.es",
    "ABC": "abc.es", "Russia Today": "rt.com", "TASS": "tass.ru", "Dagens Nyheter": "dn.se",
    "Svenska Dagbladet": "svd.se", "Le Soir": "lesoir.be", "De Standaard": "standaard.be",
    "NRC Handelsblad": "nrc.nl", "De Volkskrant": "volkskrant.nl", "Neue Zürcher Zeitung": "nzz.ch",
    "The Japan Times": "japantimes.co.jp", "Asahi Shimbun": "asahi.com", "Mainichi Shimbun": "mainichi.jp",
    "Yomiuri Shimbun": "yomiuri.co.jp", "China Daily": "chinadaily.com.cn",
    "South China Morning Post": "scmp.com", "Global Times": "globaltimes.cn", "The Hindu": "thehindu.com",
    "Times of India": "timesofindia.indiatimes.com", "Hindustan Times": "hindustantimes.com",
    "The Korea Herald": "koreaherald.com", "The Korea Times": "koreatimes.co.kr",
    "Straits Times": "straitstimes.com", "Bangkok Post": "bangkokpost.com", "The Star": "thestar.com.my",
    "Clarín": "clarin.com", "La Nación (Argentina)": "lanacion.com.ar", "O Globo": "oglobo.globo.com",
    "Folha de São Paulo": "folha.uol.com.br", "El Comercio (Perù)": "elcomercio.pe",
    "El Universal (Mexico)": "eluniversal.com.mx", "Reforma": "reforma.com",
    "El Tiempo (Colombia)": "eltiempo.com", "El Mercurio (Chile)": "emol.com",
    "Mail & Guardian": "mg.co.za", "News24": "news24.com", "Daily Nation (Kenya)": "nation.africa",
    "The Guardian Nigeria": "guardian.ng", "Al Ahram": "ahram.org.eg", "Le Matin": "lematin.ma",
    "The Sydney Morning Herald": "smh.com.au", "The Australian": "theaustralian.com.au",
    "New Zealand Herald": "nzherald.co.nz", "Reuters": "reuters.com", "Associated Press": "apnews.com",
    "Agence France-Presse": "afp.com", "Politico": "politico.com"
}
allowed_domains_set = set(siti_autorevoli.values())

# Header corretto per GDELT Export v2.0
# Fonte: http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf
export_header = [
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

# Controllo duplicati (essenziale!)
if len(export_header) != len(set(export_header)):
    print("ERRORE CRITICO: Trovati duplicati in export_header! Correggere.")
    duplicates = [item for item, count in Counter(export_header).items() if count > 1]
    print(f"Duplicati: {duplicates}")
    exit()

gkg_header = [
    "GKGRECORDID", "V2.1DATE", "V2.1SourceCollectionIdentifier", "V2.1SourceCommonName", "V2.1DocumentIdentifier", 
    "V2.1Counts", "V2.1V2Counts", "V2.1Themes", "V2.1EnhancedThemes", "V2.1Locations", "V2.1EnhancedLocations", 
    "V2.1Persons", "V2.1EnhancedPersons", "V2.1Organizations", "V2.1EnhancedOrganizations", "V2.1Tone", 
    "V2.1EnhancedDates", "V2.1GCAM", "V2.1SharingImage", "V2.1RelatedImages", "V2.1SocialImageEmbeds", 
    "V2.1SocialVideoEmbeds", "V2.1Quotations", "V2.1AllNames", "V2.1Amounts", "V2.1TranslationInfo", "V2.1Extras"
]

mentions_header = [
    "GlobalEventID", "EventTimeDate", "MentionTimeDate", "MentionType", "MentionSourceName", "MentionIdentifier", 
    "SentenceID", "Actor1CharOffset", "Actor2CharOffset", "ActionCharOffset", "InRawText", "Confidence", 
    "MentionDocLen", "MentionDocTone", "MentionDocTranslationInfo", "Extras"
]


def run_gap_analysis_refined():
    print("--- AVVIO GAP ANALYSIS RAFFINATO ---")

    # --- STEP 1: Carica TUTTI gli URL "Processati" (Set B e C) ---
    print(f"\nStep 1/5: Lettura URL e testo da {len(LISTA_FILE_PARQUET)} file Parquet...")
    try:
        paths_str = [str(p) for p in LISTA_FILE_PARQUET]
        ddf_processed = dd.read_parquet(paths_str, columns=['url', 'text'], engine='pyarrow')

        print(" -> Calcolo dati Parquet in memoria (può richiedere tempo)...")
        with ProgressBar():
            processed_df = ddf_processed.compute()

        # Set B (N-Gram con testo) e C (N-Gram senza testo)
        ngram_urls_with_text_setB = set(processed_df[processed_df['text'].notna() & (processed_df['text'] != '')]['url'])
        ngram_urls_without_text_setC = set(processed_df[processed_df['text'].isna() | (processed_df['text'] == '')]['url'])
        ngram_all_processed_urls_setBUC = ngram_urls_with_text_setB.union(ngram_urls_without_text_setC)

        print(f" -> Trovati {len(ngram_urls_with_text_setB):,} URL con testo nei Parquet (Set B N-Gram).")
        print(f" -> Trovati {len(ngram_urls_without_text_setC):,} URL senza testo/NaN nei Parquet (Set C N-Gram).")
        print(f" -> Totale URL unici processati (N-Gram): {len(ngram_all_processed_urls_setBUC):,}")

    except Exception as e:
        print(f"ERRORE: Impossibile leggere i file Parquet specificati."); print(f"Dettaglio: {e}"); return

    # --- STEP 2: Prepara il filtro per i "Potenziali" (Set A) ---
    print(f"\nStep 2/5: Preparazione filtri per i domini autorevoli...")
    domain_patterns = []
    for domain in allowed_domains_set:
        pattern = r'https?://(?:[^\./]+\.)*?' + re.escape(domain) + r'(?:/|$)'
        domain_patterns.append(pattern)
    domain_regex_pattern = '|'.join(domain_patterns)
    print(f" -> Filtro creato per {len(allowed_domains_set)} domini.")

    # --- STEP 3: Estrai tutti gli URL "Potenziali" (Set A da GDELT Event) ---
    print(f"\nStep 3/5: Scansione file GDELT Event da '{GDELT_EVENT_DATA_PATH}' (può richiedere tempo)...")
    def get_autorevoli_urls(glob_pattern, url_column, headers):
        gdelt_path = Path(GDELT_EVENT_DATA_PATH)
        files = list(gdelt_path.glob(glob_pattern))
        if not files: print(f" -> ATTENZIONE: Nessun file trovato per '{glob_pattern}' in {gdelt_path}"); return None
        print(f" -> Trovati {len(files)} file per '{glob_pattern}'")
        try:
            # Controllo header duplicato
            if len(headers) != len(set(headers)):
                 print(f" -> ERRORE INTERNO: Duplicati in header per '{glob_pattern}'!"); return None

            ddf = dd.read_csv([str(f) for f in files], sep='\t', header=None, names=headers,
                             usecols=[url_column], on_bad_lines='warn', dtype='object',
                             encoding='latin-1', blocksize='128MB', assume_missing=True)
            ddf[url_column] = ddf[url_column].astype(str).fillna('')
            filtered_ddf = ddf[ddf[url_column].str.contains(domain_regex_pattern, na=False, case=False, regex=True)]
            return filtered_ddf[url_column]
        except ValueError as ve: print(f" -> ERRORE di tipo lettura/filtro '{glob_pattern}': {ve}"); return None
        except Exception as e: print(f" -> ERRORE generico lettura/filtro '{glob_pattern}': {e}"); return None

    # Esegui lettura (ora export dovrebbe funzionare)
    urls_export = get_autorevoli_urls('*.export.[cC][sS][vV]', 'SOURCEURL', export_header)
    urls_mentions = get_autorevoli_urls('*.mentions.[cC][sS][vV]', 'MentionIdentifier', mentions_header)
    urls_gkg = get_autorevoli_urls('*.gkg.[cC][sS][vV]', 'V2.1DocumentIdentifier', gkg_header)

    potential_url_series_list = [s for s in [urls_export, urls_mentions, urls_gkg] if s is not None]
    if not potential_url_series_list:
        print("\nERRORE CRITICO: Nessun URL potenziale trovato da NESSUN file GDELT Event valido."); return

    all_potential_urls = dd.concat(potential_url_series_list)
    print("\n -> Calcolo degli URL unici totali dai file GDELT Event...")
    with ProgressBar():
        unique_potential_urls_series = all_potential_urls.dropna().drop_duplicates().compute()

    # Set A "Ground Truth"
    potential_urls_setA = set(unique_potential_urls_series)
    total_potential_A = len(potential_urls_setA)
    export_status = "export" if urls_export is not None else "NO export"
    mentions_status = "mentions" if urls_mentions is not None else "NO mentions"
    gkg_status = "gkg" if urls_gkg is not None else "NO gkg"
    print(f" -> Trovati {total_potential_A:,} URL *unici* (da {export_status}/{mentions_status}/{gkg_status}) dei domini autorevoli (Set A).")


    # --- STEP 4: Confronta Set A vs (Set B U C) e Filtra ---
    print("\nStep 4/5: Confronto e filtro delle liste (GDELT Event vs N-Gram Parquet)...")
    filtered_urls_with_text_setB = ngram_urls_with_text_setB.intersection(potential_urls_setA)
    filtered_urls_without_text_setC = ngram_urls_without_text_setC.intersection(potential_urls_setA)
    filtered_all_processed_urls_setBUC = filtered_urls_with_text_setB.union(filtered_urls_without_text_setC)
    completely_missing_urls_set = potential_urls_setA - filtered_all_processed_urls_setBUC # A - (B' U C')
    completely_missing_urls = list(completely_missing_urls_set) # Lista per report e file
    total_completely_missing = len(completely_missing_urls_set)
    total_filtered_processed_with_text = len(filtered_urls_with_text_setB)
    total_filtered_processed_without_text = len(filtered_urls_without_text_setC)
    total_filtered_processed = len(filtered_all_processed_urls_setBUC)
    print(f" -> Fatto.")
    print(f" -> URL N-Gram trovati in GDELT Event con testo (B'): {total_filtered_processed_with_text:,}")
    print(f" -> URL N-Gram trovati in GDELT Event senza testo (C'): {total_filtered_processed_without_text:,}")
    print(f" -> URL Mancanti (A - (B' U C')): {total_completely_missing:,}")


    # --- STEP 5: Report finale Raffinato (basato sui dati filtrati) ---
    print("\n--- 📊 REPORT GAP ANALYSIS RAFFINATO (vs GDELT Event) ---")
    print("=" * 60)
    print(f"URL Unici Potenziali in GDELT Event (Set A):      {total_potential_A:,.0f}")
    print("-" * 60)
    print(f"URL Processati (N-Gram ∩ A) con Testo (Set B'):    {total_filtered_processed_with_text:,.0f}")
    print(f"URL Processati (N-Gram ∩ A) senza Testo (Set C'): {total_filtered_processed_without_text:,.0f}")
    print(f"URL Totali Processati (N-Gram ∩ A) (Set B' U C'): {total_filtered_processed:,.0f}")
    print("-" * 60)
    print(f"URL MANCANTI (A - (B' U C')):                 {total_completely_missing:,.0f}")
    print("-" * 60)
    if total_potential_A > 0:
        coverage_perc = (total_filtered_processed / total_potential_A) * 100
        reconstruction_success_perc = (total_filtered_processed_with_text / total_filtered_processed) * 100 if total_filtered_processed > 0 else 0
        print(f"Percentuale di Copertura (su A):            {coverage_perc:.2f}%  ((B' U C') / A)")
        print(f"Successo Ricostruzione (su B' U C'):      {reconstruction_success_perc:.2f}%  (B' / (B' U C'))")

    if total_completely_missing > 0:
        print("\n--- 📉 DOMINI CON PIÙ ARTICOLI MANCANTI (A - (B' U C')) ---")
        try:
            missing_df = pd.DataFrame(completely_missing_urls, columns=['url'])
            missing_df['domain'] = missing_df['url'].str.extract(r'https?://(?:www\.)?([^/]+)', expand=False).fillna('Dominio sconosciuto')
            domain_counts_missing = missing_df['domain'].value_counts()
            print(domain_counts_missing.head(20).to_string())
        except Exception as e: print(f" -> Errore analisi domini mancanti: {e}")

    if total_filtered_processed_without_text > 0:
        print("\n--- ⚠️ DOMINI CON PIÙ ARTICOLI PROCESSATI SENZA TESTO (Set C') ---")
        try:
            without_text_df = pd.DataFrame(list(filtered_urls_without_text_setC), columns=['url'])
            without_text_df['domain'] = without_text_df['url'].str.extract(r'https?://(?:www\.)?([^/]+)', expand=False).fillna('Dominio sconosciuto')
            domain_counts_no_text = without_text_df['domain'].value_counts()
            print(domain_counts_no_text.head(20).to_string())
        except Exception as e: print(f" -> Errore analisi domini senza testo: {e}")

    # <<< Esporta lista URL mancanti e falliti >>>
    print("\n--- 💾 ESPORTAZIONE LISTA URL PROBLEMATICI ---")
    # Definisci il nome del file di output (verrà salvato nella cartella corrente)
    output_filename = "url_mancanti_e_falliti.txt"
    try:
        # Unisci i due set: fallimenti ricostruzione (C') + completamente mancanti (A - (B' U C'))
        combined_problematic_urls = filtered_urls_without_text_setC.union(completely_missing_urls_set)
        total_problematic = len(combined_problematic_urls)
        print(f" -> Trovati {total_problematic:,} URL totali (mancanti + ricostruzione fallita) da esportare.")

        # Scrivi su file
        with open(output_filename, 'w', encoding='utf-8') as f_out:
            f_out.write("# Lista URL da GDELT Event appartenenti a domini autorevoli che:\n")
            f_out.write("# 1. Non sono stati trovati nei dati N-Gram processati (mancanti)\n")
            f_out.write("# 2. Sono stati trovati negli N-Gram ma la ricostruzione del testo è fallita\n")
            f_out.write("# ----------------------------------------------------------------------\n")
            # Scrivi gli URL ordinati
            for url in sorted(list(combined_problematic_urls)):
                f_out.write(f"{url}\n")
        print(f" -> Lista salvata in: {os.path.abspath(output_filename)}")

    except Exception as e:
        print(f"ERRORE durante l'esportazione della lista URL: {e}")
    # <<< FINE ESPORTAZIONE >>>

    print("\n=" * 60)
    print("Analisi completata. ✅")


if __name__ == "__main__":
    try:
        run_gap_analysis_refined()
    except Exception as main_e:
        print(f"\n--- ERRORE FATALE NELL'ESECUZIONE PRINCIPALE ---")
        print(f"{type(main_e).__name__}: {main_e}")