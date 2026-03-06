import dask.dataframe as dd
from dask import compute
from dask.diagnostics import ProgressBar
from urllib.parse import urlparse
from pathlib import Path
import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns 

# Aggiunge la cartella 'src' al path di sistema
try:
    PROJECT_ROOT = Path(__file__).parent.parent
    SRC_DIR = PROJECT_ROOT / "src"
    sys.path.append(str(SRC_DIR))
    from Hybrid_RAG.constants import RAW_DATA_DIR_EVENT
except (NameError, ImportError):
    print("Avviso: Impossibile importare RAW_DATA_DIR_EVENT. Uso la cartella corrente.")
    RAW_DATA_DIR_EVENT = Path.cwd()


# Header (invariati)
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


# --- MODIFICA CHIAVE: Funzione di analisi corretta ---

def analizza_tipologia_file(lista_file: list, nome_tipologia: str, header: list, output_file=None):
    """
    Usa Dask per analizzare i file, applicando gli header corretti.
    Scrive l'output sia su console che sul file fornito.
    """
    
    def write_output(message):
        """Helper interno per scrivere su console e file."""
        if isinstance(message, pd.Series):
            message_str = message.to_string()
        else:
            message_str = str(message)
            
        print(message_str) 
        
        if output_file:
            output_file.write(message_str + '\n')

    write_output("=" * 80)
    write_output(f"🔎 Inizio analisi aggregata per la tipologia: {nome_tipologia.upper()}")
    write_output("=" * 80)

    if not lista_file:
        write_output("Nessun file trovato. Analisi saltata.\n")
        return

    write_output(f"Sto preparando l'analisi di {len(lista_file)} file...")
    
    ddf = dd.read_csv(
        lista_file, sep='\t', header=None, names=header,
        on_bad_lines='warn', blocksize=None, dtype='object', encoding='latin-1'
    )

    total_rows = len(ddf)
    null_count = ddf.isnull().sum()
    
    # --- ECCO LA CORREZIONE ---
    # Creiamo un task di approssimazione per OGNI colonna,
    # usando l'header fornito (perché ddf.columns non è noto a priori)
    unique_approx_tasks = {col: ddf[col].nunique_approx() for col in header}
    # --- FINE CORREZIONE ---
    
    write_output("▶️ Esecuzione del calcolo in parallelo (con progress bar)...")
    
    with ProgressBar():
        # Modifichiamo la chiamata compute per includere il dizionario di task
        (total_rows_res, null_count_res, unique_approx_res_dict) = compute(
            total_rows, null_count, unique_approx_tasks
        )
    
    if total_rows_res == 0:
        write_output(f"\n✅ Analisi completata. Trovate 0 righe. Impossibile calcolare statistiche.")
        write_output("\n" * 2)
        return

    # --- RICOSTRUIAMO LA PANDAS SERIES ---
    # Convertiamo il dizionario di risultati in una pd.Series
    # e la riordiniamo secondo l'header originale per coerenza
    unique_values_approx_res = pd.Series(unique_approx_res_dict).reindex(header)
    # --- FINE RICOSTRUZIONE ---

    null_percentage_res = (null_count_res / total_rows_res) * 100
    # Ora questo calcolo funzionerà per colonna
    unique_percentage_res = (unique_values_approx_res / total_rows_res) * 100

    write_output(f"\n✅ Analisi completata. Totale righe: {total_rows_res:,}")
    write_output("\n--- RISULTATI ANALISI AGGREGATA ---")
    
    write_output("\n📊 Percentuale di valori Nulli per colonna (%):")
    write_output(null_percentage_res) 
    
    write_output("\n✨ Conteggio (approssimato) di valori Unici per colonna:")
    write_output(unique_values_approx_res) # Ora stamperà la tabella

    write_output("\n📈 Percentuale (approssimata) di valori Unici sul Totale (%):")
    write_output(unique_percentage_res) # E anche questa

    write_output("\n" * 2)

# --- Funzioni invariate ---

def estrai_urls_da_file(lista_file: list, header: list, nome_colonna_url: str, write_log) -> list:
    """
    Estrae tutti gli URL da una lista di file, usando il nome corretto della colonna.
    Usa la funzione di logging 'write_log' passata.
    """
    write_log(f"▶️ Inizio estrazione URL dalla colonna '{nome_colonna_url}'...")
    if not lista_file:
        write_log("Nessun file trovato per l'estrazione URL.")
        return []

    ddf = dd.read_csv(
        lista_file, sep='\t', header=None, names=header, usecols=[nome_colonna_url],
        blocksize=None, on_bad_lines='warn', dtype='object', encoding='latin-1'
    )
    
    with ProgressBar():
        urls = ddf[nome_colonna_url].dropna().compute().tolist()
    
    write_log(f"✅ Estratti {len(urls):,} URL.")
    return urls

def crea_grafico_distribuzione_confidence(lista_file: list, header: list, write_log):
    """
    Analizza la colonna 'Confidence' dai file 'mentions' e crea un 
    VIOLIN PLOT della sua distribuzione, salvandolo come file PNG.
    """
    write_log("▶️ Inizio creazione grafico distribuzione 'Confidence' (Violin Plot)...")
    if not lista_file:
        write_log("Nessun file 'mentions' trovato per l'analisi 'Confidence'. Grafico saltato.")
        return

    try:
        ddf = dd.read_csv(
            lista_file, sep='\t', header=None, names=header, 
            usecols=['Confidence'], blocksize=None, 
            on_bad_lines='warn', encoding='latin-1'
        )

        ddf['Confidence'] = dd.to_numeric(ddf['Confidence'], errors='coerce')
        confidence_data_clean = ddf['Confidence'].dropna()

        write_log("Calcolo dei valori di 'Confidence' in corso...")
        with ProgressBar():
            confidence_values = confidence_data_clean.compute()

        if confidence_values.empty:
            write_log("ℹ️ Nessun valore 'Confidence' numerico valido trovato. Grafico saltato.")
            return

        write_log("Creazione del grafico (violin plot) in corso...")
        plt.figure(figsize=(10, 7)) 
        
        sns.violinplot(
            y=confidence_values,  
            inner='quartile',     
            palette='muted'       
        )
        
        plt.title('Distribuzione Valori "Confidence" (File Mentions)', fontsize=16)
        plt.ylabel('Punteggio di Confidence', fontsize=12)
        plt.xlabel('Distribuzione', fontsize=12)
        
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout() 

        output_image_path = Path.cwd() / "distribuzione_confidence_violin.png"
        plt.savefig(output_image_path)
        plt.close() 

        write_log(f"✅ Grafico distribuzione 'Confidence' (Violin Plot) salvato in: {output_image_path}")

    except Exception as e:
        write_log(f"❌ ERRORE durante la creazione del grafico 'Confidence': {e}")
        if 'usecols' in str(e):
             write_log("Dettaglio errore: Assicurati che 'Confidence' sia presente in 'mentions_header'.")


def main():
    """
    Funzione principale che orchestra l'analisi completa e logga tutto su file.
    """
    files_export = list(RAW_DATA_DIR_EVENT.glob('*.export.[cC][sS][vV]'))
    files_mentions = list(RAW_DATA_DIR_EVENT.glob('*.mentions.[cC][sS][vV]'))
    files_gkg = list(RAW_DATA_DIR_EVENT.glob('*.gkg.[cC][sS][vV]'))
    
    print(f"Trovati {len(files_export)} file 'export', {len(files_mentions)} 'mentions', e {len(files_gkg)} 'gkg'.\n")

    output_summary_path = Path.cwd() / "analysis_summary.txt"
    print(f"Tutti i risultati dell'analisi verranno salvati in: {output_summary_path}\n")
    
    with open(output_summary_path, 'w', encoding='utf-8') as summary_file:
        
        def write_log(message):
            """Helper per scrivere sia su console che sul file di log."""
            if isinstance(message, pd.Series):
                message_str = message.to_string()
            else:
                message_str = str(message)
                
            print(message_str) 
            summary_file.write(message_str + '\n')

        # --- 1. Analisi Statistica ---
        write_log("--- INIZIO ANALISI STATISTICA (NULLI, UNICI) ---")
        analizza_tipologia_file(files_export, "Export", export_header, output_file=summary_file)
        analizza_tipologia_file(files_mentions, "Mentions", mentions_header, output_file=summary_file)
        analizza_tipologia_file(files_gkg, "GKG", gkg_header, output_file=summary_file)
        write_log("--- FINE ANALISI STATISTICA ---")

        
        # --- 2. Creazione Grafico ---
        write_log("\n" + "=" * 80)
        write_log("📊 Inizio creazione grafico di distribuzione...")
        write_log("=" * 80)
        crea_grafico_distribuzione_confidence(files_mentions, mentions_header, write_log)
        write_log("--- FINE CREAZIONE GRAFICO ---")


        # --- 3. Estrazione e Conteggio Domini SEPARATO PER TIPOLOGIA ---
        write_log("\n" + "=" * 80)
        write_log("🌐 Inizio estrazione e conteggio domini per tipologia...")
        write_log("=" * 80)
        
        # --- Funzioni Helper ---
        def safe_parse_domain(url):
            if not isinstance(url, str) or not url.strip():
                return None
            try:
                netloc = urlparse(url).netloc 
                return netloc if netloc else None
            except Exception:
                return None

        def processa_e_salva_domini(lista_urls: list, nome_output_csv: str, nome_tipologia: str):
            if not lista_urls:
                write_log(f"ℹ️ Nessun URL valido trovato per {nome_tipologia}. File saltato.")
                return

            write_log(f"\n▶️ Inizio conteggio e classificazione per: {nome_tipologia.upper()}...")
            
            domini = [safe_parse_domain(url) for url in lista_urls]
            domini_validi = [d for d in domini if d]
            
            if not domini_validi:
                write_log(f"ℹ️ Nessun dominio valido estratto per {nome_tipologia}.")
                return
                
            write_log(f"Trovati {len(domini_validi):,} domini validi per {nome_tipologia}.")

            conteggio_series = pd.Series(domini_validi).value_counts()
            conteggio_df = conteggio_series.reset_index()
            conteggio_df.columns = ['dominio', 'numero_articoli']
            
            percorso_output_finale = Path.cwd() / nome_output_csv
            conteggio_df.to_csv(percorso_output_finale, index=False)
            
            write_log(f"✅ Conteggio {nome_tipologia} completato. Risultati salvati in: {percorso_output_finale}")
            write_log(f"\n--- Top 15 domini per {nome_tipologia} ---")
            write_log(conteggio_df.head(15).to_string()) 
            write_log("-" * 50)

        # --- ESECUZIONE ---
        write_log("\n--- TIPO: EXPORT ---")
        urls_export = estrai_urls_da_file(files_export, export_header, "SOURCEURL", write_log)
        processa_e_salva_domini(
            urls_export, 
            "conteggio_domini_EXPORT.csv", 
            "Export"
        )
        
        write_log("\n--- TIPO: MENTIONS ---")
        urls_mentions = estrai_urls_da_file(files_mentions, mentions_header, "MentionIdentifier", write_log)
        processa_e_salva_domini(
            urls_mentions, 
            "conteggio_domini_MENTIONS.csv", 
            "Mentions"
        )

        write_log("\n--- TIPO: GKG ---")
        urls_gkg = estrai_urls_da_file(files_gkg, gkg_header, "V2.1DocumentIdentifier", write_log)
        processa_e_salva_domini(
            urls_gkg, 
            "conteggio_domini_GKG.csv", 
            "GKG"
        )

        write_log("\n\nAnalisi domini separata completata con successo!")
    
    print(f"\n✅ Operazione completata. Log completo disponibile in '{output_summary_path}'.")
    print(f"📊 Grafico 'Confidence' (Violin Plot) disponibile in 'distribuzione_confidence_violin.png'.")


if __name__ == '__main__':
    main()