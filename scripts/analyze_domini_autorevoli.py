import csv
import re

# La tua lista di siti autorevoli
siti_autorevoli = {
    # Nord America (completa)
    "New York Times": "nytimes.com", 
    "Washington Post": "washingtonpost.com", 
    "Wall Street Journal": "wsj.com",
    "Los Angeles Times": "latimes.com", 
    "USA Today": "usatoday.com", 
    "Bloomberg": "bloomberg.com",
    
    # Europa (espansa)
    "BBC": "bbc.com", 
    "The Guardian": "theguardian.com", 
    "The Times": "thetimes.co.uk", 
    "Financial Times": "ft.com",
    "The Independent": "independent.co.uk", 
    "The Telegraph": "telegraph.co.uk",
    
    "Le Monde": "lemonde.fr", "Le Figaro": "lefigaro.fr", "Libération": "liberation.fr",
    "Der Spiegel": "spiegel.de", "Frankfurter Allgemeine Zeitung": "faz.net", "Die Zeit": "zeit.de", 
    "Süddeutsche Zeitung": "sueddeutsche.de",
    
    "Corriere della Sera": "corriere.it", "La Repubblica": "repubblica.it", 
    "Il Sole 24 Ore": "ilsole24ore.com", "La Stampa": "lastampa.it",
    "Il Fatto Quotidiano": "ilfattoquotidiano.it",
    
    "El Pais": "elpais.com", "El Mundo": "elmundo.es", "ABC": "abc.es",
    
    # NUOVI - Europa
    "Russia Today": "rt.com", "TASS": "tass.ru",                   # Russia
    "Dagens Nyheter": "dn.se", "Svenska Dagbladet": "svd.se",      # Svezia
    "Le Soir": "lesoir.be", "De Standaard": "standaard.be",        # Belgio
    "NRC Handelsblad": "nrc.nl", "De Volkskrant": "volkskrant.nl", # Paesi Bassi
    "Neue Zürcher Zeitung": "nzz.ch",                             # Svizzera
    
    # Asia (espansa)
    "The Japan Times": "japantimes.co.jp", "Asahi Shimbun": "asahi.com", 
    "Mainichi Shimbun": "mainichi.jp", "Yomiuri Shimbun": "yomiuri.co.jp",
    
    "China Daily": "chinadaily.com.cn", "South China Morning Post": "scmp.com",
    "Global Times": "globaltimes.cn",
    
    "The Hindu": "thehindu.com", "Times of India": "timesofindia.indiatimes.com",
    "Hindustan Times": "hindustantimes.com",
    
    # NUOVI - Asia
    "The Korea Herald": "koreaherald.com", "The Korea Times": "koreatimes.co.kr",
    "Straits Times": "straitstimes.com",                          # Singapore
    "Bangkok Post": "bangkokpost.com",                           # Thailandia
    "The Star": "thestar.com.my",                                # Malaysia
    
    # America Latina (espansa)
    "Clarín": "clarin.com", "La Nación (Argentina)": "lanacion.com.ar",
    "O Globo": "oglobo.globo.com", "Folha de São Paulo": "folha.uol.com.br",
    "El Comercio (Perù)": "elcomercio.pe",
    
    # NUOVI - America Latina
    "El Universal (Mexico)": "eluniversal.com.mx", 
    "Reforma": "reforma.com",
    "El Tiempo (Colombia)": "eltiempo.com",
    "El Mercurio (Chile)": "emol.com",
    
    # Africa (espansa)
    "Mail & Guardian": "mg.co.za", "News24": "news24.com",
    "Daily Nation (Kenya)": "nation.africa",
    
    # NUOVI - Africa
    "The Guardian Nigeria": "guardian.ng",                       # Nigeria
    "Al Ahram": "ahram.org.eg",                                 # Egitto
    "Le Matin": "lematin.ma",                                   # Marocco
    
    # Oceania (OK)
    "The Sydney Morning Herald": "smh.com.au", 
    "The Australian": "theaustralian.com.au", 
    "New Zealand Herald": "nzherald.co.nz",
    
    # Agenzie internazionali
    "Reuters": "reuters.com", 
    "Associated Press": "apnews.com", 
    "Agence France-Presse": "afp.com", 
    "Politico": "politico.com"
}

def analizza_csv_e_trova_corrispondenze(file_csv, output_file):
    """
    Analizza il CSV e trova le corrispondenze con i domini autorevoli
    """
    corrispondenze = []
    
    # Leggi il file CSV
    with open(file_csv, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Salta l'header
        
        for riga in reader:
            if len(riga) >= 2:
                dominio = riga[0].strip()
                numero_articoli = int(riga[1].strip())
                
                # Cerca corrispondenze nei domini autorevoli
                for nome_sito, dominio_autorevole in siti_autorevoli.items():
                    # Verifica se il dominio contiene il dominio autorevole
                    if dominio_autorevole in dominio:
                        corrispondenze.append({
                            'dominio_csv': dominio,
                            'nome_sito_autorevole': nome_sito,
                            'dominio_autorevole': dominio_autorevole,
                            'numero_articoli': numero_articoli
                        })
                        break
    
    # Ordina per numero di articoli (decrescente)
    corrispondenze.sort(key=lambda x: x['numero_articoli'], reverse=True)
    
    # Scrivi i risultati in un nuovo file
    with open(output_file, 'w', newline='', encoding='utf-8') as file_output:
        writer = csv.writer(file_output)
        writer.writerow(['Nome Sito Autorevole', 'Dominio Autorevole', 'Dominio nel CSV', 'Numero Articoli'])
        
        for corrispondenza in corrispondenze:
            writer.writerow([
                corrispondenza['nome_sito_autorevole'],
                corrispondenza['dominio_autorevole'],
                corrispondenza['dominio_csv'],
                corrispondenza['numero_articoli']
            ])
    
    return corrispondenze

# Versione alternativa che cerca corrispondenze più flessibili
def analizza_csv_corrispondenze_avanzate(file_csv, output_file):
    """
    Versione più avanzata che cerca corrispondenze più flessibili
    """
    corrispondenze = []
    
    with open(file_csv, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Salta l'header
        
        for riga in reader:
            if len(riga) >= 2:
                dominio = riga[0].strip()
                numero_articoli = int(riga[1].strip())
                
                # Pulisci il dominio per il matching
                dominio_pulito = re.sub(r'^www\.', '', dominio)
                dominio_pulito = re.sub(r':\d+$', '', dominio_pulito)  # Rimuovi porta
                
                for nome_sito, dominio_autorevole in siti_autorevoli.items():
                    # Diverse strategie di matching
                    if (dominio_autorevole in dominio_pulito or 
                        dominio_autorevole.replace('.', '') in dominio_pulito.replace('.', '') or
                        any(dominio_autorevole in parte for parte in dominio_pulito.split('/'))):
                        
                        corrispondenze.append({
                            'dominio_csv': dominio,
                            'nome_sito_autorevole': nome_sito,
                            'dominio_autorevole': dominio_autorevole,
                            'numero_articoli': numero_articoli
                        })
                        break
    
    # Ordina per numero di articoli (decrescente)
    corrispondenze.sort(key=lambda x: x['numero_articoli'], reverse=True)
    
    # Scrivi i risultati
    with open(output_file, 'w', newline='', encoding='utf-8') as file_output:
        writer = csv.writer(file_output)
        writer.writerow(['Nome Sito Autorevole', 'Dominio Autorevole', 'Dominio nel CSV', 'Numero Articoli'])
        
        totale_articoli = 0
        for corrispondenza in corrispondenze:
            writer.writerow([
                corrispondenza['nome_sito_autorevole'],
                corrispondenza['dominio_autorevole'],
                corrispondenza['dominio_csv'],
                corrispondenza['numero_articoli']
            ])
            totale_articoli += corrispondenza['numero_articoli']
        
        # Aggiungi totale
        writer.writerow([])
        writer.writerow(['TOTALE ARTICOLI', '', '', totale_articoli])
    
    return corrispondenze, totale_articoli

# USO DELLO SCRIPT
if __name__ == "__main__":
    # Sostituisci con il percorso del tuo file CSV
    file_csv_input = "conteggio_domini_GKG.csv"
    file_output = "corrispondenze_autorevoli.csv"
    
    try:
        # Esegui l'analisi
        corrispondenze, totale = analizza_csv_corrispondenze_avanzate(file_csv_input, file_output)
        
        # Stampa un riepilogo
        print(f"Trovate {len(corrispondenze)} corrispondenze")
        print(f"Totale articoli nei siti autorevoli: {totale}")
        print(f"Risultati salvati in: {file_output}")
        
        # Mostra le prime 10 corrispondenze
        print("\nPrime 10 corrispondenze per numero di articoli:")
        for i, corr in enumerate(corrispondenze[:10], 1):
            print(f"{i}. {corr['nome_sito_autorevole']}: {corr['numero_articoli']} articoli")
            
    except FileNotFoundError:
        print(f"Errore: File {file_csv_input} non trovato")
    except Exception as e:
        print(f"Errore durante l'analisi: {e}")