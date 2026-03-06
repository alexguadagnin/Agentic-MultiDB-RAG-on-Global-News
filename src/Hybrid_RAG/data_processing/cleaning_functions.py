import regex as re

import html
import unicodedata

def clean_abc_es(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'abc.es' (ABC), rimuovendo
    rumori specifici, paywall, metadati, didascalie e blocchi 
    promozionali, preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 41 pattern specifici (paywall, timestamp, byline,
      didascalie, blocchi "Oferplan", linee social, etc.).
    - Normalizzazione degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei pattern regex estratti dal JSON
    # Ho usato stringhe raw (r"...") per preservare correttamente
    # i caratteri speciali come \b, \s, \d.
    patterns_da_rimuovere = [
        r"^Esta funcionalidad es s[oó]lo para (registrados|suscriptores)\.?$",
        r"Art[íi]culo solo para suscriptores",
        r"Si ya est[áa](s|is) suscrito, inicia sesi[óo]n",
        r"Suscr[ií]bete",
        r"L[íi]mite de sesiones alcanzadas[\s\S]*?Sigue navegando",
        r"^Has superado el l[íi]mite de sesiones.*$",
        r"^Volver a intentar$",
        r"El acceso al contenido Premium est[áa] abierto por cortes[íi]a del establecimiento donde te encuentras.*",
        r"\bActualizado a las\s*\d{1,2}:\d{2}h\.?$",
        r"\b\d{2}/\d{2}/\d{4}\s+a las\s+\d{1,2}:\d{2}h\.?$",
        r"^Compartir$",
        r"^(Imagen principal|Imagen secundaria\s*\d*)\s*-.*$",
        r"^(IMAGEN|RECURSOS DEL ACTO)[:\s].*$",
        r"^[A-Za-zÁÉÍÓÚÜÑáéíóúüñ .'-]+\s*/\s*.+$",
        r"^Directo\b.*en directo.*$",
        r"Sigue en directo",
        r"^Lee la cr[oó]nica.*$",
        r"^Informa\s+[A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÜÑáéíóúüñ ]+\.?$",
        r"^M[ÁA]S INFORMACI[ÓO]N\b.*$",
        r"^Escrito por\b.*$",
        r"Redactor(a)? de ABC de Sevilla",
        r"\bGurmé ABC de Sevilla:.*",
        r"^Ámbitos\s*$",
        r"^¿QU[ÉE] TE OFRECEMOS\?\s*$",
        r"^¿CU[ÁA]NDO (PUEDO|PUEDES) (UTILIZAR|USAR|CANJEAR) MI CUP[ÓO]N\?\s*$",
        r"^¿CU[ÁA]NTOS CUPONES PUEDO COMPRAR\?\s*$",
        r"^¿D[ÓO]NDE CANJEO MI CUP[ÓO]N\?\s*$",
        r"^¿C[ÓO]MO CANJEO MI OFERTA\?\s*$",
        r"^¿CU[ÁA]NDO Y C[ÓO]MO SE PUEDEN UTILIZAR\?\s*$",
        r"^CONDICIONES DE USO.*$",
        r"^OTRAS CONDICIONES.*$",
        r"^PREGUNTAS FRECUENTES.*$",
        r"^FECHAS IMPORTANTES.*$",
        r"¿A[úu]n tienes dudas\? ?¡?Oferplan responde a todas tus preguntas!?\.?$",
        r"^(Instagram|Facebook|Web|Tel[eé]fono|Whatsapp Comercial|Correo):.*$",
        r"^Para saber m[áa]s:.*$",
        r"^Precio Oferplan.*$",
        r"Precio tarifa",
        r"^Tel[ée]fono de atenci[óo]n.*$",
        r"^Direcci[óo]n\b.*$",
        r"Departamento Comercial \|.*$",
        r"M[aá]s informaci[óo]n en:.*$",
        r"^ABC\b.*\d{2}/\d{2}/\d{4}.*$",
        r"^(Reuters|EFE)\b.*$"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo, separati da | (OR)
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Per il suggerimento "normalizzare a minuscolo" (nel matching)
        #    re.MULTILINE:  Per far funzionare ^ (inizio riga) e $ (fine riga)
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia, rimuovendo tutti i pattern
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "deduplicare spazi e linee vuote")
        #    Sostituisce qualsiasi sequenza di spazi, newline, tab
        #    con un singolo spazio.
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        # In caso di errore nella regex (improbabile ma possibile),
        # logga l'errore e restituisce il testo originale 
        # per non bloccare la pipeline.
        print(f"Errore regex nel cleaner 'clean_abc_es': {e}")
        return plain_text
    

def clean_afp_com(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'afp.com' (Agence France-Presse).
    Rimuove header/footer multilingua, link legali, newsletter, 
    e linee di branding AFP, preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 18 pattern specifici (leggi anche, newsletter, 
      login, share, cookies, privacy, copyright AFP, etc.).
    - Normalizzazione e deduplicazione degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei pattern regex estratti dal JSON
    # (Trattati come stringhe raw Python per una corretta interpretazione 
    # dei caratteri di escape come \s e \b)
    patterns_da_rimuovere = [
        r"^\s*(Lire|Voir)\s+(aussi|la suite|plus)\s*:?.*$",
        r"^\s*(En savoir plus|Plus d'info(?:rmations)?)\s*:?.*$",
        r"^\s*(Leia\s+(mais|tamb[eé]m)|Saiba mais|Ver (?:mais|tamb[eé]m))\s*:?.*$",
        r"^\s*(Recevez|Receba|Assine|Inscrivez-vous|Inscreva-se).{0,60}\bnewsletter\b.*$",
        r"^\s*(Se connecter|Connexion|S'inscrire|Cr[eé]er un compte)\s*$",
        r"^\s*(Entrar|Acessar|Iniciar sess[aã]o|Criar conta|Cadastrar-se)\s*$",
        r"^\s*(Partager|Partagez|Compartilhe|Compartilhar).*(article|artigo)?\s*.*$",
        r"^.*\b(cookies?|politique des cookies|pol[ií]tica de cookies)\b.*$",
        r"^.*\b(Politique de confidentialit[eé]|Pol[ií]tica de privacidade|Mentions l[eé]gales|Termos de uso|Condi[cç][õo]es de uso)\b.*$",
        r"^\s*©\s*AFP\b.*$",
        r"^.*\b(Tous droits r[eé]serv[eé]s|Todos os direitos reservados)\b.*$",
        r"^.*\b(Suivez[- ]?nous|Siga[- ]?nos)\b.*$",
        r"^.*\b(Contactez[- ]?nous|Fale conosco|Entre em contato|Parlez[- ]?nous de votre projet|Conte-nos mais sobre seu projeto)\b.*$",
        r"^.*\b(En savoir plus sur|Saiba mais sobre)\b.*$",
        r"^\s*(AFPTV|AFP Forum|AFP Services|AFP Focus|AFP Fact Check)\b.*$",
        r"^\s*(FR|EN|ES|PT|AR|DE|IT)(?:\s*[|/]+\s*(FR|EN|ES|PT|AR|DE|IT))+.*$",
        r"^.*\b(RSS|Plan du site|Mapa do site)\b.*$"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo, separati da | (OR)
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Per "normalizzare minuscolo" nel matching
        #    re.MULTILINE:  Per far funzionare ^ (inizio riga) e $ (fine riga)
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia, rimuovendo tutti i pattern
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "deduplicare spazi e linee vuote", "unire righe spezzate")
        #    Sostituisce qualsiasi sequenza di spazi, newline, tab
        #    con un singolo spazio.
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_afp_com': {e}")
        return plain_text
    


def clean_asahi_com(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'asahi.com' (Asahi Shimbun).
    Rimuove banner TCF, blocchi PR, crediti redazionali (giapponesi 
    e inglesi), metadati, blocchi di concorsi e link/note 
    a piè di pagina, preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 39 pattern specifici.
    - Normalizzazione e deduplicazione degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei pattern regex estratti dal JSON
    patterns_da_rimuovere = [
        r"You can choose how your personal data is used",
        r"TCF vendors",
        r"Vendor preferences",
        r"FCCDCF",
        r"amp-store",
        r"^〈PR〉.*$",
        r"^PR[:：].*$",
        r"^PROMOTION$",
        r"^Presented by .*$",
        r"^(カジュアルウェア|週末の過ごし方|接待と手土産|お酒|バッグ)\s*$",
        r"^\d{4}\.\d{2}\.\d{2}\s*$",
        r"^＃\d+\s*$",
        r"掲載した商品はすべて税込み価格です。",
        r"^価格は.*(税込|円).*$",
        r"^問[／/:].*$",
        r"^(取材協力|取材&文)[／＝:].*$",
        r"^(Text|Photograph|Photo|Photography|Styling|Hair & Make-up|Hair|Make-up|Direction|Edit|Edit\/)[:／:].*$",
        r"写真提供[：:].*$",
        r"^住所[：:].*$",
        r"^電話番号[：:].*$",
        r"^営業時間[：:].*$",
        r"^(定休日|会場|会期|観覧料|公式サイト)[：／:].*$",
        r"^◆.*(公式サイト|オフィシャル|Instagram|X公式).*$",
        r"^■(締め切り|賞品|当選者|当選発表|応募条件)[：:].*$",
        r"^応募はこちら\s*$",
        r"＆MEMBER",
        r"＆MILE",
        r"^関連(記事)?\s*$",
        r"^あわせて読みたい\s*$",
        r"^おすすめ記事\s*$",
        r"^＼.*／$",
        r"^(プレゼント・アンケート|講座・イベント|これまでの活動をリポート).*$",
        r"^(Previous|Next\d+)$",
        r"^(受信中|NEW|試し読み|サンプル|メールを受け取る).*$",
        r"^※.*$",
        r"^～?「.+」取材協力者募集～?$",
        r"^ご応募はこちら$",
        r"^https?://\S+\s*$",
        r"（企画・制作：.*）",
        r"^提供[：:].*$"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo, separati da | (OR)
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: I pattern contengono un mix di maiuscole/minuscole
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia, rimuovendo tutti i pattern
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "deduplicare spazi e normalizzare interruzioni di riga")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_asahi_com': {e}")
        return plain_text
    


def clean_bbc_com(plain_text: str) -> str:
    """
    Pulisce il testo multilingue estratto da 'bbc.com' (BBC).
    Rimuove didascalie, crediti, metadati (autore/timestamp), 
    box di iscrizione (WhatsApp, notifiche), banner cookie e blocchi 
    di embed (es. X/Twitter), preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 23 pattern multilingue specifici.
    - Normalizzazione e deduplicazione degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei pattern regex estratti dal JSON
    patterns_da_rimuovere = [
        r"^(?:Fuente de la imagen|Pie de foto|Crédito,|Legenda da foto|Nguồn hình ảnh|Chụp lại hình ảnh|Ảnh chụp màn hình|Chụp lại video|التعليق على الصورة|صدر الصورة،|،تصویر کا ذریعہ|،تصویر کا کیپشن|تصویر کا ذریعہ|تصویر کا کیپشن)\b.*",
        r".*\/(?:Getty Images|Reuters|EPA|AFP|AP|Shutterstock|BBC News|Bloomberg|Handout|PA Media)\s*\/.*",
        r"^(?:Información del artículo|Article Information|Article Information Author|Autor,|Título del autor|Tác giả,|Vai trò,|Author,|Role,|مصنف,|عہدہ,)\b.*",
        r"Recibe el mejor contenido.*?Fin de WhatsApp",
        r"Agora você pode receber as notícias da BBC News Brasil no seu celular.*?Fim do Whatsapp!?",
        r"Haz clic aquí para leer más historias de BBC News Mundo\..*",
        r"Suscríbete aquí.*",
        r"Y recuerda que puedes recibir notificaciones.*actívalas\.",
        r"بی بی سی اردو کی خبروں.*سبسక్రائب کرنے.*مواد پر جائیں",
        r"تابعوا التغطية الشاملة.*اضغط هنا",
        r"يمكنكم مشاهدة الحلقات اليومية.*",
        r"يستحق الانتباه نهاية",
        r"Bạn.*có thể nhận.*thông báo.*",
        r"(?:Nhấp|Bấm).*để.*(đăng ký|subscribe).*",
        # Questo pattern è multi-riga (grazie a [\s\S])
        r"هل تسمح بعرض المحتوى من X\?[\s\S]*?نهاية", 
        r"^(?:Déjanos saber si aceptas las cookies|Diga-nos se concorda com o uso de cookies).*",
        r"^(?:Línea|Línea gris|Raya gris|Línea ?gris)$",
        # Pattern molto complesso per i timestamp multilingua
        r"^\s*(?:Hace \d+ (?:minutos?|horas?)|_... (complex timestamp regex) ..._ |\d+ منٹ قبل)\s*$",
        r"neste vídeo\..*",
        r"Clique para se inscrever",
        r"Chụp lại video,",
        r"Ảnh chụp màn hình"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Per i nomi propri (es. Getty Images)
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "unire righe spezzate, normalizzare spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_bbc_com': {e}")
        return plain_text



def clean_bloomberg_com(plain_text: str) -> str:
    """
    Pulisce il testo multilingue estratto da 'bloomberg.com'.
    Rimuove metadati (sezioni, byline, editor, traduttori),
    didascalie, crediti foto (es. Getty), timestamp e disclaimer
    legali, preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 14 pattern specifici.
    - Normalizzazione e deduplicazione degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei pattern regex estratti dal JSON
    # (Usando stringhe raw r"..." per un'interpretazione corretta)
    patterns_da_rimuovere = [
        # Intestazioni di sezione (EN, ES)
        r"^(?:Management\s*&\s*Work|The Big Take|Bloomberg Green|More On Bloomberg(?: Television)?|Opinion|Markets|Technology|Politics|Business|Pursuits|CityLab)\b.*$",
        r"^(?:Más (?:en|de) Bloomberg)\b.*$",
        # Byline Autore (ES, EN, DE)
        r"^(?:Por|By|Von)\s+[A-ZÁÉÍÓÚÑÄÖÜ][^\n]*$",
        # Crediti Illustrazione (ES, PT, EN)
        r"^(?:Ilustraciones?|Ilustração(?:es)?|Illustrations?)\s+(?:de|by)\s+[^\n]*$",
        # Crediti Editor/Traduzione (ES, EN)
        r"^(?:Editor(?:es)?(?:\s+imágenes)?|Editado por|Editores?|Editor imágenes|Productor(?:a)?\s+web|Traducción\s+editada\s+por)[:：]?\s*[^\n]*$",
        # Righe "Con assistenza di..." (ES, PT, DE, EN)
        r"^[—–-]\s?(?:Con|Com|Mit|With)\b[^\n]*$",
        # Timestamp (multi-timezone)
        r"^(?:[A-Z][a-z]+|[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)\s+\d{1,2},\s+\d{4}\s+(?:at|a las|às)\s+\d{1,2}:\d{2}\s+(?:AM|PM)\s+[A-Z]{2,4}\s*$",
        # Descrizioni illustrazioni (EN, ES, PT)
        r"\billustration of\b[^\n]*",
        r"\bilustración de\b[^\n]*",
        r"\bilustração de\b[^\n]*",
        # Crediti Foto
        r"^.*\b(Foto|Photo)[:：]\s*.*$",
        r"^.*\bGetty Images\b.*$",
        # Disclaimer (es. "(Bloomberg LP)")
        r"\([^)]*Bloomberg (?:LP|Philanthropies)[^)]*\)"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Per "uniformare minuscolo" e matchare 
        #                   nomi e crediti (es. Getty)
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "normalizzare spazi bianchi e righe vuote")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_bloomberg_com': {e}")
        return plain_text
    


def clean_clarin_com(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'clarin.com' (Clarín).
    Rimuove link social (URL, hashtag, @menzioni), didascalie, 
    crediti foto, blocchi newsletter e link promozionali interni 
    ("Mirá..."), preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 24 pattern specifici.
    - Normalizzazione e deduplicazione degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei pattern regex estratti dal JSON
    patterns_da_rimuovere = [
        r"^Mirá también.*$",
        r"^Mirá el paso a paso.*$",
        r".*pic\.twitter\.com/\w+.*",
        r"^—\s?.*\(?@[A-Za-z0-9_]+\)?.*",  # Leggermente corretto per includere parentesi opzionali
        r"#[^\s]+",
        r"@[^\s]+",
        r"https?://\S+",
        r"^Foto\s?:.*$",
        r"\((AP|Reuters|EFE|AFP) Photo[^)]*\)",
        r"^Fuente:\s*(AFP|AP|Reuters|Clar[ií]n).*",
        r".*📺.*",
        r"^Newsletter Clar[ií]n.*$",
        r"^QUIERO RECIBIRLO.*$",
        r"^Noticias destacadas.*$",
        r"^Sobre la firma.*$",
        r"^Recomendamos chequear la edición.*$",
        r"^©.*$",
        r"^Contactanos.*$",
        r"^RESUMEN\b.*$",
        r"^DESTACADOS\b.*$",
        r"^DATOS\b.*$",
        r"^FAQ\b.*$",
        r"^GLOSARIO\b.*$",
        r"\(AP Photo/[^)]*\)"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Come da "normalizzare a minuscolo"
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "comprimere spazi e righe vuote")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_clarin_com': {e}")
        return plain_text
    



def clean_dn_se(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'dn.se' (Dagens Nyheter).
    Rimuove blocchi newsletter, box "Fakta", crediti foto, 
    link "leggi altro" e altre intestazioni di sezione, 
    preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 11 pattern specifici con flag inline (es. ?is, ?im).
    - Normalizzazione e deduplicazione degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista dei pattern regex estratti dal JSON.
    # I flag (?is) e (?im) sono inclusi direttamente nei pattern.
    patterns_da_rimuovere = [
        # Blocchi newsletter e "Fakta" (multi-riga grazie a ?s)
        r"(?is)Få mer kultur med våra nyhetsbrev[\s\S]*?Här kan du registrera dig[^\n]*\.",
        r"(?is)^Fakta[:\.]?[ \n]*[\s\S]*?(?=\n{2,}|$)",
        # Link "Leggi altro" (?i=ignorecase, ?m=multiline)
        r"(?im)^Läs (fler|mer)[^\n]*$",
        # Crediti e intestazioni
        r"(?im)^Foto:[^\n]*$",
        r"(?im)^DN Debatt\.[^\n]*$",
        r"(?im)^Barn/Ung\.[ \t]*$",
        r"(?im)^Bästa spår:[^\n]*$",
        r"(?im)^Här kan du registrera dig[^\n]*$",
        # Liste puntate (spesso usate per link)
        r"(?im)^\s*•\s.*$",
        # Contenuti sponsorizzati
        r"(?im).*brandstudio.*$",
        # Fonti
        r"(?im)^Källa:[^\n]*$"
    ]

    try:
        # Applica ogni pattern sequenzialmente
        # Questo è necessario per rispettare i flag inline
        # specifici di ogni pattern (es. (?is) vs (?im)).
        for pattern in patterns_da_rimuovere:
            text = re.sub(pattern, '', text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "normalizzare spaziature e interruzioni di riga")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_dn_se': {e}")
        return plain_text



def clean_corriere_it(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'corriere.it' (Corriere della Sera).
    Rimuove avvisi di accesso, inviti alla newsletter, copyright,
    byline autore, timestamp e didascalie foto, preparando
    il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 20 pattern specifici.
    - Normalizzazione e deduplicazione degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei pattern regex estratti dal JSON
    patterns_da_rimuovere = [
        # Avviso multi-riga limite accesso
        r"Questo messaggio verrà visualizzato su un altro dispositivo/accesso[\s\S]*?Ti consigliamo di cambiare la tua password cliccando qui",
        # Newsletter e registrazione
        r"^Per ricevere.*newsletter.*$",
        r"^iscriviti alle newsletter.*$",
        r"Salva questo articolo e leggilo quando vuoi\. Il servizio è dedicato agli utenti registrati\.",
        # Copyright
        r"© RIPRODUZIONE RISERVATA",
        # Byline autore (case insensitive)
        r"^\s*di [A-ZÀÈÉÌÒÙ][^\n]+$",
        r"^\s*DI [A-ZÀÈÉÌÒÙ][^\n]+$",
        r"^\s*DAL NOSTRO INVIATO.*$",
        # Timestamp e aggiornamenti
        r"^\s*\d{1,2} [a-zàéìòù]+ 202\d\s*\|\s*\d{2}:\d{2}.*$",
        r"^.*Aggiornata il.*$",
        # Didascalie e crediti foto (case insensitive)
        r"^\s*desc img.*$",
        r"\(foto[^\)]*\)",
        r"\(Foto[^\)]*\)",
        r"^\s*/ Foto.*$",
        r"^\s*/ La copertina.*$",
        # Link interni e CTA (Call To Action)
        r"^\s*APPROFONDISCI CON IL PODCAST.*$",
        r"^\s*(LEGGI ANCHE|Leggi (qui|la storia|l'articolo).*)$",
        r"^\s*Ultimi pubblicati.*$",
        r"^\s*Editoriali e commenti di oggi.*$",
        r"^\s*\—\s*Questa diretta.*$" # Per i live blog
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Per 'di/DI', 'foto/Foto', '© riproduzione...'
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "Normalizzare spazi e interruzioni di riga")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_corriere_it': {e}")
        return plain_text
    



def clean_elcomercio_pe(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'elcomercio.pe' (El Comercio, Perù).
    Rimuove newsletter, link social (Twitter), crediti agenzie (AFP/EFE),
    didascalie, URL, separatori e probabili tag SEO,
    preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 25 pattern specifici.
    - Normalizzazione e deduplicazione degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei pattern regex estratti dal JSON
    patterns_da_rimuovere = [
        r"^Newsletter\b.*$",
        r"Francisco Sanz analiza cómo los eventos internacionales transforman el mundo, cada martes\.",
        r"^(PUEDES VER|MIRA AQUÍ|MÁS INFORMACIÓN|PERFIL|VIDEO RECOMENDADO|VIDEO RECOMENDADO SOBRE EL AUTOR|LEA TAMBIÉN):?.*$",
        r"^SOBRE EL AUTOR.*$",
        r"^(Agencia|Agence)\s?(AFP|EFE)\b.*$",
        r"x-twitter",
        r"pic\.twitter\.com/\S+",
        r"—\s?[^\n]*@[^\s)]+\)[^\n]*$",
        r"^Para más datos viajeros, sígueme.*$",
        r"^[Ss]ígueme en (las )?redes sociales:.*$",
        r"^(Instagram|Facebook|Youtube):\s?.*$",
        r"^Foto:.*$",
        r" ?/ (Foto|EFE|AFP|REUTERS|AP|AFPTV|EPA)[^\n]*$",
        r"\((Foto|EFE|AFP|REUTERS|AP|AFPTV|EPA)[^)]*\)",
        r"^.*Xiaomi 15 Ultra.*$", # Probabile linea promozionale
        r"^Publicado en Diario El Comercio el .*$",
        r"^Diario El Comercio\. Todos los derechos reservados\.$",
        r"^(?:_{5,}|-{5,}|—{3,}).*$", # Separatori
        r"^SI USTED CUMPLE AÑOS HOY ES UNA PERSONA:.*$", # Oroscopo
        r"https?://\S+", # URL generici
        # Pattern euristico per rimuovere righe di tag/SEO
        r"^(?:[A-Za-zÁÉÍÓÚÜÑáéíóúüñ0-9'./-]+\s+){12,}$", 
        r"^Comercio Lima Peru.*$",
        r"^ESTADÍSTICAS DE LAS GANADERÍA.*$",
        r"^Estadistica Nuñez del Cuvillo 2025_blog.*$"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Come da "normalizzare minuscolo"
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "deduplicare spazi e righe vuote")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_elcomercio_pe': {e}")
        return plain_text
    

def clean_elmundo_es(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'elmundo.es' (El Mundo).
    Rimuove byline, timestamp, crediti foto, CTA social/newsletter,
    blocchi paywall e artefatti (es. '//'), preparando il testo 
    per il chunking.

    Regole applicate (basate sul jsonl):
    - Decodifica delle entità HTML (es. &amp; -> &).
    - Rimozione di 12 pattern specifici.
    - Normalizzazione e deduplicazione degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # 1. Decodifica entità HTML (come da 'altri_suggerimenti')
    #    Converte &amp; -> &, &quot; -> ", ecc.
    try:
        text = html.unescape(plain_text)
    except Exception:
        text = plain_text # Fallback se unescape fallisce

    # Lista dei pattern regex estratti dal JSON
    patterns_da_rimuovere = [
        r"^Por:\s*.*$",                            # Byline autore
        r"^Actualizado:\s*\d{2}/\d{2}/\d{4}\s*\d{2}:\d{2}\s*horas$", # Timestamp (intera riga)
        r"\bActualizado:\s*\d{2}/\d{2}/\d{4}\s*\d{2}:\d{2}\s*horas\b", # Timestamp (inline)
        r"^(Fotos?|Foto|Photo|Crédito|Créditos|REALIZACIÓN|Realización):\s*.*$", # Crediti
        r"^(?:Álbum|Album)\b.*$",                 # Link ad Album (combinati)
        r"\/\/\s*$",                              # Artefatto "//" a fine riga
        r"^(Síguenos en|Sigue a EL MUNDO en|Suscríbete|Boletín|Newsletter|Inicia sesión|Regístrate)\b.*$", # CTA Social/Login
        r"^(Leer más|Lee también|Ver más|Ver fotos|Ver galer[ií]a|Ver v[íi]deo)\b.*$", # CTA Link interni
        r"^(Más información|Te puede interesar|En directo|Relacionado[s]?|Sigue en directo)\b.*$", # CTA Correlati
        r"^(Comentarios(?:\s*\(\d+\))?|Comentar)\b.*$", # Sezione Commenti
        r"^(Contenido exclusivo para.*|Este contenido.*suscriptor|Suscr[íi]bete.*para seguir leyendo)\b.*$", # Paywall
        r"^D[óo]nde ver por TV.*$"                 # CTA specifici
    ]

    try:
        # 2. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 3. Compila la regex
        #    re.IGNORECASE: Per 'REALIZACIÓN'/'Realización', 'Fotos'/'Foto'
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 4. Applica la pulizia
        text = compiled_regex.sub('', text)

        # 5. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "normalizzare spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_elmundo_es': {e}")
        return plain_text # Ritorna il testo pre-regex in caso di errore
    



def clean_elpais_com(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'elpais.com' (El Pais).
    Rimuove blocchi paywall/newsletter, URL, embed di social media
    (X/Twitter) e vari formati di crediti di agenzie (AFP, EFE, 
    Reuters...), preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 25 pattern specifici.
    - Normalizzazione e deduplicazione degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei pattern regex estratti dal JSON
    # Corretti per l'uso in stringhe raw Python (r"...")
    patterns_da_rimuovere = [
        # Blocchi Paywall e Accesso (multi-riga)
        r"Tu suscripción se está usando en otro dispositivo[\s\S]*?términos y condiciones de la suscripción digital\.",
        r"¿Quieres añadir otro usuario a tu suscripción\?",
        r"¿Tienes una suscripción de empresa\? Accede aquí.*",
        r"cambiar tu contraseña aquí\.",
        r"Para seguir leyendo este artículo de Cinco Días necesitas una suscripción Premium de EL PAÍS",
        # Blocchi Newsletter
        r"^Esta es la versión web de .*newsletter.*$",
        r"^Este es un envío de la newsletter.*$",
        r"Si quiere suscribirse, puede hacerlo en este enlace\.",
        r"apúntate para recibirlo",
        r"EL PAÍS ofrece de forma gratuita .*",
        # Link Social e promo
        r"También puedes seguirnos en Instagram y Flipboard\!? ¡No te pierdas lo mejor de Verne!?",
        r"^Nuestras recomendaciones:?$",
        r"^Flecha.*$",
        r"^El País$",
        # Embed X/Twitter (pattern complesso)
        r"—\s.*?\(@.*?\)\s[A-Za-zÁÉÍÓÚÜÑa-z]+\s\d{1,2},\s\d{4}.*$",
        r"pic\.twitter\.com/\S+",
        r"https?://t\.co/\S+",
        # URL Generici (cattura anche quello sopra, ma lo teniamo per sicurezza)
        r"https?://\S+",
        # Artefatti di scraping
        r"data-link-track-dtm",
        # Crediti Agenzie (in vari formati)
        r"\s*\((REUTERS|Reuters|EFE|AFP|AP|APN|DPA|Europa Press|Getty Images|LaPresse)\)\s*",
        r"^.*? / .*?(AP|AFP|REUTERS|EFE|Getty Images|Europa Press|APN|POOL|DPA|LaPresse).*?$",
        r"^.*? / [A-Z0-9ÁÉÍÓÚÜÑ .'-]+$", # Crediti generici "Nome / Testata"
        # Righe promozionali specifiche
        r"Breakingviews\. Las opiniones son suyas\..*",
        r"Vídeo explicativo.*aquí"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Per 'Reuters'/'REUTERS', ecc.
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "normalizzare spazi bianchi, deduplicare righe")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_elpais_com': {e}")
        return plain_text
    



def clean_eltiempo_com(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'eltiempo.com' (El Tiempo, Colombia).
    Applica 44 pattern sequenziali per rimuovere un'ampia gamma di 
    rumore: blocchi cookie TCF, disclaimer chatbot, byline autore, 
    crediti foto, CTA social/newsletter e link "leggi anche".
    Rispetta i flag regex inline (?i) e (?s).

    Regole applicate (basate sul jsonl):
    - Rimozione sequenziale di 44 pattern con flag inline.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista dei pattern regex estratti dal JSON.
    # I flag inline (es. ?i, ?s) vengono rispettati applicandoli
    # sequenzialmente con re.sub().
    patterns_da_rimuovere = [
        r"(?i)la url ha sido copiada en el portapapeles",
        r"(?i)ingresa o regístrate acá para seguir este blog\.?$",
        r"(?i)sigue toda la información de\s+[A-ZÁÉÍÓÚÑa-záéíóúñ ]+\s+en Facebook y X,? o en nuestra newsletter semanal\.?$",
        r"(?i)más noticias (?:en|de) el tiempo\.?$",
        r"(?i)consulte? más noticias(?: de interés)?\.?$",
        r"(?i)otra información que le puede interesar:?$",
        r"(?i)este video le puede interesar:?$",
        r"(?i)lea también:?\s.*$",
        r"(?i)le(?: |\s)puede interesar:?\s.*$",
        r"(?i)también te (?:puede|podría) interesar:?\s.*$",
        r"(?i)\(siga leyendo:.*?.\)$",
        r"(?i)^redacci[óo]n\b.*$",
        # Questo pattern è case-sensitive (manca ?i) per matchare ruoli in maiuscolo
        r"^[A-ZÁÉÍÓÚÑ .'-]+\s+(SUBEDITORA|EDITOR|CORRESPONSAL|REPORTERO|REDACTOR)\b.*$",
        r"(?i)escríbanos a\s+\S+",
        r"(?i)en X:?\s*@\S+",
        r"(?i)\bX:?\s*@\S+",
        r"pic\.twitter\.com/\S+",
        r"—\s.*?\(?@.*?\)?.*\d{4}", # Corretto per parentesi opzionali
        r"\s*/\s*Foto:\s*/.*$",
        r"\s*/\s*Cortes[ií]a:?\s*/.*$",
        r"\s*/\s*Archivo\s*/.*$",
        r"(?i)^video:\s.*$",
        r"^(Image|Imagen|Imágen)\s.*$",
        r"^(T[ií]tulo)\s.*$",
        r"(?i)foto de referencia\.$",
        r"(?i)este contenido fue reescrito.*inteligencia artificial.*$",
        r"(?i)url ha sido copiada en el portapapeles",
        r"(?i)ya tienes una cuenta vinculada a el tiempo.*iniciar sesi[óo]n",
        r"(?i)has alcanzado tu l[ií]mite diario.*chat ?bot",
        r"(?i)has? excedido el m[áa]ximo de peticiones",
        r"(?i)error\s*505",
        r"(?i)procesando tu pregunta.*un momento, por favor",
        r"(?i)con el envío de tus consultas, aceptas los t[ée]rminos y condiciones del chat.*$",
        r"(?i)este chat tiene finalidades .* informativas\.?$",
        r"(?i)de acuerdo con las políticas de la ia que usa el tiempo.*$",
        r"(?i)^en este portal utilizamos datos de navegaci[óo]n\s*/\s*cookies.*$",
        # Cookie Banner TCF
        r"^You can choose how your personal data is used\.$",
        r"^Vendor preferences.*$",
        r"^TCF vendors.*$",
        r"^Cookie duration:.*$",
        r"^Data collected and processed:.*$",
        r"^View details \| (Storage details \| )?Privacy policy.*$",
        # Disclaimer legale multi-riga (flag ?s = DOTALL)
        r"(?s)ETCE no se responsabiliza[\s\S]*$",
        r"(?i)^compartir post\s*$"
    ]

    try:
        # Applica ogni pattern sequenzialmente per rispettare i flag inline
        for pattern in patterns_da_rimuovere:
            text = re.sub(pattern, '', text)

        # Pulizia finale e normalizzazione spazi bianchi
        # (Come da "unificar espacios y saltos de línea")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_eltiempo_com': {e}")
        return plain_text
    


import re

def clean_eluniversal_com_mx(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'eluniversal.com.mx' (El Universal).
    Applica 27 pattern sequenziali per rimuovere link "leggi anche",
    rumore social (X, Instagram), didascalie, inviti a newsletter
    e blocchi cookie TCF, preparando il testo per il chunking.
    
    Rispetta i flag regex inline (?i) forniti nel jsonl.

    Regole applicate (basate sul jsonl):
    - Rimozione sequenziale di 27 pattern con flag inline.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista dei pattern regex estratti dal JSON
    # Vengono applicati sequenzialmente per rispettare i flag inline
    patterns_da_rimuovere = [
        r"(?i)\b(Lee|Leer)\s+(también|tambien|más|mas):?.*$",
        r"(?i)^Sigue leyendo:.*$",
        r"(?i)^Te interesa:.*$",
        r"(?i)^Más Información\b.*$",
        r"^\s*\[Publicidad\]\s*$",
        r"(?i)^Noticias según tus intereses.*$",
        r"^.*\|\s*El Universal\s*$",
        r"pic\.twitter\.com/\S+",
        r"^https?://t\.co/\S+",
        r"^[—–\\-]\s.*@.*$", # Escape di \-
        r"(?i)^(Foto|Fotos|Crédito|Crédito_|Ilustración|Imagen|Captura de Pantalla|Video):?.*$",
        r"(?i)^Instagram\s@.*$",
        r"(?i)^X:\s*@.*$",
        r"(?i)^Facebook:\s*.*$",
        r"(?i)^Únete a nuestro canal.*$",
        r"(?i)^Suscríbete aquí:.*$",
        r"https?://www\.eluniversal\.com\.mx/newsletters",
        r"(?i)^Recibe todos los viernes Hello Weekend.*$",
        r"(?i)^Fuente:\s.*$",
        r"(?i)^Teléfono del Consumidor.*$",
        # Cookie Banner TCF
        r"^You can choose how your personal data is used\..*$",
        r"(?i)TCF vendors",
        r"(?i)^Vendor preferences.*$",
        r"(?i)^This site or app wants your permission.*$",
        r"(?i)^Confirm our vendors.*$",
        r"(?i)Your choices will be invalidated after 390 days",
        r"(?i)^Correo:\s*\S+@\S+"
    ]

    try:
        # Applica ogni pattern sequenzialmente
        for pattern in patterns_da_rimuovere:
            text = re.sub(pattern, '', text)

        # Pulizia finale e normalizzazione spazi bianchi
        # (Come da "comprimere spazi e righe vuote")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_eluniversal_com_mx': {e}")
        return plain_text
    


def clean_emol_com(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'emol.com' (El Mercurio).
    Decodifica le entità HTML, quindi applica 26 pattern sequenziali
    per rimuovere breadcrumb, banner cookie, CTA, byline, crediti foto
    e altro rumore, preparando il testo per il chunking.
    
    Rispetta i flag regex inline (?i) e l'assenza di flag
    (case-sensitive) dove specificato nel jsonl.

    Regole applicate (basate sul jsonl):
    - Decodifica delle entità HTML (es. &amp; -> &).
    - Rimozione sequenziale di 26 pattern con flag inline/misti.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # 1. Decodifica entità HTML (come da 'altri_suggerimenti')
    try:
        text = html.unescape(plain_text)
    except Exception:
        text = plain_text # Fallback

    # 2. Lista dei pattern regex estratti dal JSON
    # Vengono applicati sequenzialmente per rispettare i flag inline
    patterns_da_rimuovere = [
        # Pattern case-sensitive (senza ?i)
        r"^(Chile|Mundo|Economía|Deportes|Espectáculos|Tendencias|Tecnología|Autos|Nacional|Internacional|Servicios|Opinión)(\s*\|\s*(Chile|Mundo|Economía|Deportes|Espectáculos|Tendencias|Tecnología|Autos|Nacional|Internacional|Servicios|Opinión))+$",
        # Pattern case-insensitive (con ?i)
        r"(?i)(cookies?|política de cookies|política de privacidad|tu privacidad|configuraci[oó]n de cookies|aceptar cookies|rechazar cookies)",
        r"^(Compartir|Compartir en|Comparte esta noticia|Síguenos en|Seguir en|Imprimir|Enviar por (correo|email)|Copiar enlace|Comentar|Ver comentarios)\b.*$",
        r"^(Lee también|Revisa también|Te puede interesar|Relacionad[oa]s|Notas relacionadas|Artículos relacionados|Ver más|Más en|Lo más leído|Lo más visto)[:：]?\s.*$",
        r"(?i)^(suscr[ií]bete|suscripci[oó]n|newsletter|bolet[ií]n|recibe nuestras noticias|ingresa tu (correo|email))\b.*$",
        r"(?i)^(inicia sesi[oó]n|reg[ií]strate|ingresa para comentar|debes iniciar sesi[oó]n|crear cuenta|olvidaste tu contrase(?:ña|na))\b.*$",
        r"^(Normas de la comunidad|Política de comentarios|Reglas de convivencia)\b.*$",
        r"(?i)©.*(El\s+Mercurio|Emol).*",
        r"(?i)^(El\s+Mercurio\s+S\.A\.P\.|Emol\.com)\b.*$",
        r"\[(FOTO|FOTOS|VIDEO|VIDEOS|GALER[ÍI]A|IM[ÁA]GENES)\]", # Case-sensitive
        r"^(Foto|Crédito|Créditos|Imagen|Fuente de la imagen):\s.*$", # Case-sensitive
        r"^(Nota del editor|N\.\s*de la R\.|Nota de la redacci[oó]n):\s.*$", # Case-sensitive
        r"^(Publicado|Actualizado|Última (actualización|hora))[:：]\s.*$", # Case-sensitive
        r"^[A-ZÁÉÍÓÚÑ]{3,}(,?\s*\d{1,2}\s+de\s+[a-záéíóúñ]+\s+de\s+\d{4})?\.-", # Dateline (Case-sensitive)
        r"^(Publicidad|Publicidad relacionada|Anuncio|Publicación patrocinada)\b.*$", # Case-sensitive
        r"(?i)^(t[eé]rminos y condiciones|pol[ií]tica de privacidad|pol[ií]tica de cookies|contacto|ayuda|preguntas frecuentes|mapa del sitio)\b.*$",
        r"(?i)((haz|haga|pincha|presiona)\s+clic\s+aqu[íi]|ver\s+aqu[íi])",
        r"^Secci[oó]n:\s*(Deportes|Econom[ií]a|Espect[aá]culos|Tendencias|Autos|Tecnolog[ií]a)\b.*$", # Case-sensitive
        r"^(Por|Autor(?:a)?|Redacci[oó]n)\s+[^\n,]+(,\s*(Emol|El\s+Mercurio))?\.?$", # Byline (Case-sensitive start)
        r"^\d{1,2}:\d{2}\s*hrs\.?$", # Timestamp
        r"^P(?:á|a)gina\s+\d+\s+de\s+\d+.*$", # Paginazione
        r"^(Fuente|V[ií]a):\s.*$", # Fonte (Case-sensitive)
        r"^(Te recomendamos|Recomendados|Contenido recomendado|Contenido patrocinado)\b.*$", # Case-sensitive
        r"(?i)^Emol\s*TV\b.*$",
        r"^[0-9]+\s+comentarios?$" # Conteggio commenti
    ]

    try:
        # 3. Applica ogni pattern sequenzialmente
        for pattern in patterns_da_rimuovere:
            text = re.sub(pattern, '', text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "normalizzare spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_emol_com': {e}")
        # Ritorna il testo originale in caso di fallimento regex
        return plain_text
    


def clean_faz_net(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'faz.net' (Frankfurter Allgemeine Zeitung).
    Rimuove blocchi paywall/registrazione, placeholder GDPR, timestamp,
    crediti di agenzie (dpa, Reuters) e intestazioni di sezioni di test,
    preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 21 pattern specifici (incl. paywall e GDPR).
    - Normalizzazione e deduplicazione degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei pattern regex estratti dal JSON
    # (Le doppie backslash \\ del JSON diventano singole \
    # in una stringa raw Python r"...")
    patterns_da_rimuovere = [
        # Timestamp / Tempo di lettura
        r"\b(?:Von\s+)?\d{2}\.\d{2}\.\d{4},\s*\d{2}:\d{2}\s*Lesezeit:\s*\d+\s*Min\.",
        # Byline / Dateline
        r"^Von\s+[^\n]*\d{1,2}\.\s*[A-Za-zÄÖÜäöü]+\s*\d{4}\s*·.*$",
        # Blocco GDPR multi-riga
        r"DSGVO Platzhalter[\s\S]*?Weitere Informationen\s*\.",
        # Blocco contenuti esterni
        r"^Externer Inhalt von [A-Za-zÄÖÜäöü]+.*$",
        # Blocchi Paywall e Registrazione
        r"^Ohne Abo weiterlesen.*$",
        r"^Dies ist kein Abo\..*$",
        r"^Ihre Registrierung ist komplett kostenlos.*$",
        r"^Oder\s*\d+\s*Monate.*FAZ\+.*$",
        r"^Zugang zu allen FAZ\+ Beiträgen.*$",
        r"jetzt\s*nur\s*0,99\s*€",
        r"^- Mit einem Klick online kündbar$",
        # Crediti Agenzie (inizio riga o inline)
        r"^(?:Reuters|AFP|dpa|EPA|AP|Picture Alliance)(?:\s*/\s*[A-Za-zÄÖÜäöü.\- ]+)*$",
        r"(?:^|[\s.])(Reuters|AFP|dpa|EPA|AP|Picture Alliance)(?:\s*/\s*[A-Za-zÄÖÜäöü.\- ]+)+$",
        # Paginazione
        r"^\s*\d+\s+von\s+\d+\s+.*$",
        # Intestazioni Sezioni Test/Recensioni
        r"^Test:\s.*$",
        r"^(Vergleichstabelle|Kurzübersicht|Produktdetails einblenden|So haben wir getestet|Die wichtigsten Fragen|Außerdem getestet|Unser Favorit|Alternativen|Auch interessant)\b.*$",
        # Varie
        r"^Update\s+\d{2}/\d{4}.*$",
        r".*Gruppenbild.*", # Didascalia "Foto di gruppo"
        r".*Gruppenfoto.*", # Didascalia "Foto di gruppo"
        r"^Quelle:\s*FAZ\.NET/.*$", # Fonte
        r"^\d+\)\s.*$" # Liste numerate
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Per 'Ohne Abo', 'dpa', 'Reuters'...
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "normalizzare spazi bianchi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_faz_net': {e}")
        return plain_text
    



def clean_folha_uol_com_br(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'folha.uol.com.br' (Folha de São Paulo).
    Rimuove sezioni di commenti, disclaimer, intestazioni (es. Folha Mercado),
    blocchi di abbonamento/newsletter, note redazionali, URL e numeri 
    di telefono, preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 23 pattern specifici (commenti, CTA, note).
    - Rimozione di URL e numeri di telefono (da 'altri_suggerimenti').
    - Normalizzazione e deduplicazione degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei pattern regex estratti dal JSON
    patterns_da_rimuovere = [
        # Pattern specifici da jsonl
        r"^Comentários\b.*",
        r"Os comentários não representam a opinião do jornal; a responsabilidade é do autor da mensagem\.",
        r"^Folha Mercado\b.*",
        r"^Lá Fora\b.*",
        r"^Planeta em Transe\b.*",
        r"^FolhaJus\b.*",
        r"\bUma newsletter\b.*",
        r"^Recurso exclusivo para assinantes.*",
        r"(assine\s*ou\s*faça login|assineoufaça login)",
        r"^Salvar artigos$",
        r"^Final do conteúdo$",
        r"^Descrição de chapéu.*",
        r"^Ver todos os comentários$",
        r"^Como assinar a CasaFolha.*",
        r"casafolhasp\.com\.br.*",
        r"\bCasaFolha\b",
        r"^Assine nossa newsletter.*",
        r"^Leia Mais.*",
        r"E-mail inválido!",
        r"^O repórter viajou a convite.*",
        r"^Colaborou .*",
        r"^TENDÊNCIAS / DEBATES.*",
        r"Os artigos publicados com assinatura não traduzem a opinião do jornal\.",

        # Pattern generici da 'altri_suggerimenti'
        r"https?://\S+", # Rimuove URL
        # Rimuove numeri di telefono (pattern generico)
        r"\b(?:tel(?:efone|fono)?|whatsapp|celular|ligue)[\s:.]*[\+_\-() \d]{8,}\b"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Come da "normalizzare minuscolo"
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "deduplicare spazi e righe vuote")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_folha_uol_com_br': {e}")
        return plain_text
    



def clean_hindustantimes_com(plain_text: str) -> str:
    """
    Pulisce il testo multilingue (EN, BN, TE) da 'hindustantimes.com'.
    Rimuove 'leggi anche', byline, timestamp, crediti foto, CTA, 
    pubblicità, copyright, tag e URL. 
    
    NON usa IGNORECASE per rispettare la sensibilità di Bangla/Telugu,
    come da istruzioni jsonl.

    Regole applicate (basate sul jsonl):
    - Rimozione di 29 pattern multilingue specifici.
    - Flag re.MULTILINE (ma non re.IGNORECASE).
    - Normalizzazione e deduplicazione degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei pattern regex estratti dal JSON
    # (le doppie backslash \\ sono convertite in singole \
    # nelle stringhe raw r"...")
    patterns_da_rimuovere = [
        # Bengali "Leggi anche" (inline e inizio riga)
        r"আর[োও]\s+প(?:ড়|ড়)ুন[:：]?\s*[^।\n]*",
        r"^\s*আর[োও]\s+প(?:ড়|ড়)ুন[:：]?\s*.*$",
        # English "Leggi anche" (inline e inizio riga)
        r"(?:Also\s+Read|READ|Read\s+also)[:：]?\s*[^.!\n]*",
        r"^\s*(?:Also\s+Read|READ|Read\s+also)[:：]?\s*.*$",
        # Telugu "Leggi anche" (inline e inizio riga)
        r"(?:ఇంకా|మరింత)\s+చదవండి[:：]?\s*[^.\n]*",
        r"^\s*(?:ఇంకా|మరింత)\s+చదవండి[:：]?\s*.*$",
        # Byline (EN, BN, TE)
        r"^\s*(?:By|Edited\s+by|Written\s+by|Updated\s+by|Report(?:ed)?\s+by)\b.*$",
        r"^\s*(?:Published\s+on|Updated\s+on|Last\s+updated|First\s+published)\b.*$",
        r"^\s*(?:প্রকাশিত|আপডেট(?:েড)?|আপডেট)\s*[:：]?\s*.*$",
        r"^\s*(?:ప్రచురించబడింది|నవీకరించబడింది)\s*[:：]?\s*.*$",
        # Correlati (EN, BN, TE)
        r"^\s*(?:Related\s+(?:Stories|News|Articles)|Recommended\s+for\s+you|Trending\s+Now|Top\s+Stories)\b.*$",
        r"^\s*(?:সম্পর্কিত\s+খবর|ট্রেন্ডিং|সর্বাধিক\s+পঠিত|শীর্ষ\s+খবর)\b.*$",
        r"^\s*(?:సంబంధిత\s+వార్తలు|ట్రెండింగ్|టాప్\s+స్టోరీస్)\b.*$",
        # CTA Social (EN, BN, TE)
        r"^\s*(?:Share\s+(?:this|article)|Follow\s+us|Download\s+(?:the\s+)?app|Sign\s+in|Subscribe\s+now)\b.*$",
        r"^\s*(?:শেয়ার|ফলো\s+করুন|অ্যাপ\s+ডাউনলোড|সাবস্ক্রাইব|লগইন)\b.*$",
        r"^\s*(?:షేర్\s+చేయండి|ఫాలో\s+చేయండి|యాప్\s+డౌన్‌లోడ్|సబ్‌స్క్రైబ్|లాగిన్)\b.*$",
        # Crediti Foto (EN, BN, TE)
        r"^\s*(?:Photo|Image|File\s+photo|HT\s+Photo|Credit)\s*[:：]?.*$",
        r"^\s*(?:ছবি|ফটো|ক্রেডিট)\s*[:：].*$",
        r"^\s*(?:ఫోటో|చిత్రం|క్రెడిట్)\s*[:：].*$",
        # Pubblicità (EN, BN, TE)
        r"^\s*(?:Advertisement|ADVERTISEMENT|advertisement)\s*$",
        r"^\s*বিজ্ঞাপন\s*$",
        r"^\s*ప్రకటన\s*$",
        # Copyright e Tag
        r"^\s*©\s*Hindustan\s*Times.*$",
        r"^\s*All\s+Rights\s+Reserved\.?$",
        r"^\s*Copyright\s*©.*$",
        r"^\s*Tags?\s*[:：]\s*.*$",
        r"^\s*Topics?\s*[:：]\s*.*$",
        # URL
        r"https?://\S+"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        #    NO re.IGNORECASE: Come da 'altri_suggerimenti'
        compiled_regex = re.compile(combined_pattern, re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "normalizzare spazi bianchi e linee vuote")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_hindustantimes_com': {e}")
        return plain_text
    



def clean_ilfattoquotidiano_it(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'ilfattoquotidiano.it'.
    Rimuove un'ampia gamma di blocchi cookie/TCF, paywall, 
    inviti all'abbonamento, embed di social media (X/Instagram) 
    e policy dei commenti, preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 51 pattern specifici.
    - Flag re.IGNORECASE e re.MULTILINE applicati globalmente.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei 51 pattern regex estratti dal JSON
    # (Le doppie backslash \\ del JSON sono convertite in singole \)
    patterns_da_rimuovere = [
        r"^Resta in contatto con la community.*$",
        r"^Accedi o registrati.*$",
        r"^Gentile lettore.*$",
        r"^Abbiamo deciso di impostare questi limiti.*$",
        r"^I commenti saranno pubblicati.*$",
        r"^Infine non è consentito accedere al servizio tramite account multipli.*$",
        r"^Vi preghiamo di segnalare eventuali problemi tecnici.*$",
        r"^La Redazione$",
        r"^Caro navigatore, cara navigatrice.*$",
        r"^Quest'articolo è riservato agli abbonati.*$",
        r"^Abbonati a il Fatto Quotidiano.*$",
        r"^Partner Abbonati a il Fatto Quotidiano.*$",
        r"FQ IN EDICOLA",
        r"TvLoft",
        r"Scuola del Fatto",
        r"Tesseramento alla Fondazione il Fatto Quotidiano.*",
        r"Card digitale.*",
        r"Accedere a tutti gli articoli.*",
        r"Accedere all'archivio completo.*",
        r"Navigare senza pubblicità",
        r"Sconto del \d+%.*",
        r"^Abbiamo a cuore la tua privacy.*$",
        r"^Noi e i nostri partner archiviamo e/o accediamo a informazioni.*$",
        r"^Con il tuo consenso, i tuoi dati possono essere utilizzati.*$",
        r"^I tuoi dati (personali )?verranno trattati.*$",
        r"^Puoi (revocare|gestire) le tue preferenze.*$",
        r"Transparency and Consent Framework",
        r"IAB Europe",
        r"^Questi sono i nostri partner pubblicitari.*$",
        r"^Questi fornitori sono registrati su Google.*$",
        r"cl-consent-settings",
        r"priclt",
        r"^Accetta i consensi.*$",
        r"^Rifiuta e Sostienici.*$",
        r"Sostenitore.*1€ al mese.*5,99€ al mese.*",
        r"^Ci dispiace, ma per la data selezionata.*audioarticoli.*$",
        r"Gen Feb Mar Apr Mag Giu Lug Ago Set Ott Nov Dic",
        r"^— .*\(@.*\).*$",
        r"pic\.twitter\.com/\S+",
        r"^Visualizza questo post su Instagram$",
        r"^View this post on Instagram$",
        r"Visualizza questo post su Instagram",
        r"^Leggi articolo$",
        r"\bleggi anche\b", # Rimosso flag inline (?i)
        r"^Partner$",
        r"^Digital Partner$",
        r"^2019 2020 2021 2022 2023 2024 2025.*$",
        r"^Che cosa sono i cookie\?$",
        r"^Finalità$",
        r"^Gestisci partner$",
        r"^Interesse legittimo$"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Per 'leggi anche', 'FQ IN EDICOLA', 'Gentile lettore'
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "unire righe spezzate, normalizzare spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_ilfattoquotidiano_it': {e}")
        return plain_text
    



def clean_ilsole24ore_com(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'ilsole24ore.com'.
    Rimuove blocchi paywall, inviti all'abbonamento, banner cookie/TCF,
    blocchi e-commerce (prezzi/sconti), URL e didascalie,
    preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 37 pattern specifici.
    - Flag re.IGNORECASE e re.MULTILINE applicati globalmente.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei 37 pattern regex estratti dal JSON
    # (Le doppie backslash \\ del JSON diventano singole \
    # in una stringa raw Python r"...")
    patterns_da_rimuovere = [
        r"^Vai alla navigazione.*",
        r"Podcast.*Abbonati.*",
        r"^Homepage podcast.*",
        r"404 This page could not be found\.",
        r"^Oops! Sei offline.*",
        r"Accedi ad una rete Internet",
        r"Ricarica la pagina",
        r"Continua a leggere questo articolo",
        r"Prova un mese a 1 ?euro",
        r"^Accesso illimitato a:",
        r"^- Tutti gli articoli de ilsole24ore\.com",
        r"^- Tutti gli approfondimenti premium di 24\+",
        r"^- La newsletter 24\+Recap",
        r"^- Dati di borsa in tempo reale",
        r"^- Portafoglio virtuale.*",
        r"^- L'inserto digitale Plus24.*",
        r"S24 - Maschere Blocco Stand Alone",
        r"Scopri tutte le altre opzioni di abbonamento.*",
        r"^Sei già abbonato\??",
        r"^Noi e i nostri .* partner archiviamo.*",
        r"Informativa sui cookie",
        r"^LEGGI ANCHE:?$|^Leggi anche:?$",
        r"^Immagine( creata da AI| generata da AI)\.?$|^Immagine:",
        r"^Suggerimento musicale:.*",
        r"designed by Freepik",
        r"^Post di .+ —",
        r"^Linkografia.*",
        r"^https?://\S+$",
        r"^Puntate precedenti$",
        r"— continua|– continua",
        r"^TORNA ALL['’]INDICE$",
        r"continua a leggere\.\.\.|continua a$",
        r"^Ascolta su$",
        r"^Prezzo:\s*\d+[\d\.,]* ?€",
        r"^Sconto del \d+%",
        r"Attiva oraRinnovo automatico\.? ?Disattiva quando vuoi\.",
        r"^Ultime notizie Tutto il sito.*",
        r"^Canali Brand connect.*",
        r"Archivio Solo per abbonati Il Sole 24 Ore Video Radio24"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Come da "normalizzare a minuscolo"
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "unire righe spezzate, deduplicare spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_ilsole24ore_com': {e}")
        return plain_text
    

import re

def clean_japantimes_co_jp(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'japantimes.co.jp' (The Japan Times).
    Applica 9 pattern sequenziali per rimuovere blocchi "About", 
    "Inquiry" (問い合わせ) e "Press" (報道関係者), oltre a URL, email
    e righe di note (※), preparando il testo per il chunking.
    
    Rispetta i flag regex inline (?m, ?i, ?mi) e i pattern
    multi-riga ([\s\S]) forniti nel jsonl.

    Regole applicate (basate sul jsonl):
    - Rimozione sequenziale di 9 pattern con flag inline/misti.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista dei pattern regex estratti dal JSON
    # (Le doppie backslash \\ del JSON sono convertite in singole \)
    # Vengono applicati sequenzialmente
    patterns_da_rimuovere = [
        # Flag (?m) = MULTILINE
        r"(?m)^イベントの詳細・お申し込みはこちら.*$",
        r"(?m)^The Japan Times Online：.*$",
        
        # Pattern multi-riga (DOTALL) che rimuovono blocchi
        r"The Japan Times（ジャパンタイムズ）について[\s\S]*?(?=＜問い合わせ＞|＜報道関係者＞|https?://|$)",
        r"＜問い合わせ＞[\s\S]*?(?=＜報道関係者＞|$)",
        r"＜報道関係者＞[\s\S]*$",
        
        # Email (inline, case-insensitive)
        r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b",
        # URL
        r"https?://\S+",
        
        # Note (MULTILINE)
        r"(?m)^※.*$",
        # Righe Email (MULTILINE, IGNORECASE)
        r"(?mi)^E-?mail\s*:\s*\S+\s*$"
    ]

    try:
        # Applica ogni pattern sequenzialmente
        for pattern in patterns_da_rimuovere:
            text = re.sub(pattern, '', text)

        # Pulizia finale e normalizzazione spazi bianchi
        # (Come da "normalizzare spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_japantimes_co_jp': {e}")
        return plain_text
    
import re

def clean_lanacion_com_ar(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'lanacion.com.ar' (La Nación).
    Rimuove rumore social (X, Instagram), URL, email, hashtag, 
    paywall e molteplici formati di crediti foto/agenzia
    (sia tra parentesi che in formato '... / ...'), 
    preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 22 pattern specifici.
    - Flag re.IGNORECASE e re.MULTILINE applicati globalmente.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei 22 pattern regex estratti dal JSON
    # (Le doppie backslash \\ del JSON sono convertite in singole \
    # nelle stringhe raw r"...")
    patterns_da_rimuovere = [
        # Social (X/Twitter, Instagram)
        r"https?://(?:x\.com|twitter\.com)/\S+",
        r"pic\.twitter\.com/\S+",
        r"^— .*\(@[^)]+\).*$",
        r"^.*\bX/@[A-Za-z0-9_]+.*$",
        r"^.*\bInstagram/@[A-Za-z0-9_.]+.*$",
        
        # Crediti Foto/Agenzia (formato "Slash" e "Parentesi")
        r"^.*\s/\s(?:@?[A-Za-z0-9_]+|[^\n]*?(?:AFP|AP|LA NACION|Reuters|Getty Images|Shutterstock|Freepik|Europa Press|Efe|Google Maps|Prensa|Archivo|Gentileza))\s*$",
        r"\((?:Foto|Imagen|Imagen de car[aá]cter|Gentileza|Fuente|Shutterstock|Getty Images|AP|AFP|Instagram|X|Twitter|Google Maps|Freepik|Pexels|Pixabay)[^)]*\)",
        
        # Paywall e metadati
        r"^LA NACION\s*$",
        r"^Exclusivo para suscriptores.*suscripci[oó]n.*$",
        
        # CTA (Call to Action) e Link
        r"^(M[áa]s informaci[oó]n|C[oó]mo ayudar).*$",
        r".*haciendo clic?k? aqu[ií].*",
        r".*click (aqu[íi]|ac[áa]).*",
        
        # URL, Email, Telefoni, Hashtag
        r"\b[\w\.-]+@[\w\.-]+\.[A-Za-z]{2,}\b",
        r"\+?\d[\d\s\-\(\)]{6,}",
        r"^.*#\w+.*$",
        
        # Link interni e "Leggi anche"
        r"^(Mirá también|Mira también|Le[ée] también|Te puede interesar).*$",
        r"^En Foodit.*$",
        r"^Haciendo clic.*$",
        
        # Varie
        r"^Noticia en desarrollo$",
        r"FOTOGRAF[ÍI]A ILUSTRATIVA:.*$", # Flag (?i) gestito globalmente
        r"^(?:\s*-\s*\d+\s.*)$", # Righe che sembrano liste
        r"^(Fuentes?|Fuente):.*$"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Come da "normalizzare minuscolo" e flag inline (?i)
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "suprimir líneas vacías... y espacios extra")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_lanacion_com_ar': {e}")
        return plain_text
    


def clean_lastampa_it(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'lastampa.it' (La Stampa).
    Applica 14 pattern sequenziali per rimuovere banner cookie/paywall,
    timestamp, byline, tag di sezione (anche in maiuscolo) e crediti
    foto (formato ' / '), preparando il testo per il chunking.
    
    Rispetta i flag regex inline (?i) e la case-sensitivity
    dove specificato nel jsonl. Aggiunge re.MULTILINE ai pattern
    che usano ^ o $ per garantirne il funzionamento.

    Regole applicate (basate sul jsonl):
    - Rimozione sequenziale di 14 pattern con flag misti.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista dei pattern regex estratti dal JSON
    # (pattern, flag_aggiuntivi)
    # L'uso di tuple (pattern, flags) permette di gestire la logica mista
    # (alcuni case-sensitive, altri no) e di aggiungere MULTILINE.
    patterns_da_rimuovere = [
        # Flag (?i) - Cookie e Paywall (inline, senza ^ o $)
        (r"(?i)i ricavi ottenuti dalla pubblicità personalizzata.*cookie policy", 0),
        (r"(?i)gestione cookie", 0),
        (r"(?i)sei libero di rifiutare o revocare il consenso.*abbonamenti", 0),
        (r"(?i)passa a premium", 0),
        (r"(?i)in collaborazione con.*", 0),
        (r"(?i).*(\bil video dell'annuncio\b).*", 0),
        (r"(?i).*divisorio kpi.*", 0),
        
        # Flag (?i) + MULTILINE (per ^ e $)
        (r"(?i)^articolo free.*passa a premium.*$", re.MULTILINE),
        (r"(?i)^di\s+[A-Za-zÀ-ÖØ-öø-ÿ' .-]+\s+\d{1,2}\s+[A-Za-zÀ-ÖØ-öø-ÿ]+\s+\d{4}$", re.MULTILINE),
        (r"(?i)^(analisi|approfondimento|la guida|il reportage|il racconto|il commento|l’INTERVISTA|l'INTERVISTA|intervista|intelligenza artificiale|valute|stati uniti|la manovra)\b.*$", re.MULTILINE),

        # Case-Sensitive + MULTILINE (per ^ e $)
        # (Nessun flag ?i qui)
        (r"^[0-9]{1,2}\s+[A-ZÀ-Ö][a-zà-ö]+\s+\d{4}$", re.MULTILINE),
        (r"^[A-ZÀ-Ö' ]+\s+\d{1,2}\s+[A-ZÀ-Ö][a-zà-ö]+\s+\d{4}$", re.MULTILINE),
        (r"^[A-ZÀ-ÖØ-Ý][A-ZÀ-ÖØ-Ý' .-]{2,}$", re.MULTILINE),
        (r"^.*\s/\s.*$", re.MULTILINE) # Per "FOTO / ANSA"
    ]

    try:
        # Applica ogni pattern sequenzialmente con i flag corretti
        for pattern, flags in patterns_da_rimuovere:
            # Se il pattern ha già flag inline (es. ?i),
            # re.compile li combinerà correttamente con i flag aggiuntivi (re.MULTILINE).
            compiled_regex = re.compile(pattern, flags)
            text = compiled_regex.sub('', text)

        # Pulizia finale e normalizzazione spazi bianchi
        # (Come da "normalizzare spazi e deduplicare righe")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_lastampa_it': {e}")
        return plain_text




def clean_latimes_com(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'latimes.com' (Los Angeles Times).
    Rimuove pubblicità, separatori, URL, CTA social/newsletter
    e vari formati di crediti foto/agenzia (AP, Getty, Reuters)
    e righe di timestamp/dateline, preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 18 pattern specifici.
    - Flag re.IGNORECASE e re.MULTILINE applicati globalmente.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei 18 pattern regex estratti dal JSON
    # (Le doppie backslash \\ del JSON sono convertite in singole \
    # nelle stringhe raw Python r"...")
    patterns_da_rimuovere = [
        r"^\s*/?\s*Anuncio\s*/?\s*$",
        r"\s/\sAnuncio\s/\s",
        r"^_{3,}.*$",
        r"^___\s*Deportes AP: https?://\S+",
        r"^\s*Deportes AP:.*$",
        r"https?://\S+",
        # Crediti in parentesi
        r"\((?:AP Photo|AP Foto|Associated Press|Los Angeles Times|LA Times en Español|WWE via Getty Images|via Getty Images|Getty Images|Reuters|EFE)[^)]*\)",
        r"^(ARCHIVO|Archivo)\s*[–-]\s.*$",
        # Crediti formato "Slash"
        r"^.*\s/\s.*\((?:[^)]*AP|Associated Press|Los Angeles Times|Getty Images|Reuters|EFE)[^)]*\)\s*$",
        r"^\s*Historia completa aquí\.?\s*$",
        r"^Esta historia fue traducida.*$",
        # Dateline (Spagnolo e Inglese)
        r".*\b(Ene\.|Feb\.|Mar\.|Abr\.|May\.|Jun\.|Jul\.|Ago\.|Sep\.|Oct\.|Nov\.|Dic\.)\s\d{1,2},\s\d{4}\.?$",
        r"^.*\b(January|February|March|April|May|June|July|August|September|October|November|December)\b.*\d{4}.*$",
        # CTA
        r"^\s*(Suscríbete|Suscripción|Bolet[ií]n|Newsletter)\b.*$",
        r"^\s*(Comparte|Síguenos en|Sigue en X|Facebook|Twitter|Instagram)\b.*$",
        # Crediti Foto e AP
        r"^[Ff]oto[s]?\b[^\n]*$",
        r"\b\(AP\)\b",
        r"\s*\(Moises Castillo\s*/\s*Associated Press\)\s*$"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Come da "normalizzare minuscolo"
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "normalizzare spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_latimes_com': {e}")
        return plain_text



import re

def clean_lefigaro_fr(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'lefigaro.fr' (Le Figaro).
    Rimuove un'ampia gamma di blocchi paywall, offerte promozionali,
    link "À lire aussi", crediti, partner TV e tag redazionali
    (es. REPORTAGE), preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 28 pattern specifici (paywall, promo, CTA).
    - Rimozione di tag redazionali (da 'altri_suggerimenti').
    - Flag re.IGNORECASE e re.MULTILINE applicati globalmente.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei 28 pattern estratti dal JSON
    # Ho rimosso il flag (?i) inline per applicarlo globalmente
    patterns_da_rimuovere = [
        r"Passer la publicit[ée]",
        r"Cet article est r[eé]serv[eé] aux abonn[eé]s\.?",
        r"Il vous reste\s*\d+%?\s*à découvrir\.?",
        r"D[eé]j[aà] abonn[eé]\s*\?\s*Connectez-vous",
        r"Vous avez envie de lire la suite \?.*",
        r"D[ée]bloquez tous les articles imm[ée]diatement\.?",
        r"Vente Flash.*Sans engagement",
        r"Offre anniversaire\s*:\s*.*",
        r"J-?\d+.*Le Figaro f[êe]te ses \d+\s*ans",
        r"^\s*Le Figaro\s*/.*$",
        r"^\s*Le Figaro\s*$",
        r"^\s*Le Figaro TV.*$",
        r"^SFR\s*:\s*.*$",
        r"^Bouygues\s*:\s*.*$",
        r"^Retrouvez Le Figaro TV.*$",
        r"^Canal\+.*$",
        r"^TF1\+.*$",
        r"^Samsung.*$",
        r"^Molotov.*$",
        r"^Partager via\s*:\s*.*$",
        r"^À d[ée]couvrir.*$",
        r"^À lire aussi.*$",
        r"^Regarder la vid[ée]o.*$",
        r"^En direct.*$",
        r"R[eé]serv[ée] aux abonn[ée]s",
        r"^Les publications appara[îi]tront ici.*$",
        r"^Le Figaro\s*J-?\d+,?\s*Le Figaro f[êe]te ses 200 ans.*$",
        r"les articles imm[ée]diatement\.",
        
        # Pattern aggiunto da 'altri_suggerimenti'
        r"^(REPORTAGE|DÉCRYPTAGE)\b.*$"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Come da flag (?i) e "convertire in minuscolo"
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "normalizzare spazi e interruzioni di riga")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_lefigaro_fr': {e}")
        return plain_text
    


def clean_lematin_ma(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'lematin.ma' (Le Matin).
    Rimuove 47 pattern specifici, inclusi 'Lire Aussi', CTA social,
    blocchi cookie/newsletter, breadcrumb, footer legali,
    rumore social (Twitter) e formule protocollari/religiose,
    preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 47 pattern specifici.
    - Flag re.IGNORECASE e re.MULTILINE applicati globalmente.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei 47 pattern estratti dal JSON
    patterns_da_rimuovere = [
        r"^\s*(>>>|>>)?\s*Lire\s+Aussi\s*:.*$",
        r"^\s*Voir\s+(aussi|plus)\s*:.*$",
        r"^\s*Regarder\s+la\s+vid[ée]o\s*:.*$",
        r"^(Partager|Partagez|Share)\s*(cet\s+article|sur)?\s*:?.*$",
        r"^(Suivez-nous|Follow\s+us)\s*(sur)?\s*:?.*$",
        r"https?://t\.co/\w+",
        r"pic\.twitter\.com/\w+",
        r"—\s.*\(?@[A-Za-z0-9_]+\)?.*",
        r"@[A-Za-z0-9_]{2,}",
        r"https?://[^\s)]+|www\.[\w.-]+\.[a-z]{2,}",
        r".*nous\s+utilisons\s+des\s+cookies.*",
        r"^\s*Cookies?\s*.*(accepter|refuser|param(è|e)tres|consentement).*$",
        r"^Inscrivez-vous\s+à\s+la\s+newsletter.*$",
        r"^(Abonnez-vous|S'abonner|S’inscrire|Inscription)\s+(à|a)\s+la\s+newsletter.*$",
        r"^Accueil(\s*[›>/\|].*)+$",
        r"^Accueil\s*>.*$",
        r"^(A\s+la\s+une|À\s+la\s+une|Nation|Monde|[ÉE]conomie|Soci[ée]t[ée]|Culture|Sports?|R[ée]gions|Opinions?)(\s*[›>/\|]\s*(Nation|Monde|[ÉE]conomie|Soci[ée]t[ée]|Culture|Sports?|R[ée]gions|Opinions?)){2,}$",
        r"^Tags?\s*:\s.*$",
        r"^Laisser\s+un\s+commentaire.*$",
        r"^Poster\s+un\s+commentaire.*$",
        r"^Votre\s+adresse\s+e-mail\s+ne\s+sera\s+pas\s+publi[ée]e.*$",
        r"^Commentaires?\s*(\(.*\))?\s*$",
        r"^(Publicit[ée]|Annonce|Sponsored)\s*$",
        
        # --- CORREZIONE QUI ---
        # Sostituito \p{L}+ (non valido in Python) con [A-Za-zÀ-ÿ]+
        r"^Mis\s+à\s+jour\s+le\s+\d{1,2}\s+[A-Za-zÀ-ÿ]+\s+\d{4}.*$",
        # --- FINE CORREZIONE ---
        
        r"^\[\s*(VID[ÉE]O|PHOTOS?|INFOGRAPHIE|LIVE|DIRECT|Mise\s*à\s*jour|MAJ)[^\]]*\]\s*$",
        r"^\s*©\s*Le\s*Matin(\s*\d{4})?\.?$",
        r"^Tous\s+droits\s+r[ée]serv[ée]s.*$",
        r"^Cr[ée]dit(s)?\s*(photo|vid[ée]o)\s*[:：].*$",
        r"^Source\s*[:：].*$",
        r"^Mentions\s+l[ée]gales$",
        r"^Conditions\s+d'utilisation$",
        r"^Politique\s+de\s+confidentialit[ée]$",
        r"^Gestion\s+des\s+cookies$",
        r"^Contact$",
        r"^Aide$",
        r"^FAQ$",
        r"^\s*[>›]{2,}.*$",
        r"^Le\s+Matin\s*:\s*",
        # Formule protocollari/religiose
        r"^\s*Louange\s+à\s+Dieu.*(Proph[èe]te).*$",
        r"^Wassalamou\s+alaikoum\s+warahmatoullahi\s+wabarakatouh\s*\.?$",
        r"^Puisse\s+Dieu\s+garder\s+Sa\s+Majest[ée]\s+le\s+Roi.*$",
        r"^Que\s+Dieu\s+(perp[ée]tue\s+Sa\s+gloire|L'assiste|garde\s+Sa\s+Majest[ée]\s+le\s+Roi).*$",
        r"^[nN]$", # Artefatto
        # Email
        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        # Questo blocco 'except' è nello script di pulizia,
        # ma se c'è un errore di compilazione (come questo \p),
        # è meglio che l'errore venga fuori subito.
        # Nello script Dask, abbiamo aggiunto un try/except
        # attorno alla *chiamata* a questa funzione.
        print(f"Errore regex nel cleaner 'clean_lematin_ma': {e}")
        return plain_text # Restituisci l'originale se fallisce



def clean_lemonde_fr(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'lemonde.fr' (Le Monde).
    Rimuove 51 pattern specifici, inclusi blocchi paywall estesi,
    banner cookie/TCF complessi, CTA di podcast/newsletter/eventi,
    crediti foto/agenzia, URL ed email, preparando il testo 
    per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 51 pattern specifici.
    - Flag re.IGNORECASE e re.MULTILINE applicati globalmente.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei 51 pattern regex estratti dal JSON
    # (Le doppie backslash \\ del JSON sono convertite in singole \)
    patterns_da_rimuovere = [
        # CTA e Paywall
        r"^Lire aussi.*$",
        r"^(Lire (le|la|les) .*)$",
        r"Article réservé à nos abonnés.*",
        r"Il vous reste .*% de cet article à lire.*",
        r"Lecture du Monde en cours.*",
        r"Vous pouvez lire Le Monde sur un seul appareil.*",
        r"Que se passera-t-il si vous continuez à lire ici.*",
        r"Y a-t-il d’autres limites.*",
        r"Comment ne plus voir ce message.*",
        r"Accéder gratuitement en acceptant l’utilisation de vos données.*",
        r".*Gestion des cookies.*",
        r".*S’abonner.*",
        
        # Blocco Cookie TCF (pattern unico molto lungo)
        r"^(Assurer le bon fonctionnement technique du Site|Mesurer l’audience du Site|Analyser l’audience et les usages de notre Site.*|Personnaliser votre parcours sur notre Site.*|Personnaliser et cibler nos campagnes publicitaires.*|Stocker et/ou accéder à des informations sur un appareil.*|Utiliser des données limitées pour sélectionner la publicité.*|Créer des profils pour la publicité personnalisée.*|Utiliser des profils pour sélectionner des publicités personnalisées.*|Créer des profils de contenus personnalisés.*|Utiliser des profils pour sélectionner des contenus personnalisés.*|Mesurer la performance des publicités.*|Mesurer la performance des contenus.*|Comprendre les publics par le biais de statistiques.*|Développer et améliorer les services.*|Utiliser des données limitées pour sélectionner le contenu.*|Assurer la sécurité, prévenir et détecter la fraude et réparer les erreurs.*|Fournir et présenter des publicités et du contenu.*|Enregistrer et communiquer les choix en matière de confidentialité.*|Mettre en correspondance et combiner des données à partir d’autres sources de données.*|Relier différents appareils.*|Identifier les appareils en fonction des informations transmises automatiquement.*|Utiliser des données de géolocalisation précises.*|Analyser activement les caractéristiques de l’appareil pour l’identification.*)$",
        
        # Crediti e Metadati
        r"\bLe Monde avec (AFP|AP|Reuters)\b",
        r"\bRéutiliser ce contenu\b",
        r"^LETTRE DE [A-ZÉÈÀÙÂÎÔÛÇ\- ]+.*$",
        r"^[\s]*/\s.*$", # Crediti " / ..."
        r"^.+, le \d{1,2} [a-zéûàîôç]+ \d{4}\. ?/.*$", # Dateline
        
        # URL e Email
        r"https?://\S+",
        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
        
        # CTA Newsletter, Podcast, Eventi
        r"^Ce billet est extrait de l'infolettre.*$",
        r"^Vous pouvez retrouver ici tous les épisodes.*$",
        r"^Vous pouvez vous inscrire.*$",
        r"^Vous pouvez désormais retrouver .*Instagram.*$",
        r"^Inscriptions? .* (ici|là).*$",
        r"^Programmation et billetterie.*$",
        r"^On vous attend au Festival du Monde.*$",
        r"^Festival du Monde.*$",
        r"^Du \d{1,2} au \d{1,2} septembre, Le Monde ouvre ses portes.*$",
        r"^Question posée par .*$",
        r"^A (écouter|lire|regarder) (ici|là|par là)\.?$",
        r"^C'?est par là.*$",
        r"^Un épisode au hasard.*$",
        r"^Un peu de « Chaleur humaine » en plus.*$",
        r"^La question de la semaine.*$",
        r"^(Dans mes oreilles|Sur ma table de nuit|Sur mon écran).*",
        r"Plus d'informations à venir\."
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Come da "normalizzare ... minuscole"
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "normalizzare spazi", "unire righe spezzate")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_lemonde_fr': {e}")
        return plain_text
    

import re

def clean_liberation_fr(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'liberation.fr' (Libération).
    Applica 3 pattern sequenziali per rimuovere disclaimer di 
    contenuti sponsorizzati (Libé+) e, in particolare, i blocchi
    in cui questi disclaimer sono duplicati.
    
    Rispetta i flag regex inline (?is) forniti nel jsonl.
    L'ordine di esecuzione è fondamentale: prima i duplicati, 
    poi le istanze singole.

    Regole applicate (basate sul jsonl):
    - Rimozione del blocco disclaimer duplicato (pattern specifico).
    - Rimozione dei disclaimer singoli residui.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista dei pattern (con flag inline ?is).
    # (Le doppie backslash \\ del JSON sono convertite in singole \)
    
    # Pattern 1 (il più specifico): Rimuove la combinazione duplicata
    pattern_duplicato = (
        r"(?is)(?:"
        # Inizio del blocco
        r"Lib(?:é|e)\s*\+\s*est la régie publicitaire de Lib(?:é|e)ration\."
        r"[\s\n]*"  # Spazio o newline tra le due frasi
        r"La rédaction de Lib(?:é|e)ration n'a pas participé à la conception et à la réalisation de ce contenu\."
        # Cerca 2 ripetizioni consecutive di questo blocco
        r"){2}"
    )
    
    # Pattern 2: Rimuove la riga "Libé+ est la régie..." (singola)
    pattern_libe_plus = r"(?is)\bLib(?:é|e)\s*\+\s*est la régie publicitaire de Lib(?:é|e)ration\."
    
    # Pattern 3: Rimuove la riga "La rédaction..." (singola)
    pattern_redaction = r"(?is)La rédaction de Lib(?:é|e)ration n'a pas participé à la conception et à la réalisation de ce contenu\."

    # L'ordine è importante: il pattern duplicato DEVE essere
    # eseguito prima dei pattern singoli.
    patterns_da_rimuovere_ordinati = [
        pattern_duplicato,
        pattern_libe_plus,
        pattern_redaction
    ]

    try:
        # Applica ogni pattern sequenzialmente nell'ordine corretto
        for pattern in patterns_da_rimuovere_ordinati:
            text = re.sub(pattern, '', text)

        # Pulizia finale e normalizzazione spazi bianchi
        # (Come da "comprimere spazi e nuove righe multiple")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_liberation_fr': {e}")
        return plain_text
    


def clean_mainichi_jp(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'mainichi.jp' (Mainichi Shimbun).
    Rimuove blocchi paywall/registrazione, TCF (データの設定), 
    timeline (時系列で見る), crediti foto/agenzia (AP, ロイター)
    e link social (公式X), preparando il testo per il chunking.
    
    Applica prima i pattern multi-riga (DOTALL) e poi
    combina gli altri per efficienza. Non usa IGNORECASE.

    Regole applicate (basate sul jsonl):
    - Rimozione sequenziale di 2 pattern multi-riga (DOTALL/ [\s\S]).
    - Rimozione combinata di 16 pattern (line-based e inline).
    - Flag re.MULTILINE applicato dove necessario.
    - Normalizzazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # 1. Pattern multi-riga (DOTALL) da applicare sequenzialmente
    # (Usano [\s\S] e ^, quindi re.MULTILINE è necessario)
    dotall_patterns = [
        # Blocco "Timeline" (presumibilmente alla fine)
        r"^【時系列で見る】[\s\S]*$",
        # Blocco TCF "Data Settings" (rimuove tutto da qui alla fine)
        r"^\s*データの設定[\s\S]*" 
    ]

    try:
        for pattern in dotall_patterns:
            # Compila con re.MULTILINE per far funzionare ^
            compiled_regex = re.compile(pattern, re.MULTILINE)
            text = compiled_regex.sub('', text)

        # 2. Pattern rimanenti (line-based e inline)
        # (Le doppie backslash \\ del JSON diventano singole \)
        other_patterns = [
            r"この記事は有料記事です。",
            r"残り\d+文字（全文\d+文字）",
            r"無料の会員登録で続きが読めます。.*",
            r"毎日IDにご登録頂くと、.*登録は無料です。",
            r"毎日メディカルの無料メルマガの登録はこちら",
            r"^\s*- .*\d+日前.*$",
            r"^.*(公式X|インスタグラム)（?@?[^）]*）?.*から\s*$",
            r"（@[^）]+）",
            r"（C）[^\s)]+", # (C)Copyright
            r"（撮影・[^）]+）", # (Photo: ...)
            r"（(AP|ロイター)）", # (AP|Reuters)
            # Linee con ＝ e crediti
            r"^[^\\n]*＝[^\\n]*(撮影|提供|AP|ロイター)[^\\n]*$", 
            r"毎日新聞社?の?主催.*$",
            r"※?写真はイメージです＝.*$",
            r".*公式サイトを通じて.*お詫び.*$",
            r"ジョブチューン.*出演.*$"
        ]
        
        # Combina i 16 pattern rimanenti
        combined_pattern = "|".join(other_patterns)
        
        # Compila con re.MULTILINE (per ^ e $)
        # NON usa re.IGNORECASE
        compiled_regex_others = re.compile(combined_pattern, re.MULTILINE)
        
        text = compiled_regex_others.sub('', text)

        # 3. Pulizia finale degli spazi
        # (Come da "Normalizzare spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_mainichi_jp': {e}")
        return plain_text
    



def clean_nrc_nl(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'nrc.nl' (NRC Handelsblad).
    Rimuove un'ampia gamma di crediti di produzione (podcast/video),
    righe di contatto, email, metadati legali (ECLI), crediti foto
    e CTA, preparando il testo per il chunking.
    
    NON usa IGNORECASE, poiché i pattern sembrano
    intenzionalmente case-sensitive, ma usa MULTILINE.

    Regole applicate (basate sul jsonl):
    - Rimozione di 37 pattern specifici.
    - Rimozione di URL generici (da 'altri_suggerimenti').
    - Flag re.MULTILINE applicato globalmente.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei 37 pattern regex estratti dal JSON
    # (Le doppie backslash \\ del JSON sono convertite in singole \)
    patterns_da_rimuovere = [
        r"^Heeft u vragen.*Mail.*$",
        r"^Reageren.*abonnement.*$",
        r"^Reageren:? .*ombudsman.*$",
        r"^Insturen via .*@.*$",
        r"^Stuur .* naar .*@.*$",
        r".*@nrc\.nl.*",
        r"^Kijk hier.*$",
        r"^Luister hier.*$",
        r"^Het interview is ook op YouTube.*$",
        r"^Presentatie:.*$",
        r"^Redactie.*$",
        r"^Productie:.*$",
        r"^Montage:.*$",
        r"^Mixage:.*$",
        r"^Muziek:.*$",
        r"^Artwork:.*$",
        r"^Fotografie:.*$",
        r"^Coördinatie:.*$",
        r"^Eindredactie:.*$",
        r"^Met medewerking van .*$",
        r"^SerieInterviews met lijsttrekkers$",
        r"^Online-veiling .*",
        r"^Kijkdagen .*",
        r"^Info: .*",
        r"^Correctie .*",
        r"^Gerechtshof .*ECLI:.*$",
        r"^ECLI:.*$",
        r"^RTL Beeld.*$",
        r"^Reuters$",
        r"^Lezers zijn de auteurs.*$",
        r"^De rubriek .* is anoniem.*$",
        r"^Klik op het vinkje naast .*$",
        r"^[Ww]ie is\?.*$",
        r"^Geboren \d{4}.*$",
        r"^Is dol op .*$",
        r"^Houdt ook van .*$",
        r"^Foto(?:'s)?[: ].*$",
        r"^.*\bFoto\b.*$",
        
        # Pattern aggiunto da 'altri_suggerimenti'
        r"https?://\S+"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        #    NO re.IGNORECASE: I pattern sono case-sensitive
        compiled_regex = re.compile(combined_pattern, re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "normalizzare spazi e unire righe spezzate")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_nrc_nl': {e}")
        return plain_text
    



def clean_nytimes_com(plain_text: str) -> str:
    """
    Pulisce il testo multilingue (Cinese Semplificato/Tradizionale, 
    Inglese) estratto da 'nytimes.com'.
    Rimuove pubblicità, paywall, banner cookie, blocchi di 
    navigazione/footer, link "leggi anche", crediti foto/autore 
    e URL, preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione di 22 pattern multilingue specifici.
    - Flag re.IGNORECASE e re.MULTILINE applicati globalmente.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei 22 pattern regex estratti dal JSON
    # (Le doppie backslash \\ del JSON sono convertite in singole \)
    patterns_da_rimuovere = [
        r"^(广告|廣告|Advertisement|Sponsored|贊助內容|赞助内容)\s*$",
        r"^赞助[:：].*$",
        r"^(分享|分享到.*|复制链接|複製連結|电子邮件|電子郵件|Email|打印|列印|评论|評論)\s*$",
        r"^(微信|微博|Twitter|X|Facebook|臉書|LinkedIn)\s*$",
        r"^(订阅|訂閱|注册|註冊|登录|登入).{0,20}$",
        r".*(立即订阅|立即訂閱|现在订阅|現在訂閱|成为订阅者|成為訂閱者|以继续阅读|以繼續閱讀).*",
        r".*(我们使用|我們使用).*(cookies?|Cookies?|Cookie).*",
        r".*(Cookie\s*(设置|設置|设置|設定)|隐私政策|隱私政策|隐私权政策|隱私權政策|使用条款|使用條款).*",
        r"^(首页|主頁|主页|Home|国际|國際|中国|中國|美国|美國|商业|商業|科技|文化|观点|觀點|生活|健康|旅行|体育|體育|视频|視頻|图片|圖片)\s*$",
        r"^(阅读更多|閱讀更多|更多报道|更多報道|更多文章|更多内容|更多內容|相关阅读|相關閱讀|延伸閱讀)\s*$",
        r"^(点击|點擊).{0,20}(阅读|閱讀).{0,10}(原文|英文原文|简体|简体中文版|繁體|繁体|繁體中文版|英文版).*$",
        r"^(简体|繁體|繁体|English|中文|中英對照|中英对照)(\s*[\/｜\|]\s*(简体|繁體|繁体|English|中文))*\s*$",
        r".*(Give this article|赠送本文|贈送本文).*",
        r".*(在App中打开|在\s*APP\s*中打开|在應用中打開|在应用中打开|在NYT应用中打开).*",
        r"(©|©︎)?\s?\d{4}.*(New\s?York\s?Times|纽约时报|紐約時報).*",
        r"^(联系我们|聯繫我們|关于我们|關於我們|帮助|幫助|FAQ|订阅中心|訂閱中心|Cookie\s*设置|Cookie\s*設定|不出售我的个人信息)\s*$",
        r"^(照片|图片|圖片|摄影|攝影|视频|影片)[:：].*$",
        r"^(作者|撰稿|翻译|譯者|編輯|编辑|校对|校對)[:：].*$",
        r"^https?://\S+\s*$",
        r"^(上一页|上一頁|下一页|下一頁|返回|返回首页|返回主頁)\s*$"
    ]
    
    # Il JSON non includeva pattern per questi, ma erano in 
    # "domain_root": "nytimes.com"
    # Aggiungo per sicurezza (sebbene i pattern cinesi siano dominanti)
    patterns_inglese_aggiuntivi = [
        r"^(Share|Email|Print|Comments)\s*$",
        r".*(Subscribe|Log\s*In).*",
        r".*We use cookies.*",
        r".*(Privacy Policy|Terms of Service|Cookie Settings).*",
        r"^(Read More|Related coverage)\s*$",
        r"^(Photo|Video|Image|Credit)[:：].*$",
        r"^(By|Written by|Translated by|Edited by)[:：].*$"
    ]

    all_patterns = patterns_da_rimuovere + patterns_inglese_aggiuntivi

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(all_patterns)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Per 'Advertisement', 'Home', 'Cookie', ecc.
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "normalizzare spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_nytimes_com': {e}")
        return plain_text
    



def clean_nzz_ch(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'nzz.ch' (Neue Zürcher Zeitung).
    Applica 16 pattern sequenziali per rimuovere blocchi JS (multi-riga),
    timestamp, promo newsletter, crediti foto/agenzia (case-sensitive)
    e altri CTA, preparando il testo per il chunking.
    
    Rispetta i flag regex inline (?s) e la case-sensitivity
    dei pattern.

    Regole applicate (basate sul jsonl):
    - Rimozione sequenziale di 16 pattern con flag misti.
    - Flag re.MULTILINE applicato ai pattern che usano ^ o $.
    - NON usa re.IGNORECASE.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista dei pattern (pattern_string, flag_aggiuntivi)
    # L'applicazione sequenziale è necessaria per gestire
    # il flag (?s) del primo pattern e la case-sensitivity.
    patterns_to_apply = [
        # 1. Blocco JS (multi-riga, flag ?s inline)
        (r"(?s)Optimieren Sie Ihre Browsereinstellungen\s*NZZ\.ch benötigt JavaScript.*?Bitte passen Sie die Einstellungen an\.", 0),
        
        # 2. Pattern Line-based (case-sensitive)
        (r"^Drucken\b", re.MULTILINE),
        (r"^\d{2}\.\d{2}\.\d{4},.*?(?:Uhr)?\s*\d+\s*min\b", re.MULTILINE),
        (r"Sie lesen einen Auszug aus dem Newsletter.*?Abonnieren Sie den Newsletter kostenlos\.", 0),
        (r"Nicht in Deutschland wohnhaft\? Hier profitieren\.", 0),
        (r"^Ein Artikel aus der «NZZ am Sonntag.*", re.MULTILINE),
        (r"^Zur Person.*", re.MULTILINE),
        (r"^(AFP|EPA|ETH Bildarchiv|Keystone|AP|Reuters)\b.*", re.MULTILINE),
        (r"^Bilder?\b.*", re.MULTILINE),
        (r"Comet Photo ?/.*", 0),
        (r"Sie können .* auf den Plattformen X,?Linkedin und Xing folgen\.", 0),
        (r".*\bEmpfehlung(en)?\b.*", 0),
        (r"^\s*Youtube\s*$", re.MULTILINE),
        (r"Trailer\. Youtube", 0),
        (r"^Bildstrecke\b.*", re.MULTILINE),
        (r"^[a-z]{2,4}\.$", re.MULTILINE) # Pattern solo minuscolo
    ]

    try:
        # Applica ogni pattern sequenzialmente con i flag corretti
        for pattern, flags in patterns_to_apply:
            # re.compile combinerà i flag inline (es. ?s)
            # con i flag aggiuntivi (es. re.MULTILINE)
            compiled_regex = re.compile(pattern, flags)
            text = compiled_regex.sub('', text)

        # Pulizia finale e normalizzazione spazi bianchi
        # (Come da "normalizzare gli spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_nzz_ch': {e}")
        return plain_text
    



def clean_oglobo_globo_com(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'oglobo.globo.com' (O Globo).
    Applica 48 pattern (47+1) sequenziali per rimuovere riassunti IA,
    link CTA, intestazioni di colonna (es. Míriam Leitão),
    crediti foto, allerte meteo (Inmet), istruzioni della lotteria
    (Mega-Sena) e URL.
    
    Rispetta i flag regex inline (es. ?i) e la case-sensitivity
    dove specificato nel jsonl. Applica re.MULTILINE a tutti
    i pattern per il corretto funzionamento di ^ e $.

    Regole applicate (basate sul jsonl):
    - Rimozione sequenziale di 48 pattern con flag misti.
    - Flag re.MULTILINE applicato a tutti.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista dei 47 pattern estratti dal JSON
    # (Le doppie backslash \\ del JSON sono convertite in singole \)
    patterns_da_rimuovere = [
        # Riassunti IA e CTA
        r"^RESUMO\b.*",
        r"Sem tempo\? Ferramenta de IA resume para você",
        r"^GERADO EM:\s?.*",
        # Link "Leggi anche"
        r"^Leia também:.*",
        r"^Veja também:.*",
        r"^Veja novos valores:.*",
        r"^Veja imagens.*",
        r"^Veja registros.*",
        r"^Veja:.*",
        r"^Conheça:.*",
        r"^Saiba mais:.*",
        r"^Saiba quem é:.*",
        r"^Entenda:.*",
        r"^Enquanto isso\..*", # Semplificato da \{3}
        r"^Enquanto isso:.*",
        r"^Por enquanto:.*",
        r"^Separadas:.*",
        # Colonne e Sezioni (Case-Sensitive)
        r"^Míriam Leitão:.*",
        r"^Lauro Jardim:.*",
        r"^Análise:.*",
        r"^Editorial:.*",
        r"^Reunião em Washington:.*",
        r"^Negócio frustrado:.*",
        r"^Aviação civil:.*",
        r"^Captação lá fora:.*",
        r"^CNU 2025:.*",
        # Newsletter e CTA
        r"^Newsletters?:.*",
        r".*\(clique aqui\).*",
        r"^Assine aqui.*",
        r"^Quer receber.*Inscreva-se.*",
        r"^Correção:.*",
        # Crediti Foto e Agenzie
        r" ?— Foto:.*",
        r"^.*— Foto:.*$",
        r"\(\*?Com .*?agências.*?.\)", # Semplificato da \)
        # Allerte Meteo (Inmet)
        r"^Alertas do Inmet.*",
        r"A recomendação do Instituto Nacional de Meteorologia \(Inmet\).*?telefone 193\)\.?",
        r"^Previsão (da|de) .* — Foto:.*",
        r"^Temperaturas mínima.*",
        r"^Previsão do tempo.* — Foto:.*",
        # Lotteria (Case-Insensitive con ?i)
        r"(?i)^As apostas para a Mega-?sena.*",
        r"(?i)^Os sorteios acontecem sempre.*",
        r"(?i)^O palpite mínimo custa.*",
        r"(?i)^O Bolão Caixa.*",
        r"(?i)^Veja os dez maiores prêmios sorteados na História da Mega-?sena:.*",
        # Risultati Lotteria (Case-Sensitive)
        r"^Concurso \d{1,4} \(.+\) — R\$ .*",
        r"^Alertas do Inmet para .* — Foto:.*",
        r"^Previsão de chuva.* — Foto:.*",
        
        # Pattern aggiunto da 'altri_suggerimenti'
        r"https?://\S+"
    ]

    try:
        # Applica ogni pattern sequenzialmente
        # Applica re.MULTILINE a tutti per far funzionare ^ e $
        # I flag inline (es. ?i) vengono rispettati da re.compile
        for pattern in patterns_da_rimuovere:
            compiled_regex = re.compile(pattern, re.MULTILINE)
            text = compiled_regex.sub('', text)

        # Pulizia finale e normalizzazione spazi bianchi
        # (Come da "normalizzare spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_oglobo_globo_com': {e}")
        return plain_text
    



def clean_reforma_com(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'reforma.com' (Reforma).
    Applica 16 pattern sequenziali per rimuovere crediti foto, 
    timestamp, URL/handle tra parentesi, righe CTA e, in particolare,
    righe interamente in MAIUSCOLO (case-sensitive).
    
    Rispetta i flag regex inline (?mi, ?i, ?m) forniti nel jsonl.
    Esegue anche la deduplicazione di righe consecutive.

    Regole applicate (basate sul jsonl):
    - Rimozione sequenziale di 16 pattern con flag misti.
    - Deduplicazione di righe identiche consecutive.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista dei 16 pattern con flag inline.
    # Verranno applicati sequenzialmente.
    # (Le doppie backslash \\ del JSON diventano singole \)
    patterns_to_apply = [
        r"(?mi)^\s*Cr[eé]dito:\s.*$",
        r"(?mi)^\s*Tomad[ao]a?\s+(de|del)\b.*$",
        r"(?mi)^\s*/?\s*Cortes[ií]a\b.*$",
        r"(?mi)^.*\(\d{1,2}\s+\w+\s+\d{4}\)\s*\.-\s*\d{1,2}:\d{2}\s*hrs.*$",
        r"(?i)\b\d{1,2}\s*MIN\s*\d{1,2}\s*SEG\b",
        r"(?i)\bEN\s+REFORMA\b",
        r"(?mi)^\s*OPINI[ÓO]N\s*$",
        r"(?mi)^\s*LEE SU TEXTO AQU[IÍ].*$",
        # Pattern Case-Sensitive (solo ?m) per rimuovere titoli/header
        r"(?m)^(?:[A-ZÁÉÍÓÚÑ0-9][A-ZÁÉÍÓÚÑ0-9\s'’\.,:;¡!¿?\-]+)$",
        # URL in parentesi
        r"\((?:https?://|www\.)[^)]+\)",
        r"(?mi)^\s*[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+){1,3}\.[\s]*$",
        r"(?mi)^\s*Foto:\s.*$",
        r"(?mi)^\s*Tomadal\s+del\s+IG:.*$",
        r"(?mi)^\s*Lee aqu[ií]\s+la cr[ií]tica.*$",
        r"(?i)\bTomad[ao]s?\s+de(?:l)?\s+(?:www\.)?[A-Za-z0-9._\-/]+", # @ non è qui
        r"(?i)\s*/\s*Cortes[ií]a\b[^\n]*"
    ]

    try:
        # 1. Applica ogni pattern sequenzialmente
        for pattern in patterns_to_apply:
            # re.sub rispetta i flag inline (es. ?m, ?i, ?mi)
            text = re.sub(pattern, '', text)

        # 2. Deduplicazione (da 'altri_suggerimenti')
        # Rimuove righe identiche consecutive, lasciandone una sola.
        # Usa (?m) per far funzionare ^, cerca una riga (.+)
        # seguita da una o più copie esatte (\n\1)+
        dedup_pattern = r"(?m)^(.+)(\n\1)+$"
        text = re.sub(dedup_pattern, r'\1', text)

        # 3. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "normalizzare spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_reforma_com': {e}")
        return plain_text
    



def clean_repubblica_it(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'repubblica.it' (La Repubblica).
    Versione CORRETTA senza regex catastrofiche.
    """
    if not plain_text:
        return ""

    text = plain_text

    patterns_to_apply = [
        (r"(?i)^SEGUI.*$", re.MULTILINE),
        (r"(?i)^a cura di .+$", re.MULTILINE),
        (r"(?i)leggi anche:?[^\n]*", 0),
        (r"\b[\w._%+-]+@[\w.-]+\.[A-Za-z]{2,}\b", 0),
        # Case-Sensitive Dateline
        (r"^(?:[A-ZÀ-Ü]{2,}(?:\s+[A-ZÀ-Ü]{2,})*)\s+–\s+", re.MULTILINE), 
        (r"^[0-3]?\d\s+[A-Za-zÀ-ÿ]+\s+20\d{2}\s*-\s*\d{1,2}\.\d{2}\s*\([^)]+\)\s*-.*$", re.MULTILINE),
        (r"^[0-3]?\d\s+[A-Za-zÀ-ÿ]+\s+20\d{2}\s*$", re.MULTILINE),
        (r"(?i)^Punti chiave\s*$", re.MULTILINE),
        # Case-Sensitive Live Timestamp
        (r"^\d{1,2}:\d{2}\s.+$", re.MULTILINE), 
        (r"(?i)\((?:foto|getty|ansa|reuters|ap|corbis|afp|epa)[^)]*\)", 0),
        
        # --- REGEX RIMOSSE (CAUSAVANO IL BLOCCO) ---
        # (r"^(?:[^/\n]+\s/\s[^/\n]+\s*)+$", re.MULTILINE), 
        # (r"^(?:[^/\n]+\s/\s)+", re.MULTILINE),
        # (r"^.*? / [A-Z0-9ÁÉÍÓÚÜÑ .'-]+$", re.MULTILINE), 
        # (r"^.*? / .*?(AP|AFP|REUTERS|EFE|Getty Images|Europa Press|APN|POOL|DPA|LaPresse).*?$", re.MULTILINE),
        # --- FINE RIMOZIONE ---
        
        # Sostituite con una versione più sicura e specifica
        (r"^\s*Crediti:\s*[^/]+/[^/]+$", re.IGNORECASE | re.MULTILINE),
        (r"\s+/\s+(AP|AFP|REUTERS|EFE|Getty Images|Europa Press|POOL|DPA|LaPresse)\s*$", re.IGNORECASE | re.MULTILINE),

        (r"(?i)\(?\bQUI\b[^\n\.!?)]*\)?", 0),
        (r"(?i)^info e prenotazioni.*$", re.MULTILINE),
        (r"(?i)seguici su\s+(facebook|x|twitter|instagram|tiktok)[^\n]*", 0),
        (r"(?i)iscriviti alla newsletter[^\n]*", 0),
        (r"(?i)^Aggiornato (alle|il) .*$", re.MULTILINE),
        (r"(?i)^\s*\(\s*\d{4}\s*Getty Images\s*\)\s*$", re.MULTILINE),
        (r"(?i)^\s*Foto:?\s*[^\n]*$", re.MULTILINE),
        (r"(?i)^\s*Credits?:?\s*[^\n]*$", re.MULTILINE),
        (r"(?i)^Cosa ci piace\s*\?$", re.MULTILINE),
        (r"(?i)^Cosa non ci piace\s*\?$", re.MULTILINE)
    ]

    try:
        # Applica ogni pattern sequenzialmente
        for pattern, flags in patterns_to_apply:
            # Assicurati di usare il modulo 'regex' (importato come re)
            # Aggiungi un timeout per sicurezza contro future regex bomb
            compiled_regex = re.compile(pattern, flags)
            text = compiled_regex.sub('', text)

        # Pulizia finale (usa re normale o regex, è indifferente qui)
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_repubblica_it': {e}")
        return plain_text



def clean_rt_com(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'rt.com' (Russia Today).
    Rimuove CTA (error reporting, subscribe), crediti foto (Getty, RIA),
    disclaimer AI, timestamp, blocchi "Leggi anche", URL e 
    marcatori "RT)".
    
    Esegue anche una deduplicazione delle righe identiche consecutive
    come da suggerimenti.

    Regole applicate (basate sul jsonl):
    - Rimozione di 14 pattern specifici + 1 per URL.
    - Flag re.IGNORECASE e re.MULTILINE applicati globalmente.
    - Deduplicazione di righe consecutive.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista dei 14 pattern estratti dal JSON
    # (Le doppie backslash \\ del JSON sono convertite in singole \)
    patterns_da_rimuovere = [
        # CTA "Errore nel testo"
        r"Ошибка в тексте\?\s*Выделите её и нажмите\s*«?Ctrl\s*\+\s*Enter»?",
        r"Ctrl\s*\+\s*Enter",
        r"Enter»", # Frammento
        # "Leggi anche"
        r"^\s*Также по теме.*$",
        r"^\s*Читайте также.*$",
        # Disclaimer AI
        r"^\s*[—\-–]?\s*Сгенерировано с помощью ИИ\s*$",
        # Crediti Foto/Agenzie
        r"[\-—–]\s*(Gettyimages\.ru|globallookpress\.com)\b",
        r"^\s*(Gettyimages\.ru|globallookpress\.com)\s*$",
        r"РИА Новости\s*©[^\n]*",
        r"(^|\s)[\-—–]\s*РИА Новости\b",
        # Copyright
        r"^\s*©\s?.+$",
        # Timestamp (il pattern [а-яё] ora gestirà anche i mesi 
        # in maiuscolo grazie a re.IGNORECASE)
        r"^\s*\d{1,2}\s+[а-яё]+\s+202\d,\s*\d{1,2}:\d{2}\s*$",
        # CTA "Subscribe"
        r"^\s*Подписывайтесь на наш канал.*$",
        # Marcatori
        r"—\s?RT\)",
        
        # Pattern aggiunto da 'altri_suggerimenti' ("rimuovere URL")
        r"https?://\S+"
    ]

    try:
        # 1. Pulizia principale (Pattern matching)
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # Compila con IGNORECASE ("normalizzare minuscolo")
        # e MULTILINE (per ^ e $)
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        text = compiled_regex.sub('', text)

        # 2. Deduplicazione (da 'altri_suggerimenti')
        # Cerca una riga (.+) seguita da un "a capo" (\n)
        # e una o più copie esatte della prima riga (\1)+
        dedup_pattern = r"(?m)^(.+)(\n\1)+$"
        # Sostituisce l'intero blocco duplicato con una singola
        # istanza della riga (\1)
        text = re.sub(dedup_pattern, r'\1', text)

        # 3. Pulizia finale e normalizzazione spazi bianchi
        # (Come da "comprimere spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_rt_com': {e}")
        return plain_text
    



def clean_smh_com_au(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'smh.com.au' (Sydney Morning Herald).
    Applica 8 pattern sequenziali per rimuovere crediti foto inline 
    (case-sensitive), pubblicità e CTA (case-insensitive).
    Esegue inoltre la deduplicazione delle righe consecutive.
    
    Rispetta la logica mista (flag ?i e assenza di flag)
    fornita nel jsonl.

    Regole applicate (basate sul jsonl):
    - Rimozione sequenziale di 8 pattern con flag misti.
    - Deduplicazione di righe identiche consecutive.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista di tuple: (pattern_string, flag_aggiuntivi)
    # L'applicazione sequenziale è necessaria per la logica mista.
    patterns_to_apply = [
        # --- Pattern Case-Sensitive (Crediti Foto) ---
        # (Nessun ?i, nessun re.IGNORECASE)
        (r"\s*/\s*Getty Images\b", 0),
        (r"\s*/\s*Credit:\s*/?\s*[A-Za-z .'-]+", 0),
        (r"(?:\s*/\s*)(?:AAP|Reuters|AP|Bloomberg|AFP|EPA|WireImage)\b", 0),

        # --- Pattern Case-Insensitive (?i) ---
        # (Aggiungiamo re.MULTILINE dove si usa ^ o $)
        (r"(?i)^\s*Advertisement\s*$", re.MULTILINE),
        (r"(?i)^\s*(Read more|Related|Recommended|Most (?:read|viewed))\b.*", re.MULTILINE),
        (r"(?i)^\s*(Sign up|Subscribe|Get our [^\n]*newsletter|Newsletter|Breaking News Alert)\b.*", re.MULTILINE),
        (r"(?i)^\s*(Leave a comment|Comments|Share|Save|Print)\b.*", re.MULTILINE),
        (r"(?i)\bLicense this article\b", 0) # Inline, non servono flag
    ]

    try:
        # 1. Applica ogni pattern di pulizia sequenzialmente
        for pattern, flags in patterns_to_apply:
            # re.compile combinerà i flag inline (es. ?i)
            # con i flag aggiuntivi (es. re.MULTILINE)
            compiled_regex = re.compile(pattern, flags)
            text = compiled_regex.sub('', text)

        # 2. Deduplicazione (da 'altri_suggerimenti')
        # Cerca una riga (.+) seguita da un "a capo" (\n)
        # e una o più copie esatte della prima riga (\1)+
        # Flag (?m) = re.MULTILINE
        dedup_pattern = r"(?m)^(.+)(\n\1)+$"
        # Sostituisce l'intero blocco duplicato con una singola
        # istanza della riga (\1)
        text = re.sub(dedup_pattern, r'\1', text)

        # 3. Pulizia finale e normalizzazione spazi bianchi
        # (Come da "normalizzare spazi e unire righe spezzate")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_smh_com_au': {e}")
        return plain_text
    



def clean_spiegel_de(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'spiegel.de' (Der Spiegel).
    Applica 18 pattern sequenziali con logica mista: rimuove prima
    i blocchi embed DOTALL (?s), poi i pattern case-insensitive (?i)
    e case-sensitive (es. crediti foto).
    
    Esegue inoltre la deduplicazione delle righe consecutive.

    Regole applicate (basate sul jsonl):
    - Rimozione sequenziale di 2 pattern DOTALL (?s).
    - Rimozione sequenziale di 16 pattern con flag misti (?i / case-sensitive).
    - Deduplicazione di righe identiche consecutive.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    try:
        # 1. Applica i pattern DOTALL (?s) per primi
        dotall_patterns = [
            # Blocco Embed Esterno
            r"(?s)Empfohlener externer Inhalt[\s\S]*?Datenschutzerklärung\.",
            # Blocco Embed Esterno (variante)
            r"(?s)An dieser Stelle finden Sie einen externen Inhalt von [A-Za-zÄÖÜäöü]+[\s\S]*?Datenschutzerklärung\."
        ]
        
        for pattern in dotall_patterns:
            # re.sub rispetta il flag inline (?s)
            text = re.sub(pattern, '', text)

        # 2. Applica i restanti pattern (misti) sequenzialmente
        # Lista di tuple: (pattern_string, flag_aggiuntivi)
        other_patterns = [
            (r"(?i)^Mehr zum Thema.*$", re.MULTILINE),
            (r"(?i)^Lesen Sie mehr.*Themenspezial.*$", re.MULTILINE),
            (r"(?i)^.*Mehr .*lesen Sie hier.*$", re.MULTILINE),
            (r"(?i)^.*Lesen Sie (hier|dazu).*$", re.MULTILINE),
            (r"(?i)\(Mehr dazu lesen Sie hier\.?\)", 0),
            (r"^Hier geht'?s zum Archiv\.$", re.MULTILINE), # Case-sensitive
            (r"^Podcast Cover$", re.MULTILINE), # Case-sensitive
            (r"^Dieses Audio ist derzeit nicht verfügbar\.$", re.MULTILINE), # Case-sensitive
            (r"(?i)^Sie haben Themenvorschläge.*$", re.MULTILINE),
            (r"(?i)WhatsApp an \+?\d[\d\s\-()]{6,}", 0),
            (r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", 0), # Case-sensitive
            (r"^Foto:\s?.*$", re.MULTILINE), # Case-sensitive
            (r" ?/ Foto: [^\n]+", 0), # Case-sensitive
            # Case-sensitive (include REUTERS e Reuters)
            (r" ?/ (?:dpa|AFP|AP|REUTERS|Reuters|IMAGO|imago|picture alliance|EPA|Getty Images|ZUMAPRESS|ddp|KNA|ANP|AA|Bloomberg|AP Photo|APF)[^\n]*", 0),
            (r" ?/ CC BY[^\n]*", 0), # Case-sensitive
            (r"(?i)^Mehr über den Streit lesen Sie hier\.?$", re.MULTILINE)
        ]

        for pattern, flags in other_patterns:
            # re.compile combinerà i flag inline (es. ?i)
            # con i flag aggiuntivi (es. re.MULTILINE)
            compiled_regex = re.compile(pattern, flags)
            text = compiled_regex.sub('', text)

        # 3. Deduplicazione (da 'altri_suggerimenti')
        # Cerca una riga (.+) seguita da un "a capo" (\n)
        # e una o più copie esatte della prima riga (\1)+
        # Flag (?m) = re.MULTILINE
        dedup_pattern = r"(?m)^(.+)(\n\1)+$"
        text = re.sub(dedup_pattern, r'\1', text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        # (Come da "normalizzare spaziatura")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_spiegel_de': {e}")
        return plain_text
    



def clean_standaard_be(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'standaard.be' (De Standaard).
    Rimuove 20 pattern specifici, inclusi crediti agenzia,
    orari TV, link streaming, CTA (podcast/paywall), URL ed email.
    
    Esegue anche una deduplicazione delle righe identiche consecutive
    come da suggerimenti.

    Regole applicate (basate sul jsonl):
    - Rimozione di 20 pattern (combinati).
    - Flag re.IGNORECASE e re.MULTILINE applicati globalmente.
    - Deduplicazione di righe consecutive.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista dei 20 pattern estratti dal JSON
    # Ho rimosso il flag (?i) inline da ciascuno per applicarlo
    # globalmente con re.IGNORECASE.
    # (Le doppie backslash \\ del JSON sono convertite in singole \)
    patterns_da_rimuovere = [
        r"^\s*Lees ook:.*$",
        # Crediti agenzia (es. (belga), (reuters))
        r"\s*\(((?:belga|reuters|afp|ap|apa|bloomberg|nyt)[^)]*)\)\s*$",
        r",\s*red\.\)",
        r"\s*\(red\.\)",
        # Orari TV
        r"^\s*(VRT(?:\s*1|\s*Canvas)?|NPO\s*[23]?|VTM(?:\s*[23])?|BBC2?|Canvas|Comedy Central|Sporza|VRT\s*Max|NPO\s*Radio\s*1)\s*,\s*\d{1,2}\.\d{2}-\d{1,2}\.\d{2}\s*uur\s*$",
        r"\b\d{1,2}\.\d{2}-\d{1,2}\.\d{2}\s*uur\b",
        # Link Streaming
        r"^\s*(Op|Te bekijken op)\s+(Netflix|HBO\s*Max|Streamz|Amazon\s*Prime|Prime|VRT\s*Max|Disney\+|Apple\s*TV\+)\b.*$",
        # CTA Podcast
        r"^Waar kan ik luisteren\?.*$",
        r"^Alle podcasts die getipt worden.*$",
        r"^Podcasts van VRT zijn ook te beluisteren.*$",
        # Crediti
        r"^(CREDITS|CREDITSGast).*$",
        r"^(Foto|Beeld|Illustratie):.*$",
        # Contatti
        r"\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b",
        r"https?://\S+",
        # Paywall
        r"^\s*Onbeperkt alle artikels.*$",
        r"^\s*Sluit je aan als abonnee.*$",
        r"^\s*Investeer in inzicht.*$",
        # CTA Moduli
        r"^\s*Denkt u eraan.*$",
        r"^\s*Vul dan onderstaand formulier.*$",
        r"^.*QR-code.*$"
    ]

    try:
        # 1. Pulizia principale (Pattern matching)
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # Compila con IGNORECASE ("minuscolizzare")
        # e MULTILINE (per ^ e $)
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        text = compiled_regex.sub('', text)

        # 2. Deduplicazione (da 'altri_suggerimenti')
        # Cerca una riga (.+) seguita da un "a capo" (\n)
        # e una o più copie esatte della prima riga (\1)+
        dedup_pattern = r"(?m)^(.+)(\n\1)+$"
        # Sostituisce l'intero blocco duplicato con una singola
        # istanza della riga (\1)
        text = re.sub(dedup_pattern, r'\1', text)

        # 3. Pulizia finale e normalizzazione spazi bianchi
        # (Come da "normalizzare spazi bianchi e righe vuote")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_standaard_be': {e}")
        return plain_text
    


def clean_sueddeutsche_de(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'sueddeutsche.de' (Süddeutsche Zeitung).
    Applica una logica mista: prima rimuove un blocco embed/cookie
    DOTALL (?s), poi rimuove 15 pattern case-sensitive
    (crediti dpa, didascalie, CTA, URL).
    
    Infine, esegue una deduplicazione delle righe consecutive.

    Regole applicate (basate sul jsonl):
    - Rimozione di 1 pattern DOTALL (?s) (embed).
    - Rimozione combinata di 15 pattern (case-sensitive + multiline).
    - Deduplicazione di righe identiche consecutive.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    try:
        # 1. Applica il pattern DOTALL (?s) per primo
        # (Usa [\s\S] per matchare newline)
        dotall_pattern = (
            r"Die SZ-Redaktion hat diesen Artikel mit einem Inhalt von[^\n]*"
            r"angereichert[\s\S]*?sz\.de/datenschutz\."
        )
        text = re.sub(dotall_pattern, '', text)

        # 2. Lista dei restanti pattern (case-sensitive)
        # (Le doppie backslash \\ del JSON sono convertite in singole \)
        other_patterns = [
            r"© dpa-infocom, dpa:\d{6}-\d{3}-\d+(?:/\d+)?",
            r"© dpa-infocom[^\n]*",
            r"Ich bin damit einverstanden, dass mir Inhalte von [^\n]+ angezeigt werden\.",
            r"Mehr Informationen und eine Widerrufsmöglichkeit finden Sie unter sz\.de/datenschutz\.",
            r"\((?:Foto|Archivbild|Symbolbild):[^)]+\)",
            r"\s*/\s*[^\n]*\((?:Foto|Archivbild|Symbolbild):[^)]+\)",
            r"\(Anm\. d\. Red\.[^)]+\)",
            r"SZ Bayern auf Whatsapp[^\n]*",
            r"Von Aschaffenburg bis Berchtesgaden:[^\n]*",
            r"Hier entlang, [^\n]*aufs Handy[^\n]*",
            r"^(?:US-Kolumne|SZ-Kolumne|Live [^:]{1,100}|Partys heute in München|Alternative Hotspots in München|Semesterstart|Landkreis [A-Za-zÄÖÜäöüß ]+)\s*:[^\n]+$",
            r"\s*\(PA Wire/dpa\)|\s*\(dpa\)|\s*\(dpa/[a-z]+\)",
            r"\s*\(Symbolbild\)",
            r"\s*\(Archivbild\)",
            
            # Pattern aggiunto da 'altri_suggerimenti'
            r"https?://\S+"
        ]

        # 3. Combina e applica i pattern case-sensitive
        combined_pattern = "|".join(other_patterns)
        
        # Compila con MULTILINE (per ^ e $)
        # NON usa re.IGNORECASE (i pattern sono case-sensitive)
        compiled_regex = re.compile(combined_pattern, re.MULTILINE)
        text = compiled_regex.sub('', text)

        # 4. Deduplicazione (da 'altri_suggerimenti')
        # Cerca una riga (.+) seguita da un "a capo" (\n)
        # e una o più copie esatte della prima riga (\1)+
        dedup_pattern = r"(?m)^(.+)(\n\1)+$"
        text = re.sub(dedup_pattern, r'\1', text)

        # 5. Pulizia finale e normalizzazione spazi bianchi
        # (Come da "normalizzare spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_sueddeutsche_de': {e}")
        return plain_text
    



def clean_svd_se(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'svd.se' (Svenska Dagbladet).
    Rimuove 19 pattern (17+2) case-sensitive, inclusi paywall/promo,
    crediti foto/podcast, metadati editoriali, URL, email e 
    simboli elenco (■).
    
    Esegue inoltre la deduplicazione delle righe consecutive.
    NON usa IGNORECASE.

    Regole applicate (basate sul jsonl):
    - Rimozione combinata di 19 pattern (case-sensitive + multiline).
    - Deduplicazione di righe identiche consecutive.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista dei 17 pattern estratti dal JSON
    # (Le doppie backslash \\ del JSON sono convertite in singole \)
    patterns_da_rimuovere = [
        # Paywall e Promo
        r"Läs gratis till 25 december",
        r"Tillgång till alla artiklar, inklusive eSvD\.",
        r"Prova nu\s*Prenumerera och få obegränsad tillgång",
        r"Prova nuBesök SvD\.se för mer information\.",
        r"Besök SvD\.se för mer information\.",
        r"Förnyas till halva priset i \d+ månader \([^)]+\)\. Ordinarie pris [^\n]+\.",
        r"Visa hela artikeln\?",
        r"Prenumerera och få obegränsad tillgång",
        # Crediti Foto e Podcast
        r"Foto:[^\n]*",
        r"SvD Ledarredaktionen\s*-\s*\d+\s*min",
        r"Lyssna på fortsättningen på svd\.se/[^\s]+(?: eller hos Podme)?\.",
        r"En produktion från Svenska Dagbladet och Podme\.",
        # Metadati editoriali
        r"Ansvarig utgivare:[^\n]*",
        r"Kontakta oss:[^\n]*",
        # Contatti e Artefatti
        r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}",
        r"svd\.se/badfluence",
        r"eSvD\b",

        # Pattern aggiunti da 'altri_suggerimenti'
        r"https?://\S+", # Rimuove URL
        r"■" # Rimuove simboli elenco
    ]

    try:
        # 1. Pulizia principale (Pattern matching)
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # Compila con MULTILINE (per ^ e $)
        # NON usa re.IGNORECASE (patterns sono case-sensitive)
        compiled_regex = re.compile(combined_pattern, re.MULTILINE)
        text = compiled_regex.sub('', text)

        # 2. Deduplicazione (da 'altri_suggerimenti')
        # Cerca una riga (.+) seguita da un "a capo" (\n)
        # e una o più copie esatte della prima riga (\1)+
        dedup_pattern = r"(?m)^(.+)(\n\1)+$"
        # Sostituisce l'intero blocco duplicato con una singola
        # istanza della riga (\1)
        text = re.sub(dedup_pattern, r'\1', text)

        # 3. Pulizia finale e normalizzazione spazi bianchi
        # (Come da "normalizzare spazi bianchi e line-break")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_svd_se': {e}")
        return plain_text
    



def clean_tass_ru(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'tass.ru' (TASS).
    Rimuove dateline, timestamp, branding TASS (in vari formati),
    note redazionali e intestazioni di sezione ("О ...").
    
    Esegue anche una deduplicazione delle righe identiche consecutive
    e applica re.IGNORECASE globalmente, come da suggerimenti.

    Regole applicate (basate sul jsonl):
    - Rimozione combinata di 16 pattern specifici.
    - Flag re.IGNORECASE e re.MULTILINE applicati globalmente.
    - Deduplicazione di righe identiche consecutive.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista dei 16 pattern estratti dal JSON
    # (Le doppie backslash \\ del JSON sono convertite in singole \)
    patterns_da_rimuovere = [
        # Dateline (diventerà case-insensitive)
        r"^[A-ZА-ЯЁ\-\s(),]+, \d{1,2} [а-яё]+\. ?/ТАСС/\.?$",
        r"^Редакция сайта ТАСС.*$",
        r"/ТАСС/\.?",
        r"\bТАСС\b",
        # Disclaimer "TASS - general information..."
        r"ТАСС\s*[—-]\s*генеральн[а-яA-Za-z]*\s+информационн[а-яA-Za-z]*.*",
        r"\(есть у ТАСС\)",
        # Attribuzioni ".... reported TASS"
        r"[—-]\s*(сообщил[аи]?|рассказал[аи]?|передал[аи]?|передает|выяснил[аи]?|сообщили|отметил[аи]?|добавил[аи]?|уточнил[аи]?|прокомментировал[аи]?|подчеркнул[аи]?)\s+(корреспондент[а-яё]*\s+)?ТАСС\b",
        # Timestamp (potenziato con 'мск' opzionale da 'altri_suggerimenti')
        r"\b\d{1,2} [а-яё]+, \d{2}:\d{2}(\s*мск)?\b",
        # Sezioni "About..."
        r"^(О|Об|Обо) [^\n]{1,40}$",
        r"^[Кк]онкурсы$",
        r"^О компании\.?$",
        r"^О премии\.?$",
        r"^О форуме\.?$",
        r"^О Дне СПО\.?$",
        r"^Об СПО\.?$",
        r"\b(документ|исследование|отчет|текст) есть у ТАСС\b"
    ]

    try:
        # 1. Pulizia principale (Pattern matching)
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # Compila con IGNORECASE ("normalizzare minuscolo")
        # e MULTILINE (per ^ e $)
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        text = compiled_regex.sub('', text)

        # 2. Deduplicazione (da 'altri_suggerimenti')
        # Cerca una riga (.+) seguita da un "a capo" (\n)
        # e una o più copie esatte della prima riga (\1)+
        dedup_pattern = r"(?m)^(.+)(\n\1)+$"
        # Sostituisce l'intero blocco duplicato con una singola
        # istanza della riga (\1)
        text = re.sub(dedup_pattern, r'\1', text)

        # 3. Pulizia finale e normalizzazione spazi bianchi
        # (Come da "normalizzare ... spaziatura")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_tass_ru': {e}")
        return plain_text
    


def clean_thehindu_com(plain_text: str) -> str:
    """
    Pulisce il testo multilingue (Inglese, Hindi) da 'thehindu.com'.
    Rimuove 43 pattern specifici, inclusi timestamp (IST), 
    paywall/abbonamenti, CTA social, pubblicità, banner cookie, 
    sezioni 'Topics' e crediti foto, preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Rimozione combinata di 43 pattern multilingue.
    - Flag re.IGNORECASE e re.MULTILINE applicati globalmente.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    # Lista dei 43 pattern estratti dal JSON
    # (Le doppie backslash \\ del JSON sono convertite in singole \)
    patterns_da_rimuovere = [
        # Timestamp (Inglese e Hindi)
        r"^Published\s*[-–:]*\s*[A-Za-z]+\s+\d{1,2},\s+\d{4}.*?IST$",
        r"^Updated\s*[-–:]*\s*[A-Za-z]+\s+\d{1,2},\s+\d{4}.*?IST$",
        r"^[A-Za-z]+\s+\d{1,2},\s+\d{4}.*?IST\s*\|\s*Updated\s*[:\-]\s*.*?IST$",
        r"^[A-Za-z]+\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}\s*(?:am|pm)?\s*IST$",
        r"^प्रकाशित\s*[-–:]*\s*.*$",
        r"^अपडेट(?:ेड)?\s*[-–:]*\s*.*$",
        r"^अंतिम\s+अपडेट\s*[-–:]*\s*.*$",
        # "Leggi anche" (Inglese e Hindi)
        r"^Also\s*Read\b.*$",
        r"^यह\s*भी\s*पढ़ें\b.*$",
        r"^और\s*पढ़ें\b.*$",
        # Pubblicità (Inglese e Hindi)
        r"^ADVERTISEMENT$",
        r"^Advertisement\b.*$",
        r"^विज्ञापन\b.*$",
        # Topics (Inglese e Hindi)
        r"^Related\s*Topics\b.*$",
        r"^Topics\b.*$",
        r"^संबंधित\s*विषय\b.*$",
        r"^टॉपिक्स\b.*$",
        # Paywall / Login (Inglese e Hindi)
        r"^Subscribe\b.*$",
        r"^Subscription\b.*$",
        r"^सब्सक्राइब\b.*$",
        r"^सदस्यता\b.*$",
        r"^(?:Sign\s*in|Sign\s*up|Log\s*in|Register)\b.*$",
        r"^(?:साइन\s*इन|लॉग\s*इन|रजिस्टर)\b.*$",
        r"^Already\s+a\s+subscriber\?\b.*$",
        r"^This\s+article\s+is\s+available\s+to\s+subscribers\s+only\.?$",
        r"^Support\s+.*The\s+Hindu.*$",
        r"^E-?paper\b.*$",
        # Social / Commenti (Inglese e Hindi)
        r"^Share\b.*$",
        r"^साझा\s*करें\b.*$",
        r"^(?:Facebook|Twitter|WhatsApp|Telegram|Email|Print)\b.*$",
        r"^Comments?\b.*$",
        r"^टिप्पण(?:ी|ियां)\b.*$",
        # Crediti Foto (Inglese e Hindi)
        r"^Photo\s*:?\s*Credit\b.*$",
        r"^फोटो\s*:?\s*क्रेडिट\b.*$",
        # Cookie (Inglese e Hindi)
        r"^(We\s*use\s*cookies|Accept\s*Cookies|Cookie\s*Policy)\b.*$",
        r"^(हम\s*कुकीज़\s*का\s*उपयोग\s*करते\s*हैं|कुकी\s*नीति)\b.*$",
        # Newsletter e Footer
        r"^Subscribe\s+to\s+our\s+newsletter\b.*$",
        r"^न्यूज़लेटर\s+के\s+लिए\s+सब्सक्राइब\b.*$",
        r"^Back\s*to\s*top$",
        r"^शीर्ष\s*पर\s*वापस\s*जाएँ$",
        # Trending e Tempo di lettura
        r"^(Trending|Recommended|Latest\s+news|Most\s+read)\b.*$",
        r"^(ट्रेंडिंग|सिफारिश|ताज़ा\s+खबर|सबसे\s+ज़्यादा\s+पढ़ा\s+गया)\b.*$",
        r"^\d+\s*(min|मिनट)\s*read$"
    ]

    try:
        # 1. Combina tutti i pattern in uno solo
        combined_pattern = "|".join(patterns_da_rimuovere)
        
        # 2. Compila la regex
        #    re.IGNORECASE: Per 'ADVERTISEMENT'/'Advertisement'/'Subscribe'
        #    re.MULTILINE:  Per far funzionare ^ e $ su ogni riga
        compiled_regex = re.compile(combined_pattern, re.IGNORECASE | re.MULTILINE)
        
        # 3. Applica la pulizia
        text = compiled_regex.sub('', plain_text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        #    (Come da "normalizzare spazi bianchi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_thehindu_com': {e}")
        return plain_text
    



def clean_yomiuri_co_jp(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'yomiuri.co.jp' (Yomiuri Shimbun).
    Come primo passo, normalizza i caratteri full-width (Zenkaku)
    in half-width (Hankaku) (es. ０ -> 0) come da suggerimenti.
    
    Successivamente, applica 22 pattern sequenziali (logica mista) 
    per rimuovere CTA, commenti, URL, crediti redazionali e
    numeri di pagina, preparando il testo per il chunking.

    Regole applicate (basate sul jsonl):
    - Normalizzazione Unicode NFKC (full-width -> half-width).
    - Rimozione sequenziale di 22 pattern con flag misti.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    try:
        # 1. Normalizzazione (da 'altri_suggerimenti')
        # Converte caratteri full-width (es. ０, Ａ) in
        # half-width (es. 0, A) e normalizza la compatibilità.
        text = unicodedata.normalize('NFKC', plain_text)

    except Exception:
        text = plain_text # Fallback

    # 2. Lista dei pattern (pattern_string, flag_aggiuntivi)
    # L'applicazione sequenziale è necessaria per la logica mista.
    # (Le doppie backslash \\ del JSON diventano singole \)
    patterns_to_apply = [
        # Flag aggiuntivo re.MULTILINE per ^ e $
        (r"^\s*[\-–—]\s*標準\s*-\s*拡大.*$", re.MULTILINE),
        (r"^メディカルトリビューン(?:\s+メディカルトリビューン)?$", re.MULTILINE),
        (r"^医療・健康・介護のコラム$", re.MULTILINE),
        (r"^社会(?:\s+社会)?$", re.MULTILINE),
        (r"^【関連記事】.*$", re.MULTILINE),
        (r"（関連記事「.*?」）", 0),
        (r"^【写真[^】]*】.*$", re.MULTILINE),
        (r"つづきを読む", 0),
        (r"違反報告", 0),
        (r"^※コメントは承認制.*$", re.MULTILINE),
        (r"^※個人情報は書き込まないでください。$", re.MULTILINE),
        (r"^※.*$", re.MULTILINE),
        (r"^\s*\d+\s*/\s*\d+\s*$", re.MULTILINE), # Paginazione
        (r"(?i)https?://\S+", 0), # Flag (?i) inline
        (r"http://\S+", 0), # Case-sensitive
        (r"問い合わせは.*（[0-9・\-－ー]+）へ。", 0), # Numeri già normalizzati
        (r"^トピ内ID：[0-9a-f]+$", re.MULTILINE),
        (r"^これポチに投票しよう！$", re.MULTILINE),
        (r"^[ 　]*ランキング本当に必要？$", re.MULTILINE),
        (r"^芸人\s.*?さん（\d+）$", re.MULTILINE),
        (r"^歌手\s.*?さん（\d+）$", re.MULTILINE),
        (r"^（[^）]*(編集部|ライター|聞き手|撮影)[^）]*）$", re.MULTILINE) # Crediti
    ]

    try:
        # 3. Applica ogni pattern sequenzialmente
        for pattern, flags in patterns_to_apply:
            # re.compile combinerà i flag inline (es. ?i)
            # con i flag aggiuntivi (es. re.MULTILINE)
            compiled_regex = re.compile(pattern, flags)
            text = compiled_regex.sub('', text)

        # 4. Pulizia finale e normalizzazione spazi bianchi
        # (Come da "normalizzare spazi")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_yomiuri_co_jp': {e}")
        return plain_text
    



def clean_zeit_de(plain_text: str) -> str:
    """
    Pulisce il testo estratto da 'zeit.de' (Die Zeit).
    Applica 13 pattern sequenziali con logica mista per rimuovere
    blocchi embed, metadati di articolo (es. ZEIT am Wochenende),
    disclaimer AI (Artikelzusammenfassung), crediti dpa, note
    redazionali (Anm. d. Red.) e URL.
    
    Rispetta i flag regex inline (?i, ?mi) e la case-sensitivity
    dove specificato (mantenendo la capitalizzazione tedesca).
    Esegue inoltre la deduplicazione delle righe consecutive.

    Regole applicate (basate sul jsonl):
    - Rimozione sequenziale di 13 pattern con flag misti.
    - Deduplicazione di righe identiche consecutive.
    - Normalizzazione e deduplicazione finale degli spazi bianchi.
    """
    if not plain_text:
        return ""

    text = plain_text

    # Lista dei 13 pattern con flag inline.
    # Verranno applicati sequenzialmente.
    # (Le doppie backslash \\ del JSON diventano singole \)
    patterns_to_apply = [
        # 1. Pattern Case-Sensitive (nessun flag ?i)
        r"An dieser Stelle ist ein externer Inhalt eingebunden Zum Anschauen benötigen wir Ihre Zustimmung",
        
        # 2. Pattern Case-Insensitive (?i) o (?mi)
        r"(?mi)^Dieser Artikel ist Teil von ZEIT am Wochenende, Ausgabe \d{1,2}\/\d{4}\.$",
        r"(?mi)^Dieser Artikel stammt aus unserem Ressort .*$",
        r"(?i)Mehr in Kürze hier bei der ZEIT",
        r"(?mi)^Artikelzusammenfassung.*$",
        r"(?mi)^Dies ist ein experimentelles Tool\..*$",
        r"(?mi)^Fanden Sie die Zusammenfassung hilfreich\?$",
        r"(?mi)^©\s*dpa-infocom.*$",
        r"(?mi)^dpa:[0-9\-]+.*$",
        r"(?i)\bdpa:[0-9\-]+(?:/\d+)?\b",
        r"(?i)\(Anm\. d\. Red\.\)|,\s*Anm\. d\. Red\.",
        r"(?i)Zum Anschauen.*Ihre Zustimmung",
        r"(?i)https?:/\S+|www\.[\w.-]+\.[a-z]{2,}"
    ]

    try:
        # 1. Applica ogni pattern sequenzialmente
        for pattern in patterns_to_apply:
            # re.sub rispetta i flag inline (es. ?i, ?mi)
            text = re.sub(pattern, '', text)

        # 2. Deduplicazione (da 'altri_suggerimenti')
        # Cerca una riga (.+) seguita da un "a capo" (\n)
        # e una o più copie esatte della prima riga (\1)+
        # Flag (?m) = re.MULTILINE
        dedup_pattern = r"(?m)^(.+)(\n\1)+$"
        # Sostituisce l'intero blocco duplicato con una singola
        # istanza della riga (\1)
        text = re.sub(dedup_pattern, r'\1', text)

        # 3. Pulizia finale e normalizzazione spazi bianchi
        # (Come da "normalizzare whitespace")
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    except re.error as e:
        print(f"Errore regex nel cleaner 'clean_zeit_de': {e}")
        return plain_text