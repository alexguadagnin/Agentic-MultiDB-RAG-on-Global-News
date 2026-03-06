BOT_NAME = 'news_scraper'
SPIDER_MODULES = ['news_scraper.spiders']
NEWSPIDER_MODULE = 'news_scraper.spiders'

# --- Output JSON ---
# Esporta automaticamente in 'output.json'
# Usiamo indent=4 per un JSON leggibile
FEEDS = {
    'output.jl': { # USA .jl COME ESTENSIONE
        'format': 'jsonlines', # USA jsonlines
        'encoding': 'utf8',
        'overwrite': False, 
        'indent': None, # jsonlines non usa indentazione
    }
}

# --- Politeness ---
ROBOTSTXT_OBEY = False # ;)

# Abilita AutoThrottle per adattare la velocità al server
CONCURRENT_REQUESTS = 64
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 1.0  # Inizia con 1 secondo di attesa
AUTOTHROTTLE_MAX_DELAY = 10.0 # Non superare i 10 secondi
AUTOTHROTTLE_TARGET_CONCURRENCY = 8.0
CONCURRENT_REQUESTS_PER_DOMAIN = 8 # Meno aggressivo del default

# --- Abilitazione Playwright ---
"""
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}
# Necessario per l'async di Playwright
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
"""

# Impostazioni Playwright
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True  # Esegui in background
}
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30000  # 30 secondi di timeout


# --- Abilitazione Fake User-Agent ---
# Disabilita il middleware User-Agent di default e attiva quello "fake"
DOWNLOADER_MIDDLEWARES = {
    'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
    'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
    'scrapy_fake_useragent.middleware.RandomUserAgentMiddleware': 400,
    'scrapy_fake_useragent.middleware.RetryUserAgentMiddleware': 401,
}
# Fallback se il servizio fake-useragent non risponde
FAKEUSERAGENT_FALLBACK = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'


# --- Abilitazione Pipeline ---
# Attiva la nostra pipeline di validazione personalizzata
ITEM_PIPELINES = {
   'news_scraper.pipelines.TextLengthValidationPipeline': 100,
}