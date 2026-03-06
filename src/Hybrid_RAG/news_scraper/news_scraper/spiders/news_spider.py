import scrapy
import trafilatura
import json
from news_scraper.items import NewsArticle
from scrapy_playwright.page import PageMethod
# Importa il reactor corretto per Playwright
from twisted.internet.asyncioreactor import AsyncioSelectorReactor
scrapy.utils.reactor.install_reactor('twisted.internet.asyncioreactor.AsyncioSelectorReactor')


class NewsSpider(scrapy.Spider):
    name = 'news'
    
    # Lunghezza minima per validare il testo
    MIN_TEXT_LENGTH = 150

    def start_requests(self):
        """
        Legge gli URL e genera richieste HTTP VELOCI (default).
        """
        try:
            with open('urls_top_rimasti.txt', 'r') as f: # Assicurati che legga dal file giusto
                urls = f.read().splitlines()
        except FileNotFoundError:
            self.logger.error("File 'urls_da_processare.txt' non trovato!")
            return
        
        for url in urls:
            if url.strip():
                yield scrapy.Request(
                    url,
                    callback=self.parse, # Manda alla callback VELOCE
                    errback=self.handle_error_http # Gestore errori HTTP
                )

    def parse(self, response):
        """
        CALLBACK VELOCE (HTTP): Prova a estrarre con Trafilatura.
        Se fallisce, rilancia la richiesta con Playwright.
        """
        
        # 1. Prova estrazione diretta
        article_text = trafilatura.extract(response.body, url=response.url)
        
        # 2. Controllo successo
        if article_text and len(article_text) > self.MIN_TEXT_LENGTH:
            # SUCCESSO! Estrai gli altri metadati (che rimarranno null)
            self.logger.info(f"Successo [HTTP] per {response.url}")
            item = NewsArticle()
            item['url'] = response.url
            item['body_text'] = article_text
            
            # Tentativo base di estrarre titolo da HTML grezzo
            item['title'] = response.css('title::text').get() 
            item['author'] = None # Trafilatura su HTML grezzo è pessimo per i metadati
            item['date'] = None
            yield item
        else:
            # FALLIMENTO. Riprova con Playwright
            self.logger.warning(f"Fallback [Playwright] per {response.url} (Testo troppo corto/nullo)")
            yield scrapy.Request(
                response.url,
                callback=self.parse_playwright, # Manda alla callback LENTA
                errback=self.handle_error_playwright,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "networkidle"),
                    ],
                }
            )

    async def parse_playwright(self, response):
        """
        CALLBACK LENTA (PLAYWRIGHT): Esegue l'estrazione completa
        su HTML renderizzato da JS.
        """
        page = response.meta.get("playwright_page")
        
        try:
            extracted_data_json = trafilatura.extract(
                await page.content(), # Usa il contenuto renderizzato dalla pagina
                output_format='json',
                include_comments=False,
                include_tables=False,
                url=response.url
            )
        except Exception as e:
            self.logger.error(f"Errore Trafilatura [Playwright] su {response.url}: {e}")
            extracted_data_json = None
        finally:
            if page:
                await page.close() # Chiudi la pagina!

        if not extracted_data_json:
            self.logger.error(f"Fallimento finale [Playwright] per {response.url}")
            return

        data = json.loads(extracted_data_json)

        if not data or not data.get('text'):
            self.logger.warning(f"Dati estratti vuoti [Playwright] per {response.url}")
            return

        # Popolamento Item (come prima)
        item = NewsArticle()
        item['url'] = data.get('source') or response.url
        item['title'] = data.get('title')
        item['body_text'] = data.get('text')
        item['author'] = data.get('author')
        item['date'] = data.get('date')
        
        yield item

    def handle_error_http(self, failure):
        """ Gestisce errori HTTP (es. 404, 500) """
        self.logger.error(f"Errore HTTP: {failure.request.url} - {failure.value}")
        # NOTA: Potresti voler rilanciare con Playwright anche qui
        # se l'errore è un 403 (Forbidden), che a volte Playwright supera.

    async def handle_error_playwright(self, failure):
        """ Gestisce errori Playwright (es. timeout) """
        self.logger.error(f"Errore Playwright: {failure.request.url} - {failure.value}")
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()