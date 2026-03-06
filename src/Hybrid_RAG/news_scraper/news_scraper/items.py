import scrapy

class NewsArticle(scrapy.Item):
    # Metadati che vogliamo
    url = scrapy.Field()
    title = scrapy.Field()
    body_text = scrapy.Field()
    author = scrapy.Field()
    date = scrapy.Field()