from scrapy.exceptions import DropItem

class TextLengthValidationPipeline:
    """
    Pipeline per scartare gli item se il testo estratto è troppo corto,
    sintomo di un fallimento nell'estrazione (es. pagina di login, paywall).
    """
    def process_item(self, item, spider):
        min_length = 150  # Numero minimo di caratteri

        if not item.get('body_text') or len(item['body_text']) < min_length:
            raise DropItem(f"Testo troppo corto (< {min_length} caratteri) per {item['url']}")
        
        # Pulisce spazi bianchi extra prima di salvare
        item['body_text'] = ' '.join(item['body_text'].split())
        
        return item