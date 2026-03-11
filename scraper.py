import requests
from bs4 import BeautifulSoup
import time
import random
import json
import re
import os
import cloudscraper
import urllib.parse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Globalna sesja i scraper
_session = None
_scraper_instance = None

# Lista User-Agentów do rotacji
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0'
]

NORD_SERVERS = [
    'nl.socks.nordhold.net:1080',
    'se.socks.nordhold.net:1080',
    'us.socks.nordhold.net:1080',
    'amsterdam.nl.socks.nordhold.net:1080',
    'atlanta.us.socks.nordhold.net:1080',
    'chicago.us.socks.nordhold.net:1080',
    'dallas.us.socks.nordhold.net:1080',
    'los-angeles.us.socks.nordhold.net:1080',
    'new-york.us.socks.nordhold.net:1080',
    'phoenix.us.socks.nordhold.net:1080',
    'san-francisco.us.socks.nordhold.net:1080',
    'stockholm.se.socks.nordhold.net:1080'
]

def get_session():
    """Zwraca aktywną sesję lub tworzy nową."""
    global _session
    if _session is None:
        _session = requests.Session()
        # Konfiguracja Retry (ponawianie prób przy błędach sieciowych)
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retries)
        _session.mount('http://', adapter)
        _session.mount('https://', adapter)

        # Inicjalne ustawienie proxy i nagłówków
        rotate_proxy(_session)

    return _session

def get_cloudscraper():
    """Zwraca instancję cloudscrapera (zachowuje sesję)."""
    global _scraper_instance
    if _scraper_instance is None:
        _scraper_instance = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
        )
        rotate_proxy(_scraper_instance)
    return _scraper_instance

def rotate_proxy(session_obj):
    """Losuje nowe proxy i User-Agenta dla podanej sesji."""
    new_agent = random.choice(USER_AGENTS)

    session_obj.headers.update({
        'User-Agent': new_agent,
        'Accept-Language': 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Referer': 'https://www.google.com/',
        'DNT': '1'
    })

    nord_user = os.environ.get('NORD_USER')
    nord_pass = os.environ.get('NORD_PASS')

    if nord_user and nord_pass:
        current_server = random.choice(NORD_SERVERS)
        safe_nord_pass = urllib.parse.quote(nord_pass)
        proxy_url = f"socks5h://{nord_user}:{safe_nord_pass}@{current_server}"
        proxies = {
            "http": proxy_url,
            "https": proxy_url
        }
        session_obj.proxies.update(proxies)
        print(f"[PROXY] Przełączono na serwer: {current_server}")
    else:
        print("[PROXY] Brak danych NordVPN - tryb bezpośredni.")
        session_obj.proxies = {}

def check_my_ip(proxies=None):
    """Helper do testowania IP."""
    try:
        session = get_session()
        response = session.get('http://api.ipify.org', timeout=10)
        return response.text
    except Exception as e:
        return f"Błąd ({e})"

def get_current_price(url):
    session = get_session()

    # Próba pobrania strony
    try:
        # Losowe opóźnienie, żeby nie zabić serwera docelowego, ale mniejsze niż wcześniej, bo sesja jest otwarta
        time.sleep(random.uniform(0.5, 1.0))

        response = session.get(url, timeout=15)

        # Jeśli dostaniemy bana (403, 429) lub błąd serwera, rotujemy proxy i ponawiamy RAZ
        if response.status_code in [403, 429, 503]:
            print(f"[!] Błąd {response.status_code}. Rotacja proxy i ponowna próba...")
            rotate_proxy(session)
            time.sleep(2)
            response = session.get(url, timeout=15)

        # Jeśli nadal błędy 400-500, używamy Cloudscrapera
        if 400 <= response.status_code < 500:
            print(f"[!] Błąd {response.status_code} dla {url}. Odpalam cloudscraper...")
            scraper = get_cloudscraper()

            # Synchronizujemy proxy scrapera z sesją, lub rotujemy jeśli trzeba
            if session.proxies != scraper.proxies:
                scraper.proxies.update(session.proxies)

            response = scraper.get(url, timeout=20)

            if response.status_code in [403, 429]:
                 print(f"[!] Cloudscraper też dostał {response.status_code}. Rotuję proxy scrapera.")
                 rotate_proxy(scraper)
                 response = scraper.get(url, timeout=20)

        if response.status_code != 200:
            print(f"Błąd połączenia: {response.status_code}")
            return None, False

        # --- PARSOWANIE (Reszta bez zmian) ---
        soup = BeautifulSoup(response.content, 'html.parser')
        price = None
        available = True

        # ==================================================================
        # METODA 1: SCHEMA.ORG (JSON-LD)
        # ==================================================================
        scripts = soup.find_all('script', type='application/ld+json')
        for script in scripts:
            try:
                data = json.loads(script.string)

                offers = None
                if isinstance(data, dict):
                    if 'offers' in data:
                        offers = data['offers']
                    elif '@graph' in data:
                        for item in data['@graph']:
                            if 'offers' in item:
                                offers = item['offers']
                                break
                elif isinstance(data, list):
                    for item in data:
                        if 'offers' in item:
                            offers = item['offers']
                            break

                if offers:
                    offer = offers[0] if isinstance(offers, list) else offers
                    if 'price' in offer and not price:
                        price = float(offer['price'])
                    if 'availability' in offer:
                        avail_url = offer['availability']
                        if 'OutOfStock' in avail_url or 'Discontinued' in avail_url or 'SoldOut' in avail_url:
                            available = False
                        else:
                            available = True
            except:
                continue

        # ==================================================================
        # METODA 2: META TAGI (Open Graph)
        # ==================================================================
        if not price:
            meta_price = soup.find("meta", property="product:price:amount")
            if meta_price:
                try: price = float(meta_price.get("content"))
                except: pass

        meta_avail = soup.find("meta", property="product:availability")
        if meta_avail:
            content = meta_avail["content"].lower()
            if "oos" in content or "out of stock" in content or "unavailable" in content:
                available = False

        # ==================================================================
        # METODA 3: MIKRODANE (itemprop)
        # ==================================================================
        if not price:
            microdata_price = soup.find(attrs={"itemprop": "price"})
            if microdata_price:
                candidate_price = microdata_price.get("content") or microdata_price.get_text()
                if candidate_price: price = candidate_price

            microdata_avail = soup.find(attrs={"itemprop": "availability"})
            if microdata_avail:
                avail_value = microdata_avail.get("content") or microdata_avail.get("href")
                if avail_value and ("OutOfStock" in avail_value or "SoldOut" in avail_value or "Discontinued" in avail_value):
                    available = False

        # ==================================================================
        # METODA 4: OBSŁUGA POPULARNYCH PLATFORM E-COMMERCE
        # ==================================================================
        if not price:
            # IDOSELL
            idosell_elem = soup.find(id="projector_price_value")
            if idosell_elem and idosell_elem.has_attr('data-price'):
                price = idosell_elem['data-price']

            # PRESTASHOP
            if not price:
                presta_id = soup.find(id="our_price_display")
                if presta_id: price = presta_id.get_text()
                else:
                    presta_class = soup.find(class_="current-price")
                    if presta_class: price = presta_class.get_text()

            # WOOCOMMERCE
            if not price:
                price_span = soup.find('span', class_='woocommerce-Price-amount')
                if price_span:
                    bdi_tag = price_span.find('bdi')
                    price = bdi_tag.get_text().strip() if bdi_tag else price_span.get_text().strip()

        # ==================================================================
        # Fallback dla znanych sklepów
        # ==================================================================
        if not price:
            if 'morele.net' in url:
                elem = soup.find('div', id='product_price')
                if elem: price = elem.get('data-price')
            elif 'arante.pl' in url:
                elem = soup.find('span', id='st_product_options-price-brutto')
                if elem: price = elem.text.strip()
            elif 'nowaszkola.com' in url:
                div = soup.find('div', class_='price')
                if div and div.find('span'):
                    price = div.find('span').get_text().replace('Cena:', '').replace('PLN', '').strip()
            elif 'rerek.pl' in url:
                elem = soup.find('span', id='st_product_options-price-brutto')
                if elem: price = elem.get_text().replace('zł', '').replace('*', '').strip()

        # ==================================================================
        # Analiza tekstowa dostępności (Fallback)
        # ==================================================================
        if available:
            text_content = soup.get_text().lower()
            keywords_unavailable = ["brak w magazynie", "produkt niedostępny", "wyprzedany", "oczekiwanie na dostawę", "powiadom o dostępności", "produkt chwilowo niedostępny"]
            for kw in keywords_unavailable:
                if kw in text_content:
                    available = False
                    break

        # ==================================================================
        # FINALIZACJA
        # ==================================================================
        if price:
            if isinstance(price, str):
                clean_price = price.lower().replace('zł', '').replace('pln', '').replace(' ', '').replace(',', '.')
                clean_price = re.sub(r'[^\d.]', '', clean_price)
                try: final_price = float(clean_price)
                except ValueError: return None, False
            else:
                final_price = float(price)
            return final_price, available
        else:
            print(f"Nie znaleziono ceny: {url}")
            return None, False

    except Exception as e:
        print(f"Wyjątek scrapera: {e}")
        # W razie błędu krytycznego (np. zerwane połączenie proxy), wymuszamy rotację przy następnym wywołaniu
        try: rotate_proxy(session)
        except: pass
        return None, False