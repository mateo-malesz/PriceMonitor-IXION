import requests
from bs4 import BeautifulSoup
import time
import random
import json
import re
import os

# Lista User-Agentów do rotacji
USER_AGENTS = [
    # Chrome Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    # Chrome Mac
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    # Firefox Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    # Safari Mac
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    # Edge Windows
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0'
]

def check_my_ip(proxies=None):
    """Zwraca publiczne IP, z proxy lub bez."""
    try:
        # Zmiana 1: Używamy zwykłego http zamiast https dla testu (omija błędy handshake SSL proxy)
        # Zmiana 2: Dodajemy chociaż podstawowy nagłówek, żeby nie wyglądać jak automat
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get('http://api.ipify.org', headers=headers, proxies=proxies, timeout=10)
        return response.text
    except Exception as e:
        return f"Błąd ({e})"

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

def get_current_price(url):
    current_user_agent = random.choice(USER_AGENTS)

    headers = {
        'User-Agent': current_user_agent,
        'Accept-Language': 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Referer': 'https://www.google.com/',
        'DNT': '1'
    }

    # --- TEST IP: PRZED PROXY ---
    real_ip = check_my_ip(proxies=None)
    print(f"\n[TEST] 1. Prawdziwe IP serwera: {real_ip}")

    # NOWOŚĆ: Konfiguracja Proxy
    proxies = None
    nord_user = os.environ.get('NORD_USER')
    nord_pass = os.environ.get('NORD_PASS')

    if nord_user and nord_pass:
        current_server = random.choice(NORD_SERVERS)
        # UWAGA: Zmieniamy z http:// na socks5h://
        proxy_url = f"socks5h://{nord_user}:{nord_pass}@{current_server}"
        proxies = {
            "http": proxy_url,
            "https": proxy_url
        }

        # --- TEST IP: PO PROXY ---
        proxy_ip = check_my_ip(proxies=proxies)
        print(f"[TEST] 2. Skanuje przez serwer: {current_server}")
        print(f"[TEST] 3. Sklep widzi IP: {proxy_ip}")
    else:
        print("[TEST] Brak danych NordVPN w .env. Lecę na czysto.")

    try:
        time.sleep(random.uniform(0.5, 1.5))
        response = requests.get(url, headers=headers, proxies=proxies, timeout=15)

        if response.status_code != 200:
            print(f"Błąd połączenia: {response.status_code}")
            return None, False

        soup = BeautifulSoup(response.content, 'html.parser')

        price = None
        available = True  # Domyślnie zakładamy, że jest, chyba że znajdziemy dowód że nie ma

        # ==================================================================
        # METODA 1: SCHEMA.ORG (JSON-LD) - NAJLEPSZA
        # Szukamy ukrytego JSON-a, którego czyta Google. Tam dane są najczystsze.
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

                    # 1. CENA Z SCHEMA
                    if 'price' in offer and not price:
                        price = float(offer['price'])

                    # 2. DOSTĘPNOŚĆ Z SCHEMA
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
                try:
                    price = float(meta_price.get("content"))
                except:
                    pass

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
                candidate_price = microdata_price.get("content")
                if not candidate_price:
                    candidate_price = microdata_price.get_text()

                if candidate_price:
                    price = candidate_price

            microdata_avail = soup.find(attrs={"itemprop": "availability"})
            if microdata_avail:
                avail_value = microdata_avail.get("content") or microdata_avail.get("href")
                if avail_value and (
                        "OutOfStock" in avail_value or "SoldOut" in avail_value or "Discontinued" in avail_value):
                    available = False

        # ==================================================================
        # METODA 4: OBSŁUGA POPULARNYCH PLATFORM E-COMMERCE
        # ==================================================================
        if not price:

            # IDOSELL (IAI SHOP)
            idosell_elem = soup.find(id="projector_price_value")
            if idosell_elem and idosell_elem.has_attr('data-price'):
                price = idosell_elem['data-price']

            # PRESTASHOP
            if not price:
                presta_id = soup.find(id="our_price_display")
                if presta_id:
                    price = presta_id.get_text()
                else:
                    presta_class = soup.find(class_="current-price")
                    if presta_class:
                        price = presta_class.get_text()

            # WOOCOMMERCE
            if not price:
                price_span = soup.find('span', class_='woocommerce-Price-amount')
                if price_span:
                    bdi_tag = price_span.find('bdi')
                    if bdi_tag:
                        price = bdi_tag.get_text().strip()
                    else:
                        price = price_span.get_text().strip()

        # ==================================================================
        # Fallback dla znanych sklepów
        # ==================================================================
        if not price:
            # MORELE.NET
            if 'morele.net' in url:
                price_element = soup.find('div', id='product_price')
                if price_element:
                    raw_price = price_element.get('data-price')
                    if raw_price: price = raw_price

            # ARANTE.PL
            elif 'arante.pl' in url:
                price_element = soup.find('span', id='st_product_options-price-brutto')
                if price_element:
                    raw_price = price_element.text.strip()
                    if raw_price: price = raw_price

            # NOWASZKOLA.COM
            elif 'nowaszkola.com' in url:
                price_div = soup.find('div', class_='price')
                if price_div:
                    price_span = price_div.find('span')
                    if price_span:
                        raw_price = price_span.get_text().replace('Cena:', '').replace('PLN', '').strip()
                        if raw_price: price = raw_price


        # ==================================================================
        # Analiza tekstowa dostępności (Fallback)
        # ==================================================================
        if available:
            text_content = soup.get_text().lower()
            keywords_unavailable = [
                "brak w magazynie",
                "produkt niedostępny",
                "wyprzedany",
                "oczekiwanie na dostawę",
                "powiadom o dostępności",
                "produkt chwilowo niedostępny"
            ]

            for kw in keywords_unavailable:
                if kw in text_content:
                    available = False
                    break

        # ==================================================================
        # FINALIZACJA I CZYSZCZENIE DANYCH
        # ==================================================================
        if price:
            if isinstance(price, str):
                clean_price = price.lower().replace('zł', '').replace('pln', '').replace(' ', '').replace(',', '.')
                clean_price = re.sub(r'[^\d.]', '', clean_price)
                try:
                    final_price = float(clean_price)
                except ValueError:
                    return None, False
            else:
                final_price = float(price)

            return final_price, available
        else:
            print(f"Nie znaleziono ceny: {url}")
            return None, False

    except Exception as e:
        print(f"Wyjątek scrapera: {e}")
        return None, False