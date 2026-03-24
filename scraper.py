import requests
from bs4 import BeautifulSoup
import time
import random
import json
import re
import os
import urllib.parse
import cloudscraper
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

logger = logging.getLogger(__name__)

# Lista User-Agentów do rotacji
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36 Edg/121.0.0.0'
]

NORD_SERVERS = [
    'nl.socks.nordhold.net:1080', 'se.socks.nordhold.net:1080', 'us.socks.nordhold.net:1080',
    'amsterdam.nl.socks.nordhold.net:1080', 'atlanta.us.socks.nordhold.net:1080',
    'chicago.us.socks.nordhold.net:1080', 'dallas.us.socks.nordhold.net:1080',
    'los-angeles.us.socks.nordhold.net:1080', 'new-york.us.socks.nordhold.net:1080',
    'phoenix.us.socks.nordhold.net:1080', 'san-francisco.us.socks.nordhold.net:1080',
    'stockholm.se.socks.nordhold.net:1080'
]


def init_batch_session():
    """Tworzy sesję na start paczki skanowania. Maksymalnie 3 próby połączenia z proxy."""
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    # Ustawiamy nagłówki
    new_agent = random.choice(USER_AGENTS)
    session.headers.update({
        'User-Agent': new_agent,
        'Accept-Language': 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Referer': 'https://www.google.com/',
        'DNT': '1'
    })

    nord_user = os.environ.get('NORD_USER')
    nord_pass = os.environ.get('NORD_PASS')

    if not nord_user or not nord_pass:
        logger.warning("[PROXY] Brak danych NordVPN - tryb bezpośredni (bez proxy).")
        return session

    safe_nord_pass = urllib.parse.quote(nord_pass)

    logger.info("--- INICJALIZACJA PROXY DLA PACZKI ZADAŃ ---")
    for attempt in range(1, 4):
        current_server = random.choice(NORD_SERVERS)
        proxy_url = f"socks5h://{nord_user}:{safe_nord_pass}@{current_server}"
        session.proxies = {"http": proxy_url, "https": proxy_url}

        logger.info(f"[PROXY] Próba {attempt}/3 - Łączenie z {current_server}...")

        try:
            # Testujemy połączenie
            resp = session.get("http://api.ipify.org", timeout=10)
            if resp.status_code == 200:
                logger.info(f"[PROXY] Sukces! Połączono. IP widoczne w sieci: {resp.text}")
                return session  # Zwracamy sprawną sesję
        except requests.exceptions.RequestException as e:
            print(f"[PROXY] Błąd na próbie {attempt}: {e}")
            time.sleep(2)  # Czekamy chwilę przed kolejną próbą

    # Jeśli pętla dojdzie tutaj, znaczy że 3 próby zawiodły
    print("[!] Wszystkie 3 próby proxy zawiodły. Skanowanie odbędzie się z Twojego prawdziwego IP.")
    session.proxies = {}
    return session


def close_batch_session(session):
    """Zamyka i czyści sesję."""
    if session:
        session.proxies = {}
        session.close()
        logger.info("--- SESJA I PROXY ZAKOŃCZONE ---")


def get_current_price(url, session):
    logger.info(f"[SCAN] START: {url}")
    try:
        time.sleep(random.uniform(0.5, 1.5))

        try:
            response = session.get(url, timeout=15)
            logger.info(f"[SCAN] STATUS: {response.status_code} | {url}")
        except requests.exceptions.RequestException as e:
            logger.error(f"[SCAN] BŁĄD POŁĄCZENIA: {e} | {url}")
            return None, False
        # Jeśli błędy 400-500 (np. ban 403), używamy Cloudscrapera, zachowując TO SAMO PROXY
        if response.status_code in [404, 410]:
            logger.error(f"[!] Produkt nie istnieje (Błąd {response.status_code}): {url}")
            return None, False
        if response.status_code in [401, 403, 429, 503]:
            logger.warning(f"[!] Sklep zablokował dostęp (Błąd {response.status_code}). Odpalam Cloudscraper...")

            scraper = cloudscraper.create_scraper(
                browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
            )
            scraper.headers.update(session.headers)
            scraper.proxies.update(session.proxies)  # Przepisujemy to samo proxy do scrapera

            try:
                response = scraper.get(url, timeout=20)
            except requests.exceptions.RequestException as e:
                logger.error(f"[!] Cloudscraper też wyrzucił błąd: {e}")
                return None, False

        if response.status_code != 200:
            logger.error(f"Ostateczny błąd HTTP: {response.status_code} dla {url}")
            return None, False

        if response.status_code == 200:
            if 'arante.pl' in url:
                logger.info(
                    f"[ARANTE] Status 200, długość HTML: {len(response.content)}, tytuł: {BeautifulSoup(response.content, 'html.parser').title}")

        # Sprawdź czy mimo 200 dostaliśmy Cloudflare challenge
        if response.status_code == 200:
            if 'cloudflare' in response.text.lower() and 'challenge' in response.text.lower():
                logger.warning(f"[!] Cloudflare challenge wykryty dla {url}, przełączam na Cloudscraper...")
                scraper = cloudscraper.create_scraper(
                    browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
                )
                scraper.headers.update(session.headers)
                scraper.proxies.update(session.proxies)
                time.sleep(2)
                try:
                    response = scraper.get(url, timeout=20)
                except requests.exceptions.RequestException as e:
                    logger.error(f"[!] Cloudscraper błąd: {e}")
                    return None, False

        # --- PARSOWANIE ---
        soup = BeautifulSoup(response.content, 'html.parser')
        price = None
        available = True

        # SCHEMA.ORG (JSON-LD)
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

        # META TAGI
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
            if "oos" in content or "out of stock" in content or "unavailable" in content: available = False

        # MIKRODANE
        if not price:
            microdata_price = soup.find(attrs={"itemprop": "price"})
            if microdata_price:
                candidate_price = microdata_price.get("content") or microdata_price.get_text()
                if candidate_price: price = candidate_price

            microdata_avail = soup.find(attrs={"itemprop": "availability"})
            if microdata_avail:
                avail_value = microdata_avail.get("content") or microdata_avail.get("href")
                if avail_value and (
                        "OutOfStock" in avail_value or "SoldOut" in avail_value or "Discontinued" in avail_value): available = False

        # PLATFORMY
        if not price:
            idosell_elem = soup.find(id="projector_price_value")
            if idosell_elem and idosell_elem.has_attr('data-price'): price = idosell_elem['data-price']
            if not price:
                presta_id = soup.find(id="our_price_display")
                if presta_id:
                    price = presta_id.get_text()
                else:
                    presta_class = soup.find(class_="current-price")
                    if presta_class: price = presta_class.get_text()
            if not price:
                price_span = soup.find('span', class_='woocommerce-Price-amount')
                if price_span:
                    bdi_tag = price_span.find('bdi')
                    price = bdi_tag.get_text().strip() if bdi_tag else price_span.get_text().strip()

        # FALLBACK SKLEPÓW
        if not price:
            if 'morele.net' in url:
                elem = soup.find('div', id='product_price')
                if elem: price = elem.get('data-price')
            elif 'arante.pl' in url:
                elem = soup.find('span', id='st_product_options-price-brutto')
                if elem: price = elem.text.strip()
            elif 'nowaszkola.com' in url:
                div = soup.find('div', class_='price')
                if div and div.find('span'): price = div.find('span').get_text().replace('Cena:', '').replace('PLN',
                                                                                                              '').strip()
            elif 'rerek.pl' in url:
                elem = soup.find('span', id='st_product_options-price-brutto')
                if elem: price = elem.get_text().replace('zł', '').replace('*', '').strip()
            elif 'edumax.com.pl' in url:
                    edumax_elem = soup.find('strong', id='projector_price_value')
                    if edumax_elem:
                        raw_price = edumax_elem.get('data-price')
                        if not raw_price or raw_price.strip() == '':
                            raw_price = edumax_elem.text.strip()
                        if raw_price:
                            price = raw_price
            elif 'rehazakupy.pl' in url:
                elems = soup.find_all('span', attrs={'data-type': 'product-price'})
                for elem in elems:
                    classes = elem.get('class', [])
                    if 'hide' not in classes:
                        price = elem.get_text().strip()
                        break
            elif 'phuimpuls.pl' in url:
                elem = soup.find('strong', id='priceValue')
                if elem:
                    price = elem.get_text().strip()
            elif 'edukacyjna.pl' in url:
                elem = soup.find('span', class_='current-price-value')
                if elem:
                    price = elem.get('content') or elem.get_text().strip()
            elif 'czytam.pl' in url:
                elem = soup.find('div', class_='product-single-price')
                if elem:
                    price = elem.get_text().strip()
            elif 'medicon.pl' in url:
                low = soup.find('span', itemprop='lowprice')
                if low:
                    price = low.get_text().strip()
            elif 'lumen.pl' in url:
                elem = soup.find('span', class_='price_view_span')
                if elem:
                    part1 = elem.find('span', class_='price_1_pinfo')
                    part2 = elem.find('span', class_='price_2_pinfo')
                    if part1 and part2:
                        price = part1.get_text().replace(',', '') + '.' + part2.get_text()
            elif 'empik.com' in url:
                section = soup.find('section', attrs={'data-product-price': True})
                if section:
                    price = section.get('data-product-price')
            elif 'kaufland.pl' in url:
                elem = soup.find('span', attrs={'data-test': 'product-price'})
                if elem:
                    aria = elem.get('aria-label', '')
                    if aria:
                        price = aria.replace('Cena:', '').strip()
                    else:
                        price = elem.get_text().strip()
            elif 'zegarki-diament.pl' in url:
                elem = soup.find('div', class_='price')
                if elem and elem.find('span'):
                    price = elem.find('span').get_text().strip()
            elif 'ksiazki-medyczne.eu' in url:
                elem = soup.find('span', id='st_product_options-price-brutto')
                if elem:
                    price = elem.get_text().replace('zł', '').strip()
            elif 'pomocedydaktyczne.eu' in url:
                price_element = soup.find('span', class_='brutto')
                if price_element:
                    raw_price = price_element.text.strip()
                    if raw_price: price = raw_price
            elif 'akademia-umyslu.pl' in url:
                price_element = soup.find('span', id='price_mob_span')
                if price_element:
                    raw_price = price_element.text.strip()
                    if raw_price: price = raw_price
            elif 'atabi.pl' in url:
                elem = soup.find('div', class_='meta-price')
                if elem and elem.find('span'):
                    price = elem.find('span').text.strip()


        # TEKST
        if available:
            text_content = soup.get_text().lower()
            keywords_unavailable = ["brak w magazynie", "produkt niedostępny", "wyprzedany", "oczekiwanie na dostawę",
                                    "powiadom o dostępności", "produkt chwilowo niedostępny"]
            for kw in keywords_unavailable:
                if kw in text_content:
                    available = False
                    break

                # WYNIK
        if price:
            if isinstance(price, str):
                 # Usuwamy waluty i twarde spacje (\xa0)
                clean_price = price.lower().replace('zł', '').replace('pln', '').replace(' ', '').replace(
                            '\xa0', '')

                # Jeśli w cenie jest przecinek dziesiętny, kropka to na 99% separator tysięcy
                if ',' in clean_price:
                    clean_price = clean_price.replace('.', '')  # Usuwamy kropkę tysięcy (10.599,00 -> 10599,00)
                    clean_price = clean_price.replace(',',
                                                              '.')  # Zamieniamy przecinek na kropkę (10599,00 -> 10599.00)

                clean_price = re.sub(r'[^\d.]', '', clean_price)

                try:
                    final_price = float(clean_price)
                except ValueError:
                    return None, False
            else:
                final_price = float(price)
            logger.info(f"[SCAN] CENA: {final_price} PLN | {url}")
            return final_price, available
        else:
            logger.warning(f"[SCAN] BRAK CENY | {url}")
            return None, False
    except Exception as e:
        logger.error(f"[SCAN] WYJĄTEK: {e} | {url}")
        return None, False