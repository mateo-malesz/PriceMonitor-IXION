# PriceMonitor - System Monitorowania Cen Konkurencji

Aplikacja webowa służąca do automatycznego monitorowania cen produktów w sklepach internetowych konkurencji. System pozwala na zarządzanie projektami, importowanie produktów, wizualizację historii cen na wykresach oraz generowanie raportów.

## 🛠 Technologie
* **Backend:** Python 3.8+, Flask, SQLAlchemy
* **Baza danych:** SQLite
* **Frontend:** HTML5, CSS3, Bootstrap 5, Chart.js, Jinja2
* **Inne:** BeautifulSoup4 (Scraping), APScheduler (Harmonogram zadań)

## 🚀 Instalacja i Uruchomienie

### 1. Wymagania wstępne
Upewnij się, że masz zainstalowany Python w wersji 3.8 lub nowszej.

### 2. Instalacja zależności
Zalecane jest użycie wirtualnego środowiska. W katalogu głównym projektu uruchom terminal:

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Instalacja bibliotek
pip install -r requirements.txt
```

### 3. Konfiguracja (.env)
Znajdź plik .env.example w folderze głównym (jeśli istnieje) lub utwórz nowy plik o nazwie .env.

Wpisz w nim niezbędne dane (np. login/hasło do maila, SECRET_KEY).

### 4. Uruchomienie aplikacji
Aby uruchomić serwer, wpisz w terminalu:

```bash
python app.py
Aplikacja uruchomi się pod adresem: http://127.0.0.1:5000
```

## 🔑 Dostęp Administratora (Demo)
Baza danych dołączona do projektu zawiera gotowe konto administratora:
* Email: admin@test.pl
* Hasło: tajne

## 🌟 Główne Funkcje
* Dashboard: Podgląd statystyk, liczby skanów i błędów w dniu dzisiejszym.
* Scraper: Moduł pobierania cen z obsługą rotacji User-Agent i platform (PrestaShop, WooCommerce, IdoSell).
* Wykresy: Wizualizacja zmian cen i dostępności w czasie (Chart.js).
* Harmonogram: Automatyczne skanowanie produktów o zadanej godzinie.
* Bezpieczeństwo: Autoryzacja dostępu, hashowanie haseł.
