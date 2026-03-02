# Changelog

Wszystkie znaczące zmiany w projekcie będą dokumentowane w tym pliku.

## [1.0.0] - 2026-02-03 (Wersja Finalna)
### Dodano
- Prosta identyfikacja wizualna PriceMonitor
- Pełna obsługa harmonogramu zadań (APScheduler) z poziomu interfejsu użytkownika.
- Zaawansowane wykresy cen i dostępności (Chart.js) z obsługą osi czasu.
- Rotacja `User-Agent` w module scrapera dla zwiększenia anonimowości.
- Dedykowana obsługa platform e-commerce: PrestaShop, WooCommerce, IdoSell.
- Zabezpieczenie zmiennych konfiguracyjnych poprzez plik `.env`.

### Zmieniono
- Zoptymalizowano algorytm wyszukiwania cen (priorytetyzacja JSON-LD).
- Poprawiono interfejs użytkownika (responsywność tabel i wykresów).
- Zaktualizowano strukturę bazy danych (dodanie tabeli `ScheduledTask`).

## [0.9.0] - 2026-01-20 (Beta)
### Dodano
- Podstawowy mechanizm logowania i rejestracji użytkowników.
- Panel zarządzania projektami (CRUD).
- Ręczne odświeżanie cen z poziomu listy produktów.
- Import produktów z plików XML (Google Shopping feed).

## [0.1.0] - 2025-11-15 (Alpha)
### Dodano
- Inicjalizacja projektu Flask.
- Konfiguracja bazy danych SQLite i SQLAlchemy.
- Prosty scraper pobierający tytuł strony.