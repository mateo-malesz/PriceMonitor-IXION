import logging
from zeep import Client
from zeep.exceptions import Fault
from zeep.helpers import serialize_object
from datetime import datetime, timedelta, date

logger = logging.getLogger(__name__)


def fetch_sales_for_date(api_url, username, password, target_date):
    """
    Łączy się z API SOTE i pobiera zagregowaną sprzedaż dla konkretnej daty (target_date).
    Zwraca słownik: {'SKU-123': {'qty': 5, 'revenue': 150.0}, ...}
    """
    if not api_url or not username or not password:
        logger.error("[SOTE API] Brak pełnych danych logowania.")
        return {}

    base_url = api_url.rstrip('/')
    wsdl_login = f"{base_url}/webapi/soap?wsdl"
    wsdl_order = f"{base_url}/order/soap?wsdl"
    culture = "pl"

    try:
        client_login = Client(wsdl_login)
        login_type = client_login.get_type("ns0:doLogin")
        login_request = login_type(_culture=culture, username=username, password=password)
        session_hash = client_login.service.doLogin(login_request)
    except Exception as e:
        logger.error(f"[SOTE API] Błąd logowania do {base_url}: {e}")
        return {}

    # Konwertujemy docelową datę na string np. '2026-04-10'
    target_date_str = target_date.strftime('%Y-%m-%d')

    # Okno szukania modyfikacji: od dnia szukanego do "teraz"
    # (żeby złapać opóźnione płatności)
    date_from = target_date.strftime('%Y-%m-%dT00:00:00+01:00')
    date_to = datetime.now().strftime('%Y-%m-%dT23:59:59+01:00')

    client_order = Client(wsdl_order)
    order_list_type = client_order.get_type("ns0:GetOrderList")

    offset = 0
    limit = 50
    valid_order_ids = []

    # 1. Pobieranie nagłówków zamówień
    while True:
        try:
            req_data = order_list_type(
                _session_hash=session_hash,
                _offset=offset,
                _limit=limit,
                _modified_from=date_from,
                _modified_to=date_to
            )
            response = client_order.service.GetOrderList(req_data)

            if not response:
                break

            orders = serialize_object(response)

            for order in orders:
                created_at_raw = order.get('created_at')
                # Łapiemy tylko to, co zostało ZŁOŻONE w naszym docelowym dniu
                if created_at_raw and str(created_at_raw).startswith(target_date_str):
                    valid_order_ids.append(order['id'])

            if len(orders) < limit:
                break
            offset += limit

        except Fault as fault:
            logger.error(f"[SOTE API] Błąd GetOrderList: {fault}")
            break

    if not valid_order_ids:
        logger.info(f"[SOTE API] Brak zamówień dla daty {target_date_str}.")
        return {}

    # 2. Pobieranie szczegółów zamówień
    sales_data = {}
    product_list_type = client_order.get_type("ns0:GetOrderProductList")

    for order_id in valid_order_ids:
        try:
            req_prod = product_list_type(_session_hash=session_hash, order_id=order_id)
            resp_prod = client_order.service.GetOrderProductList(req_prod)

            if resp_prod:
                products = serialize_object(resp_prod)
                if not isinstance(products, list):
                    products = [products]

                for prod in products:
                    if not isinstance(prod, dict): continue

                    sku = prod.get('sku') or prod.get('code')
                    if not sku: continue

                    sku_upper = str(sku).strip().upper()
                    qty = float(prod.get('quantity') or 0)
                    price = float(prod.get('price_brutto') or 0)

                    if sku_upper not in sales_data:
                        sales_data[sku_upper] = {'qty': 0, 'revenue': 0.0}

                    sales_data[sku_upper]['qty'] += qty
                    sales_data[sku_upper]['revenue'] += (qty * price)

        except Exception as e:
            continue

    logger.info(f"[SOTE API] {target_date_str}: Pobrano {len(sales_data)} SKU.")
    return sales_data