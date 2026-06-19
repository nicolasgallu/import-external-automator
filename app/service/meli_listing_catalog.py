import requests
from datetime import datetime, timezone
from app.utils.logger import logger
from app.service.database import get_method, load_data
from app.service.secrets import meli_secrets


API_BASE = "https://api.mercadolibre.com"
token = meli_secrets()
schema_mercadolibre = "mercadolibre"
query = {
        'q_columns': [
            'meli_id'
        ],
        'q_from':f'FROM {schema_mercadolibre}.catalog_listing',
    }

def format_mysql_timestamp(value):
    if not value:
        return None

    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def get_meli_catalog_ids(token, meli_ids_omitidos=None):
    logger.info("Getting meli catalog ID's")
    
    meli_ids_omitidos = set(meli_ids_omitidos or [])
    headers = {"Authorization": f"Bearer {token}"}

    user_id = requests.get(f"{API_BASE}/users/me", headers=headers).json().get("id")
    if not user_id:
        raise Exception("Token inválido o expirado")

    item_ids, offset, limit = [], 0, 50

    while True:
        res = requests.get(
            f"{API_BASE}/users/{user_id}/items/search",
            headers=headers,
            params={"offset": offset, "limit": limit}
        ).json()

        results = res.get("results", [])
        if not results:
            break

        item_ids += [x for x in results if x not in meli_ids_omitidos]

        offset += limit
        if offset >= res.get("paging", {}).get("total", 0):
            break

    rows = []

    for i in range(0, len(item_ids), 20):
        items = requests.get(
            f"{API_BASE}/items",
            headers=headers,
            params={
                "ids": ",".join(item_ids[i:i + 20]),
                "attributes": "id,catalog_product_id,date_created"
            }
        ).json()

        for item in items:
            body = item.get("body", {})
            catalog_product_id = body.get("catalog_product_id")

            if catalog_product_id:
                rows.append({
                    "meli_id": body.get("id"),
                    "catalog_product_id": catalog_product_id,
                    "created_at": format_mysql_timestamp(body.get("date_created"))
                })
    return rows

def update_meli_catalog():
    current_list = [i.get('meli_id') for i in get_method(query)]
    data = get_meli_catalog_ids(token, meli_ids_omitidos=current_list)
    logger.info(f"New rows to add in catalog listing table: {len(data)}")
    if data != []:
        fields = 'meli_id, catalog_product_id, created_at'    
        load_data(fields, data, stage=None)