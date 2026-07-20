import requests
from datetime import datetime, timezone
from app.utils.logger import logger
from app.service.database import load_data
from app.service.secrets import meli_secrets


API_BASE = "https://api.mercadolibre.com"

def format_mysql_timestamp(value):
    if not value:
        return None
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def get_meli_catalog_ids(token):
    logger.info("Getting meli catalog ID's")

    headers = {"Authorization": f"Bearer {token}"}

    user_res = requests.get(f"{API_BASE}/users/me", headers=headers)
    user_res.raise_for_status()

    user_id = user_res.json().get("id")
    if not user_id:
        raise Exception("Token inválido o expirado")

    item_ids = []
    offset = 0
    limit = 50

    while True:
        res = requests.get(
            f"{API_BASE}/users/{user_id}/items/search",
            headers=headers,
            params={
                "offset": offset,
                "limit": limit
            }
        )
        res.raise_for_status()

        data = res.json()
        results = data.get("results", [])

        logger.info(
            f"Items search page offset={offset}, "
            f"total={data.get('paging', {}).get('total')}, "
            f"results={len(results)}"
        )

        if not results:
            break

        # Ya no omitimos meli_ids existentes.
        item_ids.extend(results)

        offset += limit

        if offset >= data.get("paging", {}).get("total", 0):
            break

    # Evitamos duplicados por seguridad.
    item_ids = list(set(item_ids))

    logger.info(f"Total item_ids to inspect: {len(item_ids)}")

    rows = []

    for i in range(0, len(item_ids), 20):
        batch_ids = item_ids[i:i + 20]

        items_res = requests.get(
            f"{API_BASE}/items",
            headers=headers,
            params={
                "ids": ",".join(batch_ids),
                "attributes": "id,catalog_product_id,date_created,status"
            }
        )
        items_res.raise_for_status()

        items = items_res.json()

        for item in items:
            body = item.get("body", {})
            catalog_product_id = body.get("catalog_product_id")

            if not catalog_product_id:
                continue

            rows.append({
                "meli_id": body.get("id"),
                "catalog_product_id": catalog_product_id,
                "created_at": format_mysql_timestamp(body.get("date_created"))            
            })

    logger.info(f"Total rows with catalog_product_id to load/upsert: {len(rows)}")

    return rows


def update_meli_catalog():
    token = meli_secrets()
    data = get_meli_catalog_ids(token)

    logger.info(f"Rows to load/upsert in catalog listing table: {len(data)}")

    if data:
        fields = "meli_id, catalog_product_id, created_at"
        result = load_data(fields, data, stage=None)