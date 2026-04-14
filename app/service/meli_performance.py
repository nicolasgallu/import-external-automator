import requests
from app.service.database import get_item_actives, load_item_performance
from app.service.secrets import meli_secrets
from app.utils.logger import logger
from datetime import datetime
import json

def get_performance():
    """
    return performance over items, just works for current active published items
    """
    access_token = meli_secrets()
    headers = {'Authorization': f'Bearer {access_token}'}
    published_items = get_item_actives()

    items = []

    headers = {'Authorization': f'Bearer {access_token}'}
    for item_id in published_items:
        url = f"https://api.mercadolibre.com/item/{item_id}/performance"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            clean = {
            "entity_type": data.get("entity_type"),
            "meli_id": data.get("entity_id"),
            "score": data.get("score"),
            "level": data.get("level"),
            "level_wording": data.get("level_wording"),
            "calculated_at": datetime.strptime(str(data.get("calculated_at")),"%Y-%m-%dT%H:%M:%S.%fZ"),
            "updated_at": datetime.now(),
            "buckets": json.dumps(data.get("buckets")) 
            }
            items.append(clean)
        else:
            logger.info("Error calling performance API")
            logger.error(response.reason)
    if items:
        load_item_performance(items)
    else:
        None
