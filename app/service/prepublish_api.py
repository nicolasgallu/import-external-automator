from app.service.database import get_method
from app.settings.config import SCHEMA_INVENTORY, PRODUCTS_TABLE, WEBHOOK_PUBLICATIONS, SECRET
import time
import requests
from app.utils.logger import logger


def prepublish_call_ai():
    query = {
        'q_columns': [
            'id',
        ],
        'q_from':f'FROM {SCHEMA_INVENTORY}.{PRODUCTS_TABLE}',
        'q_where': f'WHERE meli_id is null and stock > 0 and (product_name_meli is null or description is null)',
    }

    item_ids = [i.get('id') for i in get_method(query)]
    logger.info(f"Calling Prepublish for {len(item_ids)} items.")

    for i,id in enumerate(item_ids):
        pre_publish= {
            "event_type":"pre-publish", 
            "item_id": id,
            "secret": SECRET
        }
        if i %10 == 0:
            time.sleep(15)

        requests.post(url=WEBHOOK_PUBLICATIONS, json=pre_publish)


