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
        'q_where': f"WHERE meli_id is null and stock > 0 and (product_name_meli IS NULL OR product_name_meli = '' OR description IS NULL OR description = '' OR brand IS NULL OR brand = '' OR model IS NULL OR model = '')",
    }

    item_ids = [i.get('id') for i in get_method(query)]
    logger.info(f"Calling Prepublish for {len(item_ids)} items.")

    for id in item_ids:
        logger.info(f'requesting prepublish for: {id}')
        pre_publish= {"event_type":"pre-publish", "item_id": id,"secret": SECRET}
        requests.post(url=WEBHOOK_PUBLICATIONS, json=pre_publish)
        time.sleep(8)