import asyncio
import aiohttp
import json
from datetime import datetime

from app.service.database import get_method, update_method, run_procedure
from app.service.secrets import meli_secrets
from app.utils.logger import logger
from app.settings.config import SCHEMA_INVENTORY, PRODUCTS_TABLE


async def fetch_performance(session, semaphore, item_id, access_token):

    async with semaphore:

        url = f"https://api.mercadolibre.com/item/{item_id}/performance"

        async with session.get(
            url,
            headers={"Authorization": f"Bearer {access_token}"}
        ) as response:

            if response.status != 200:
                logger.error(f"{item_id}: {response.reason}")

            data = await response.json()

            calculated_at = data.get("calculated_at")

            if "." in str(calculated_at):
                calculated_at = datetime.strptime(
                    calculated_at,
                    "%Y-%m-%dT%H:%M:%S.%fZ"
                )
            elif calculated_at is not None:
                calculated_at = datetime.strptime(
                    calculated_at,
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            else:
                calculated_at = datetime.now()

            return {
                "meli_id": {"value": item_id, "type": "char(50)"},
                "entity_type": {"value": data.get("entity_type",'None'), "type": "char(50)"},
                "score": {"value": data.get("score",0), "type": "int signed"},
                "level": {"value": data.get("level",'None'), "type": "char(50)"},
                "level_wording": {"value": data.get("level_wording",'None'), "type": "char(50)"},
                "buckets": {"value": json.dumps(data.get("buckets", data)), "type": "json"},
                "calculated_at": {"value": calculated_at, "type": "datetime"},
                "updated_at": {"value": datetime.now(), "type": "datetime"},
            }


async def get_performance():

    access_token = meli_secrets()

    query = {
        "q_columns": [
            "meli_id",
        ],
        "q_from": f"FROM {SCHEMA_INVENTORY}.{PRODUCTS_TABLE}",
        "q_where": "WHERE status = 'active' and meli_id is not null",
    }

    active_items = [i["meli_id"] for i in get_method(query)]

    logger.info(f"Getting performance of {len(active_items)} active items in Meli")

    semaphore = asyncio.Semaphore(20)
    async with aiohttp.ClientSession() as session:

        tasks = [
            fetch_performance(
                session,
                semaphore,
                item_id,
                access_token,
            )
            for item_id in active_items
        ]

        results = await asyncio.gather(*tasks)

    items = [item for item in results if item is not None]

    if items:
        update_method(items, "mercadolibre", "performance_raw")
        run_procedure("mercadolibre", "refresh_performance_data")

def run_get_performance():
    asyncio.run(get_performance())