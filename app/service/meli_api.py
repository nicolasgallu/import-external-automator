import asyncio
import aiohttp
import json
from datetime import datetime
from app.utils.logger import logger
from app.service.secrets import meli_secrets
from app.service.database import get_method, update_method, run_procedure
from app.settings.config import SCHEMA_INVENTORY, PRODUCTS_TABLE


def variation_metadata(variation):
    meta = {}
    for attr in variation.get("attribute_combinations", []):
        meta[attr["id"]] = attr.get("value_name")
    for attr in variation.get("attributes", []):
        meta.setdefault(attr["id"], attr.get("value_name"))
    return meta


def product_status_sync():
    """
    Retorna el estado completo de los items publicados:
    meli_id, stock, status, reason, remedy y updated_at.
    """
    logger.info("Starting Product Status Sync Process..")
    token = meli_secrets()
    headers = {"Authorization": f"Bearer {token}"}

    async def fetch_json(session, url, params=None):
        async with session.get(url, params=params) as resp:
            if resp.status == 200:
                return await resp.json(), resp.status
            return None, resp.status

    async def main():
        try:
            timeout = aiohttp.ClientTimeout(total=60)
            connector = aiohttp.TCPConnector(limit_per_host=10)
            semaphore = asyncio.Semaphore(20)

            final_results = []

            async with aiohttp.ClientSession(
                headers=headers,
                timeout=timeout,
                connector=connector
            ) as session:


                query = {
                    'q_columns': [
                        'a.meli_id',
                    ],
                    'q_from':f'FROM {SCHEMA_INVENTORY}.{PRODUCTS_TABLE} as a',
                    'q_where': f'WHERE a.meli_id is not null',
                }

                item_ids = [i.get('meli_id') for i in get_method(query)]
                logger.info(f"Products Published in Mercadolibre: {len(item_ids)}")
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # ==========================================================
                # 3. MULTIGET ITEMS
                # ==========================================================

                for i in range(0, len(item_ids), 20):

                    chunk = item_ids[i:i + 20]
                    logger.info(f"Processing Chunk: {chunk}")

                    async with semaphore:
                        items_data, _ = await fetch_json(
                            session,
                            "https://api.mercadolibre.com/items",
                            params={
                                "ids": ",".join(chunk)
                            }
                        )

                    if not items_data:
                        continue

                    for item_info in items_data:

                        body = item_info.get("body", {})
                        item_id = body.get("id")
                        status = body.get("status")
                        reason = "None"
                        remedy = "None"
                        variations = body.get("variations", [])
                        variants_data=json.dumps('None')
                        if variations:
                            total_stock = sum(v.get("available_quantity", 0) for v in variations)
                            variants_data ={
                                    "product_stock": total_stock,
                                    "variants": []}
                            
                            for variation in variations:
                                variants_data["variants"].append({
                                    "variant_id": variation["id"],
                                    "stock": variation.get("available_quantity"),
                                    "price": variation.get("price"),
                                    "metadata": variation_metadata(variation)
                                })

                            variants_data = json.dumps(variants_data)

                        # ==================================================
                        # 4. MODERATION
                        # ==================================================

                        if status != "active" and item_id:

                            async with semaphore:
                                response_mod, status_code = await fetch_json(
                                    session,
                                    f"https://api.mercadolibre.com/moderations/last_moderation/{item_id}-ITM"
                                )

                            if status_code == 200 and response_mod:

                                if isinstance(response_mod, list):
                                    wordings = response_mod[0].get("wordings", [])
                                else:
                                    wordings = []

                                if len(wordings) > 0:
                                    reason = wordings[0].get(
                                        "value",
                                        "No reason provided"
                                    )

                                if len(wordings) > 1:
                                    remedy = wordings[1].get(
                                        "value",
                                        "No remedy provided"
                                    )

                        final_results.append({
                            "meli_id": {"value":item_id, "type":"char(255)"},
                            "status": {"value":status, "type":"char(255)"},
                            "reason": {"value":reason[:255], "type":"char(255)"},
                            "remedy": {"value":remedy[:255], "type":"char(255)"},
                            "updated_at": {"value":current_time, "type":"datetime"} ,
                            "variants": {"value":variants_data, "type":"json"} 
                        })

                update_method(final_results, "mercadolibre", "product_status")
                run_procedure("app_import", "update_meli_status")
                logger.info("Process Completed.")
                return
            
        except Exception as e:
            logger.error(f"Error crítico en proceso de auditoría: {e}")
            return []

    return asyncio.run(main())