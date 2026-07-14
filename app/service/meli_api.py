import asyncio
import aiohttp
from datetime import datetime
from app.utils.logger import logger
from app.service.secrets import meli_secrets


def obtain_items():
    """
    Retorna el estado completo de los items publicados:
    meli_id, stock, variation_quantity, status, reason, remedy y updated_at.
    """

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

                # ==========================================================
                # 1. USER
                # ==========================================================

                response, _ = await fetch_json(
                    session,
                    "https://api.mercadolibre.com/users/me"
                )

                if not response:
                    return []

                user_id = response["id"]

                # ==========================================================
                # 2. SEARCH ITEMS (SCAN)
                # ==========================================================

                item_ids = []
                scroll_id = None

                url_search = f"https://api.mercadolibre.com/users/{user_id}/items/search"

                logger.info("Iniciando búsqueda de items con modo scan...")

                while True:

                    params = {"search_type": "scan"}

                    if scroll_id:
                        params["scroll_id"] = scroll_id

                    data, _ = await fetch_json(
                        session,
                        url_search,
                        params=params
                    )

                    if not data or not data.get("results"):
                        break

                    item_ids.extend(data["results"])

                    scroll_id = data.get("scroll_id")

                    if not scroll_id:
                        break

                logger.info(f"Total de IDs recuperados: {len(item_ids)}")

                current_time = datetime.now()

                # ==========================================================
                # 3. MULTIGET ITEMS
                # ==========================================================

                for i in range(0, len(item_ids), 20):

                    chunk = item_ids[i:i + 20]

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
                        product_name = body.get("title")
                        stock = body.get("available_quantity", 0)
                        status = body.get("status")

                        # NEW FIELD
                        variation_quantity = len(body.get("variations", []))

                        reason = "None"
                        remedy = "None"

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
                            "meli_id": item_id,
                            "product_name": product_name,
                            "stock": stock,
                            "variation_quantity": variation_quantity,
                            "status": status,
                            "reason": reason[:255],
                            "remedy": remedy[:255],
                            "updated_at": current_time
                        })

            return final_results

        except Exception as e:
            logger.error(f"Error crítico en proceso de auditoría: {e}")
            return []

    return asyncio.run(main())