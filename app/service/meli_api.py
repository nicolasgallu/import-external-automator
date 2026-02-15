import asyncio
import aiohttp
from datetime import datetime
from app.utils.logger import logger
from app.service.secrets import meli_secrets

def obtain_items():
    """
    Retorna el estado completo de los itmes publicados: meli_id, stock, status, reason, remedy y updated_at.
    """
    token = meli_secrets()
    headers = {'Authorization': f'Bearer {token}'}
    final_results = []

    async def fetch_json(session, url, params=None):
        async with session.get(url, params=params) as resp:
            if resp.status == 200:
                return await resp.json(), resp.status
            return None, resp.status

    async def main():
        try:
            timeout = aiohttp.ClientTimeout(total=60)
            connector = aiohttp.TCPConnector(limit_per_host=10)
            # El semáforo protege de saturar el endpoint de moderaciones
            semaphore = asyncio.Semaphore(20)

            async with aiohttp.ClientSession(
                headers=headers,
                timeout=timeout,
                connector=connector
            ) as session:

                # 1. IDENTIFICACIÓN DE USUARIO
                response, _ = await fetch_json(session, "https://api.mercadolibre.com/users/me")
                if not response:
                    return []
                user_id = response.get('id')

                # 2. BÚSQUEDA PAGINADA DE IDS
                item_ids = []
                limit = 50
                offset = 0
                while True:
                    url_search = f"https://api.mercadolibre.com/users/{user_id}/items/search"
                    data, _ = await fetch_json(session, url_search, params={'limit': limit, 'offset': offset})
                    if not data or not data.get('results'):
                        break
                    item_ids.extend(data.get('results', []))
                    offset += limit

                logger.info(f"Analizando {len(item_ids)} publicaciones...")

                # 3. PROCESAMIENTO POR CHUNKS
                current_time = datetime.now()

                for i in range(0, len(item_ids), 20):
                    chunk = item_ids[i:i + 20]
                    async with semaphore:
                        items_data, _ = await fetch_json(
                            session, 
                            "https://api.mercadolibre.com/items", 
                            params={'ids': ','.join(chunk)}
                        )

                    if not items_data:
                        continue

                    for item_info in items_data:
                        body = item_info.get('body', {})
                        item_id = body.get('id')
                        status = body.get('status')
                        stock = body.get('available_quantity', 0)
                        
                        reason = "None"
                        remedy = "None"

                        # 4. LÓGICA DE MODERACIÓN (Solo si no está activo)
                        if status != 'active' and item_id:
                            async with semaphore:
                                # Consultamos la última moderación para entender el problema
                                response_mod, status_code = await fetch_json(
                                    session,
                                    f"https://api.mercadolibre.com/moderations/last_moderation/{item_id}-ITM"
                                )

                            if status_code == 200 and response_mod:
                                wordings = response_mod[0].get('wordings', [])
                                if len(wordings) > 0:
                                    reason = wordings[0].get('value', 'No reason provided')
                                if len(wordings) > 1:
                                    remedy = wordings[1].get('value', 'No remedy provided')

                        final_results.append({
                            "meli_id": item_id,
                            "stock": stock,
                            "status": status,
                            "reason": reason,
                            "remedy": remedy,
                            "updated_at": current_time
                        })

            return final_results

        except Exception as e:
            logger.error(f"Error crítico en proceso de auditoría: {e}")
            return []

    return asyncio.run(main())