import asyncio
import aiohttp
from datetime import datetime
from app.utils.logger import logger
from app.service.secrets import meli_secrets
    

def obtain_item_status():
    """
    Retorna una lista de diccionarios con el status de los items y sus infracciones.
    """
    token = meli_secrets()
    headers = {'Authorization': f'Bearer {token}'}
    # Lista donde acumularemos los resultados para el bulk load
    final_results = []

    async def fetch_json(session, url, params=None):
        async with session.get(url, params=params) as resp:
            if resp.status == 200:
                return await resp.json(), resp.status
            return None, resp.status

    async def main():
        try:
            timeout = aiohttp.ClientTimeout(total=60) # Aumentado para evitar cortes en procesos largos
            connector = aiohttp.TCPConnector(limit_per_host=10)
            semaphore = asyncio.Semaphore(20)

            async with aiohttp.ClientSession(
                headers=headers,
                timeout=timeout,
                connector=connector
            ) as session:

                # 1. OBTENER USER ID
                response, _ = await fetch_json(session, "https://api.mercadolibre.com/users/me")
                if not response:
                    return []
                user_id = response.get('id')

                # 2. BÚSQUEDA DE ITEMS (PAGINADA)
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

                logger.info(f"Total Items to Check: {len(item_ids)}")

                # 3. PROCESAR ITEMS EN CHUNKS DE 20
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
                        
                        # Valores por defecto
                        reason = "None"
                        remedy = "None"

                        # 4. SI NO ESTÁ ACTIVO, BUSCAR MODERACIONES
                        if status != 'active' and item_id:
                            async with semaphore:
                                response_mod, status_code = await fetch_json(
                                    session,
                                    f"https://api.mercadolibre.com/moderations/last_moderation/{item_id}-ITM"
                                )

                            if status_code == 200 and response_mod:
                                # Extraer Reason y Remedy de los wordings
                                wordings = response_mod[0].get('wordings', [])
                                if len(wordings) > 0:
                                    reason = wordings[0].get('value', 'No reason provided')
                                if len(wordings) > 1:
                                    remedy = wordings[1].get('value', 'No remedy provided')
                        
                        # Construir el diccionario para el Bulk Load
                        final_results.append({
                            "meli_id": item_id,
                            "status": status,
                            "reason": reason,
                            "remedy": remedy,
                            "updated_at": current_time
                        })

            return final_results

        except Exception as e:
            logger.error(f"Error general en obtain_item_status: {e}")
            return []

    # Ejecutar la corrutina y devolver el resultado
    return asyncio.run(main())