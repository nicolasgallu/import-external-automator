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
           final_results = [] # Asegúrate de que esta lista esté inicializada

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

               # 2. BÚSQUEDA DE IDS (USANDO SCAN & SCROLL)
               item_ids = []
               scroll_id = None
               url_search = f"https://api.mercadolibre.com/users/{user_id}/items/search"
               
               logger.info("Iniciando búsqueda de items con modo 'scan'...")

               while True:
                   # Configuramos los parámetros de búsqueda
                   params = {'search_type': 'scan'}
                   if scroll_id:
                       params['scroll_id'] = scroll_id

                   data, _ = await fetch_json(session, url_search, params=params)
                   
                   # Si no hay datos o la lista de resultados está vacía, terminamos el loop
                   if not data or not data.get('results'):
                       break
                   
                   item_ids.extend(data.get('results', []))
                   
                   # Actualizamos el scroll_id para la siguiente iteración
                   scroll_id = data.get('scroll_id')
                   
                   # Si el API no devuelve un scroll_id, significa que no hay más registros
                   if not scroll_id:
                       break
                   
                
                
               logger.info(f"Recuperados {len(item_ids)} IDs...")

               logger.info(f"Total de IDs recuperados: {len(item_ids)}. Iniciando análisis de publicaciones...")

               # 3. PROCESAMIENTO POR CHUNKS
               current_time = datetime.now()

               for i in range(0, len(item_ids), 20):
                   chunk = item_ids[i:i + 20]
                   async with semaphore:
                       # ML permite consultar hasta 20 IDs en un solo multiget
                       items_data, _ = await fetch_json(
                           session, 
                           "https://api.mercadolibre.com/items", 
                           params={'ids': ','.join(chunk)}
                       )

                   if not items_data:
                       continue

                   for item_info in items_data:
                       # El multiget devuelve una lista de diccionarios con 'code' y 'body'
                       body = item_info.get('body', {})
                       item_id = body.get('id')
                       status = body.get('status')
                       stock = body.get('available_quantity', 0)
                       product_name = body.get('title')
                       
                       reason = "None"
                       remedy = "None"

                       # 4. LÓGICA DE MODERACIÓN
                       if status != 'active' and item_id:
                           async with semaphore:
                               response_mod, status_code = await fetch_json(
                                   session,
                                   f"https://api.mercadolibre.com/moderations/last_moderation/{item_id}-ITM"
                               )

                           if status_code == 200 and response_mod:
                               # response_mod suele ser una lista
                               wordings = response_mod[0].get('wordings', []) if isinstance(response_mod, list) else []
                               if len(wordings) > 0:
                                   reason = wordings[0].get('value', 'No reason provided')
                               if len(wordings) > 1:
                                   remedy = wordings[1].get('value', 'No remedy provided')

                       final_results.append({
                           "meli_id": item_id,
                           "product_name": product_name,
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