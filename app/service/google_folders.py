import asyncio
import aiohttp
import time
import google.auth
from google.auth import default
from google.auth.transport.requests import Request
from app.utils.logger import logger
from app.settings.config import SCOPES, PARENT_FOLDER_ID, MAX_CONCURRENT_TASKS

# Variable global para trackear el progreso entre corrutinas
progress_counter = 0

async def get_access_token():
    """
    Obtiene el token de acceso de forma automatica.
    Funciona via Application Default Credentials (ADC) tanto en local como en GCP.
    """
    try:
        # google.auth.default() devuelve una tupla (credentials, project_id)
        # Desempaquetamos la tupla para obtener el objeto de credenciales
        creds, project = google.auth.default(scopes=[SCOPES])
        
        auth_req = Request()
        
        # Ahora sí, llamamos a refresh sobre el objeto 'creds'
        creds.refresh(auth_req)
        
        if not creds.token:
            raise Exception("No se pudo obtener el token de acceso desde el entorno.")
            
        logger.info("Credenciales obtenidas exitosamente mediante ADC.")
        return creds.token

    except Exception as e:
        logger.error(f"Error critico al obtener credenciales de Google: {str(e)}")
        raise


async def create_folder_task(session, item_id, token, semaphore, total_items):
    """Tarea individual para crear una carpeta en Drive con tracking de progreso"""
    global progress_counter
    async with semaphore:
        url = "https://www.googleapis.com/drive/v3/files"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            'name': str(item_id),
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [PARENT_FOLDER_ID] if PARENT_FOLDER_ID else []
        }

        try:
            async with session.post(url, json=payload, headers=headers) as resp:
                progress_counter += 1
                
                # Log de progreso cada 50 items para no saturar la consola
                if progress_counter % 50 == 0 or progress_counter == total_items:
                    percentage = (progress_counter / total_items) * 100
                    logger.info(f"Progreso: {progress_counter}/{total_items} ({percentage:.2f}%)")

                if resp.status in [200, 201]:
                    data = await resp.json()
                    folder_id = data.get('id')
                    return {
                        "item_id": item_id, 
                        "drive_url": f"https://drive.google.com/drive/folders/{folder_id}"
                    }
                else:
                    err_body = await resp.text()
                    logger.error(f"Error API Drive para {item_id}: {resp.status} - {err_body}")
                    return None
        except Exception as e:
            logger.error(f"Error de red crítico para {item_id}: {e}")
            return None

async def run_drive_automation(items_input):
    """
    Función principal con métricas de tiempo y progreso.
    """
    global progress_counter
    progress_counter = 0 # Reiniciar contador
    total_items = len(items_input)
    
    logger.info(f"🚀 Iniciando automatización de Drive para {total_items} ítems.")
    start_time = time.time()

    token = await get_access_token()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    
    async with aiohttp.ClientSession() as session:
        tasks = [
            create_folder_task(session, item['id'], token, semaphore, total_items) 
            for item in items_input
        ]
        
        # gather ejecuta todo y mantiene el orden
        results = await asyncio.gather(*tasks)
    
    end_time = time.time()
    duration = end_time - start_time
    
    clean_results = [r for r in results if r is not None]
    
    logger.info("--- Resumen de Ejecución ---")
    logger.info(f"Finalizado en: {duration:.2f} segundos")
    logger.info(f"Exitosos: {len(clean_results)}")
    logger.info(f"Fallidos: {total_items - len(clean_results)}")
    logger.info("----------------------------")
    
    return clean_results