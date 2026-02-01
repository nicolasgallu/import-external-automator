from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector
from app.utils.logger import logger
import time
from app.settings.config import INSTANCE_DB, USER_DB, PASSWORD_DB, NAME_DB

def getconn():
    connector = Connector() 
    return connector.connect(
        INSTANCE_DB,
        "pymysql",
        user=USER_DB,
        password=PASSWORD_DB,
        db=NAME_DB,
    )   

engine = create_engine(
        "mysql+pymysql://",
        creator=getconn,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=2,
    )


def get_items_with_stock():
    """return all id from items with stock and without URL Drive created"""

    with engine.begin() as conn:
        logger.info(f"Extracting items with stock from DB")
        result = conn.execute(
            text(f"""
                SELECT id FROM app_import.product_catalog_sync
                WHERE stock > 0 and drive_url is null;
            """)
        )
        data = [dict(row) for row in result.mappings()]
        if data:
            logger.info("Data extraction completed.")
            return data
        else:
            logger.info("There are not items to process")
            return None


def load_item_drive_url(data_list):
    """
    Carga masiva optimizada utilizando tabla temporal y JOIN para maximizar el rendimiento.
    """
    if not data_list:
        logger.warning("Advertencia: No se recibieron datos para procesar en la base de datos.")
        return

    total_records = len(data_list)
    start_time = time.time()
    
    logger.info(f"Iniciando proceso de carga masiva: {total_records} registros.")

    try:
        with engine.begin() as conn:
            # 1. Crear tabla temporal
            logger.info("Paso 1/3: Creando tabla temporal en el motor de base de datos.")
            conn.execute(text("""
                CREATE TEMPORARY TABLE temp_drive_urls (
                    item_id INT, 
                    drive_url VARCHAR(255),
                    PRIMARY KEY (item_id)
                ) ENGINE=InnoDB;
            """))

            # 2. Insertar datos en la tabla temporal
            logger.info(f"Paso 2/3: Insertando {total_records} registros en la tabla temporal.")
            conn.execute(text("""
                INSERT INTO temp_drive_urls (item_id, drive_url) 
                VALUES (:item_id, :drive_url)
            """), data_list)

            # 3. Update masivo con JOIN
            logger.info("Paso 3/3: Ejecutando operacion UPDATE JOIN sobre la tabla destino.")
            result = conn.execute(text("""
                UPDATE app_import.product_catalog_sync AS target
                INNER JOIN temp_drive_urls AS source ON target.id = source.item_id
                SET target.drive_url = source.drive_url
            """))
            
            rows_affected = result.rowcount
            
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info("Resumen de ejecucion de base de datos:")
        logger.info(f"Tiempo total: {duration:.2f} segundos.")
        logger.info(f"Registros procesados: {total_records}")
        logger.info(f"Registros actualizados exitosamente: {rows_affected}")

    except Exception as e:
        logger.error(f"Error critico en la carga masiva: {str(e)}")
        # Re-lanzamos la excepcion para manejo en capas superiores si es necesario
        raise e

def truncate_meli_item_status(product_status):

    with engine.begin() as conn:
        
        conn.execute(text("TRUNCATE TABLE mercadolibre.product_status"))
        logger.info("Truncate of mercadolibre.product_status Done.")
        
        logger.info("Starting Data Load")
        conn.execute(
            text("""
                INSERT INTO mercadolibre.product_status (meli_id, status, reason, remedy, updated_at)
                VALUES (:meli_id, :status, :reason, :remedy, :updated_at)
            """),
            product_status
        )
        logger.info("Load Completed")

        logger.info("Running Procedures.")
        conn.execute(text("""CALL `app_import`.`update_meli_status`()"""))
        logger.info("Procedures Completed.")