from sqlalchemy import create_engine, text
from google.cloud.sql.connector import Connector
from app.utils.logger import logger
from app.settings.config import INSTANCE_DB, USER_DB, PASSWORD_DB, NAME_DB, SCHMA_FOLDER, SCHMA_MELI

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


def truncate_items_data(product_status, run_procedure):
    """"""
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {SCHMA_MELI}.product_status"))
        logger.info(f"Truncate of {SCHMA_MELI}.product_status Done.")
        logger.info("Starting Data Load")
        conn.execute(
            text(f"""
                INSERT INTO {SCHMA_MELI}.product_status (meli_id, stock, status, reason, remedy, updated_at)
                VALUES (:meli_id, :stock, :status, :reason, :remedy, :updated_at)
            """),
            product_status
        )
        logger.info("Load Completed")
        if run_procedure == 1:
            logger.info("Running Procedures.")
            conn.execute(text(f"""CALL {SCHMA_FOLDER}.update_meli_status()"""))
            logger.info("Procedures Completed.")

def get_items_without_folder():
    """"""
    with engine.begin() as conn:
        logger.info("Extracting items with stock from DB")
        result = conn.execute(
            text(f"""
                SELECT id FROM {SCHMA_FOLDER}.product_catalog_sync
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

def load_item_folder_url(data_list):
    """"""
    logger.info("Iniciando proceso de carga de folder urls.")
    try:
        with engine.begin() as conn:
            # 1. Crear tabla temporal
            logger.info("Paso 1/3: Creando tabla temporal en el motor de base de datos.")
            conn.execute(text(f"""
                CREATE TEMPORARY TABLE {SCHMA_FOLDER}.temp_drive_urls (
                    item_id INT, 
                    drive_url VARCHAR(255),
                    PRIMARY KEY (item_id)
                ) ENGINE=InnoDB;
            """))
            # 2. Insertar datos en la tabla temporal
            conn.execute(text(f"""
                INSERT INTO {SCHMA_FOLDER}.temp_drive_urls (item_id, drive_url) 
                VALUES (:item_id, :drive_url)
            """), data_list)
            # 3. Update masivo con JOIN
            logger.info("Paso 3/3: Ejecutando operacion UPDATE JOIN sobre la tabla destino.")
            conn.execute(text(f"""
                UPDATE {SCHMA_FOLDER}.product_catalog_sync AS target
                INNER JOIN {SCHMA_FOLDER}.temp_drive_urls AS source ON target.id = source.item_id
                SET target.drive_url = source.drive_url
            """))
    except Exception as e:
        logger.error(f"Error critico en la carga masiva: {str(e)}")
        raise e