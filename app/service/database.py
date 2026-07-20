from sqlalchemy import create_engine, text, insert
from google.cloud.sql.connector import Connector
from app.utils.logger import logger
from app.settings.config import INSTANCE_DB, USER_DB, PASSWORD_DB, NAME_DB, SCHMA_FOLDER, SCHMA_MELI, SCHMA_APP

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

def get_method(data):
    """"""
    with engine.begin() as conn:

        q_columns = ', '.join(data.get('q_columns'))
        q_from = data.get('q_from')
        q_join = data.get('q_join', '')
        q_where  = data.get('q_where', '')
        q_limit  = data.get('q_limit', '')

        result = conn.execute(
            text(f"""
                SELECT 
                {q_columns} 
                {q_from} 
                {q_join} 
                {q_where} 
                {q_limit}
                """)
            )
        data = [dict(row) for row in result.mappings()]
        logger.info("Data extraction completed.")
        return data
 




def load_data(fields:str, data:list, stage:str):
    """"""
    try:
        with engine.begin() as conn:
            to_update = ""
            to_update_conflict = ""
            fields_aux = fields.split(',')
            for i in fields_aux:
                if i =='id':
                    to_update+= f":{i.strip()}, "
                    continue
                if i == fields_aux[-1]:
                    to_update_conflict+= f"{i} = values({i.strip()})"
                    to_update+= f":{i.strip()}"
                else:
                    to_update_conflict+= f"{i} = values({i.strip()}), "
                    to_update+= f":{i.strip()}, "

            logger.info(f"updating {len(data)} records - stage: {stage}.")

            conn.execute(text(f"""
                INSERT INTO mercadolibre.catalog_listing ({fields})
                VALUES({to_update})
                ON DUPLICATE KEY UPDATE {to_update_conflict}
            """),data)
            logger.info("Upsert Completed.")

    except Exception as e:
        logger.error(f"Error critico en la carga masiva: {str(e)}")
        raise e



def get_item_actives():
    """"""
    with engine.begin() as conn:
        logger.info("Getting active published products")
        result = conn.execute(
            text(f"""SELECT distinct meli_id from {SCHMA_APP}.product_catalog_sync 
                WHERE status = 'active' and meli_id is not null""")
        )
        data = [dict(row).get('meli_id') for row in result.mappings()]
        return data


def load_item_performance(data):
    """"""
    with engine.begin() as conn:
        logger.info("Starting load of items performance")
        conn.execute(
            text(f"""
                INSERT INTO {SCHMA_MELI}.performance_raw 
                (meli_id, entity_type, score, level, level_wording, calculated_at, updated_at, buckets)
                VALUES (:meli_id, :entity_type, :score, :level, :level_wording, :calculated_at, :updated_at, :buckets) 
                ON DUPLICATE KEY UPDATE 
                entity_type = VALUES(entity_type),
                score = VALUES(score),
                level = VALUES(level),
                level_wording = VALUES(level_wording),
                calculated_at = VALUES(calculated_at),
                updated_at = VALUES(updated_at),
                buckets = VALUES(buckets)
            """), data
        )
        conn.execute(text(f"CALL {SCHMA_MELI}.refresh_performance_data()"))
        logger.info("Load Completed")


def get_items_without_folder():
    """"""
    with engine.begin() as conn:
        logger.info("Extracting items with stock from DB")
        result = conn.execute(
            text(f"""
                SELECT id FROM {SCHMA_FOLDER}.product_catalog_sync
                WHERE drive_url is null and stock >0;
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

import uuid

def update_method(rows: list[dict], schema: str, table: str):
    """
    rows = [
        {
            "id": {"value": 1, "type": "integer"},
            "name": {"value": "John", "type": "varchar"},
            "active": {"value": True, "type": "boolean"}
        },
        {
            "id": {"value": 2, "type": "integer"},
            "name": {"value": "Jane", "type": "varchar"},
            "active": {"value": False, "type": "boolean"}
        }
    ]
    """

    if not rows:
        return

    try:
        # Assume every row has the same schema
        fields = list(rows[0].keys())
        id_field = fields[0]
        temp_table = f"tmp_{table}_{uuid.uuid4().hex[:8]}"

        columns = []

        for field in fields:
            value_type = rows[0][field]["type"]
            columns.append(f"{field} {value_type}")

        create_temp_query = text(f"""
            CREATE TEMPORARY TABLE {temp_table} (
                {", ".join(columns)}
            )
        """)

        params_list = []

        for row in rows:
            params = {}
            for field in fields:
                params[field] = row[field]["value"]
            params_list.append(params)


        logger.info(f"Updating {len(params_list)} records in {schema}.{table}")


        with engine.begin() as conn:

            # 1. Create temporary table
            conn.execute(create_temp_query)

            # 2. Insert rows into temp table
            insert_query = text(f"""
                INSERT INTO {temp_table}
                ({", ".join(fields)})
                VALUES
                ({", ".join([f":{field}" for field in fields])})
            """)

            conn.execute(insert_query, params_list)

            # 3. Update target table from temp table
            update_clauses = []
            
            for field in fields[1:]:
                update_clauses.append(f"{field} = VALUES({field})")
            
            merge_query = text(f"""
                INSERT INTO {schema}.{table}
                ({", ".join(fields)})
                SELECT
                    {", ".join(fields)}
                FROM {temp_table}
                ON DUPLICATE KEY UPDATE
                    {", ".join(update_clauses)}
            """)
            
            result = conn.execute(merge_query)
            logger.info(f"Updated {result.rowcount} rows")
            conn.execute(text(f"DROP TEMPORARY TABLE {temp_table}"))
            
        logger.info("Bulk update completed.")
        conn.execute(text(f"""CALL {SCHMA_FOLDER}.update_meli_status()"""))

    except Exception as e:
        logger.error(
            f"Error during bulk update: {e}"
        )
        raise