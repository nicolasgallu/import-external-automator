import asyncio
from app.service.meli_api import obtain_items
from app.service.database import get_items_without_folder, load_item_folder_url, truncate_items_data, update_method
from app.service.meli_performance import get_performance
from app.service.meli_listing_catalog import update_meli_catalog
from app.service.google_folders import run_drive_automation 
from app.settings.config import RUN_FOLDERS, RUN_PROCEDURES, RUN_PERFORMANCE, RUN_CATALOG_LIST
import json

items_data = obtain_items()
update_method(items_data,'mercadolibre','product_status')


#if items_data:
#    truncate_items_data(items_data, RUN_PROCEDURES)

#if RUN_PERFORMANCE == 1:
#    get_performance()
#
#if RUN_CATALOG_LIST == 1:
#    update_meli_catalog()
#
#if RUN_FOLDERS == 1:
#    items_list = get_items_without_folder()
#    if items_list:
#        data_para_db = asyncio.run(run_drive_automation(items_list))
#        print(data_para_db)
#        load_item_folder_url(data_para_db)
#else:
#    None