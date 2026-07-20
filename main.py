import asyncio
from app.service.meli_api import product_status_sync
from app.service.database import get_items_without_folder, load_item_folder_url
from app.service.meli_performance import run_get_performance
from app.service.meli_listing_catalog import update_meli_catalog
from app.service.google_folders import run_drive_automation
from app.service.prepublish_api import prepublish_call_ai
from app.settings.config import RUN_FOLDERS, RUN_PERFORMANCE, RUN_CATALOG_LIST

product_status_sync()
prepublish_call_ai()

if RUN_PERFORMANCE == 1:
    run_get_performance()

if RUN_CATALOG_LIST == 1:
   update_meli_catalog()

if RUN_FOLDERS == 1:
    items_list = get_items_without_folder()
    if items_list:
        data_para_db = asyncio.run(run_drive_automation(items_list))
        print(data_para_db)
        load_item_folder_url(data_para_db)
else:
    None