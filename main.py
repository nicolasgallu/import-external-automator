import asyncio
from app.service.meli_api import obtain_items
from app.service.database import get_items_without_folder, load_item_folder_url, truncate_items_data
from app.service.google_folders import run_drive_automation 
from app.settings.config import RUN_FOLDERS, RUN_PROCEDURES

items_data = obtain_items()
truncate_items_data(items_data, RUN_PROCEDURES)

if RUN_FOLDERS == 1:
    items_list = get_items_without_folder()
    if items_list:
        data_para_db = asyncio.run(run_drive_automation(items_list))
        print(data_para_db)
        load_item_folder_url(data_para_db)
else:
    None