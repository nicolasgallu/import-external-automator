import asyncio
from app.service.meli_product_status import obtain_item_status
from app.service.database import truncate_meli_item_status, get_items_with_stock, load_item_drive_url
from app.service.google_folders import run_drive_automation 

product_status = obtain_item_status() #obtain all items status.
truncate_meli_item_status(product_status) #overwrite the status in DB.

items_list = get_items_with_stock() #get items id for ones with stock and without drive url.
if items_list:
    data_para_db = asyncio.run(run_drive_automation(items_list)) #creating folders and returning drive url.
    load_item_drive_url(data_para_db) #writting in db drive urls.