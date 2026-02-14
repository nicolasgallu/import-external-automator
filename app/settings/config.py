import os
from dotenv import load_dotenv
load_dotenv()

PROJECT_ID=os.getenv("PROJECT_ID")
SECRET_ID=os.getenv("SECRET_ID")


INSTANCE_DB=os.getenv("INSTANCE_DB")
USER_DB=os.getenv("USER_DB")
PASSWORD_DB=os.getenv("PASSWORD_DB")
NAME_DB=os.getenv("NAME_DB")


SCOPES=os.getenv("SCOPES")
PARENT_FOLDER_ID=os.getenv("PARENT_FOLDER_ID")
MAX_CONCURRENT_TASKS=int(os.getenv("MAX_CONCURRENT_TASKS"))

RUN_FOLDERS=int(os.getenv("RUN_FOLDERS"))
