import json
from google.cloud import secretmanager
from app.utils.logger import logger
from app.settings.config import PROJECT_ID, SECRET_ID

def meli_secrets():
    logger.info("Getting secrets from Meli Account")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    response = response.payload.data.decode("UTF-8")
    response = json.loads(response)
    token = response['questions']['TOKEN']
    if token:
        logger.info("Secrets from Meli obtained")
        return token
    else:
        logger.error("Failed to get secrets from Meli")
        return None