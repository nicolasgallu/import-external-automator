import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s / %(module)s / %(levelname)s / %(message)s',
    datefmt='%Y-%m-%d %I:%M:%S'

)

logger = logging.getLogger()
