import os
from dotenv import load_dotenv

load_dotenv()

CONFIG = {
    'DB_HOST': os.environ.get('DB_HOST', ''),
    'DB_NAME': os.environ.get('DB_NAME', ''),
    'DB_PASS': os.environ.get('DB_PASS', ''),
    'DB_USER': os.environ.get('DB_USER', ''),
    'DB_PORT': int(os.environ.get('DB_PORT', 0)),
}
