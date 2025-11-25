import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
}

EVENT_API_URL = "https://gamma-api.polymarket.com/events"
MARKET_API_URL = "https://gamma-api.polymarket.com/markets"

PAGE_LIMIT = 500
DELAY = 2.0
MAX_RETRIES = 3
DELAY_BETWEEN_RETRIES = 5