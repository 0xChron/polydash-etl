import logging
from config import DB_CONFIG, EVENT_API_URL, MARKET_API_URL, PAGE_LIMIT, DELAY, MAX_RETRIES, DELAY_BETWEEN_RETRIES
from database import Database
from api_client import PolymarketAPIClient
from transformers import DataTransformer

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def main():
    db = None
    
    try:
        logger.info("initializing polymarket data pipeline...")
        db = Database(DB_CONFIG)
        db.connect()
        db.create_tables()
        
        api_client = PolymarketAPIClient(
            event_url=EVENT_API_URL,
            market_url=MARKET_API_URL,
            page_limit=PAGE_LIMIT,
            delay=DELAY,
            max_retries=MAX_RETRIES,
            retry_delay=DELAY_BETWEEN_RETRIES
        )
        
        transformer = DataTransformer()
        
        logger.info("=== FETCHING EVENTS ===")
        raw_events = api_client.fetch_events()
        logger.info(f"fetched {len(raw_events)} events from api")
        
        events_data = transformer.transform_events(raw_events)
        logger.info(f"transformed {len(events_data)} events for insertion")
        db.insert_events(events_data)
        
        logger.info("=== FETCHING MARKETS ===")
        raw_markets = api_client.fetch_markets()
        logger.info(f"fetched {len(raw_markets)} markets from api")
        
        markets_data = transformer.transform_markets(raw_markets)
        logger.info(f"transformed {len(markets_data)} markets for insertion")
        db.insert_markets(markets_data)
        
        logger.info("=== PIPELINE COMPLETED SUCCESSFULLY ===")
        
    except Exception as e:
        logger.error(f"pipeline failed: {e}", exc_info=True)
        raise
    finally:
        if db:
            db.close()


if __name__ == "__main__":
    main()