import json
from datetime import date
from typing import List, Dict, Tuple
import logging

logger = logging.getLogger(__name__)


class DataTransformer:
    @staticmethod
    def transform_events(events_data: List[Dict]) -> List[Tuple]:
        insert_data = []
        seen_event_ids = set()
        duplicates = 0
        
        for event in events_data:
            event_id = event.get('id', '')
            
            # skip duplicates
            if event_id in seen_event_ids:
                duplicates += 1
                continue
            seen_event_ids.add(event_id)
            
            # extract fields
            tags = event.get('tags', [])
            labels = [tag.get('label', '') for tag in tags]
            slugs = [tag.get('slug', '') for tag in tags]
            
            insert_data.append((
                event_id,
                event.get('slug', ''),
                event.get('title', ''),
                event.get('startDate'),
                event.get('endDate'),
                event.get('volume24hr', 0),
                event.get('volume1wk', 0),
                event.get('volume1mo', 0),
                event.get('volume1yr', 0),
                event.get('volume', 0),
                event.get('image', ''),
                event.get('new', False),
                event.get('featured', False),
                event.get('liquidity', 0),
                event.get('negRisk', False),
                json.dumps(labels),
                json.dumps(slugs),
                date.today()
            ))
        
        if duplicates > 0:
            logger.info(f"removed {duplicates} duplicate event(s)")
        
        return insert_data
    
    @staticmethod
    def transform_markets(markets_data: List[Dict]) -> List[Tuple]:
        insert_data = []
        seen_market_keys = set()
        duplicates = 0
        fetch_date = date.today()
        
        for market in markets_data:
            market_id = market.get('id', '')
            market_key = (market_id, fetch_date)
            
            if market_key in seen_market_keys:
                duplicates += 1
                continue
            seen_market_keys.add(market_key)
            
            outcomes = json.loads(market.get('outcomes', '[]'))
            outcome_yes = outcomes[0] if len(outcomes) > 0 else None
            outcome_no = outcomes[1] if len(outcomes) > 1 else None
            
            outcome_prices = json.loads(market.get('outcomePrices', '[]'))
            outcome_yes_price = float(outcome_prices[0]) if len(outcome_prices) > 0 else None
            outcome_no_price = float(outcome_prices[1]) if len(outcome_prices) > 1 else None
            
            insert_data.append((
                market_id,
                market.get('slug', ''),
                market.get('question', ''),
                market.get('endDate'),
                market.get('liquidity', 0),
                market.get('startDate'),
                market.get('image', ''),
                outcome_yes,
                outcome_no,
                market.get('volume24hr', 0),
                market.get('volume1wk', 0),
                market.get('volume1mo', 0),
                market.get('volume1yr', 0),
                market.get('volume', 0),
                market.get('new', False),
                market.get('featured', False),
                market.get('negRisk', False),
                outcome_yes_price,
                outcome_no_price,
                market.get('oneDayPriceChange', 0),
                market.get('oneHourPriceChange', 0),
                market.get('oneWeekPriceChange', 0),
                market.get('oneMonthPriceChange', 0),
                market.get('lastTradePrice', 0),
                fetch_date
            ))
        
        if duplicates > 0:
            logger.info(f"removed {duplicates} duplicate market(s)")
        
        return insert_data