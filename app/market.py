"""
Market finding and data extraction logic.
"""

import json
from datetime import datetime, timedelta, timezone
from dateutil import parser

from app.api import fetch_active_bitcoin_markets
from app.utils import get_target_time_h1

def find_markets():
    """
    Find current H1 and M15 markets.
    """
    now = datetime.now(timezone.utc)
    
    # 1. Target time for H1 - next exact hour
    h1_target = get_target_time_h1()
    
    # 2. Target time for M15 - next 15-minute interval
    current_minute = now.minute
    
    if current_minute < 15:
        next_m15_minute = 15
        m15_target = now.replace(minute=next_m15_minute, second=0, microsecond=0)
    elif current_minute < 30:
        next_m15_minute = 30
        m15_target = now.replace(minute=next_m15_minute, second=0, microsecond=0)
    elif current_minute < 45:
        next_m15_minute = 45
        m15_target = now.replace(minute=next_m15_minute, second=0, microsecond=0)
    else:  # current_minute >= 45
        # Next interval is 00 minutes of next hour
        m15_target = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    print(f"🎯 Шукаємо H1 (Кінець години): {h1_target.strftime('%H:%M:%S')} UTC")
    print(f"🎯 Шукаємо M15 (наступний інтервал): {m15_target.strftime('%H:%M:%S')} UTC")

    events = fetch_active_bitcoin_markets()
    
    h1_market = None
    m15_market = None

    # Keywords to EXCLUDE for H1 markets
    # We want strictly hourly markets, not Daily, Weekly, Monthly, or 4h markets that happen to expire at the same time.
    NON_H1_KEYWORDS = ["daily", "weekly", "month", "year", "4h", "quarterly"]

    for event in events:
        if 'endDate' not in event: continue
        try:
            end_date = parser.isoparse(event['endDate'])
        except: continue
        
        title = event.get('title', '').lower()
        slug = event.get('slug', '').lower()
        is_m15_slug = "15m" in slug or "15 min" in title

        # Check for non-H1 keywords
        is_long_term = any(kw in slug for kw in NON_H1_KEYWORDS) or any(kw in title for kw in NON_H1_KEYWORDS)

        # --- H1 SEARCH LOGIC ---
        # Must match target time, NOT be M15, and NOT be long-term (Daily/Weekly/etc)
        diff_h1 = abs((end_date - h1_target).total_seconds())
        if diff_h1 < 120 and not is_m15_slug and not is_long_term:
             h1_market = event

        # --- M15 SEARCH LOGIC ---
        diff_m15 = abs((end_date - m15_target).total_seconds())
        if diff_m15 < 120 and is_m15_slug:
             m15_market = event

        if h1_market and m15_market:
            break
    
    return h1_market, m15_market


def find_next_market(current_end_time, is_m15=False):
    """
    Finds the next market after the current one.
    
    Args:
        current_end_time: datetime object of current market close time
        is_m15: True if searching for M15 market, False for H1
    
    Returns:
        event object of next market or None
    """
    if is_m15:
        next_target = current_end_time + timedelta(minutes=15)
    else:
        next_target = current_end_time + timedelta(hours=1)
    
    print(f"🔍 Шукаємо наступний {'M15' if is_m15 else 'H1'} ринок: {next_target.strftime('%H:%M:%S')} UTC")
    
    events = fetch_active_bitcoin_markets()
    
    # Keywords to EXCLUDE for H1 markets
    NON_H1_KEYWORDS = ["daily", "weekly", "month", "year", "4h", "quarterly"]

    for event in events:
        if 'endDate' not in event: continue
        try:
            end_date = parser.isoparse(event['endDate'])
        except: continue
        
        title = event.get('title', '').lower()
        slug = event.get('slug', '').lower()
        is_m15_slug = "15m" in slug or "15 min" in title
        is_long_term = any(kw in slug for kw in NON_H1_KEYWORDS) or any(kw in title for kw in NON_H1_KEYWORDS)
        
        if is_m15 and not is_m15_slug:
            continue
        if not is_m15:
            # If looking for H1, skip M15 and skip Long Term
            if is_m15_slug or is_long_term:
                continue
        
        diff = abs((end_date - next_target).total_seconds())
        if diff < 120:  # Within 2 minutes
            print(f"✅ Знайдено наступний ринок: {event.get('title')}")
            return event
    
    print(f"⚠️  Наступний ринок не знайдено")
    return None

def extract_ids(market_event):
    """
    Extracts relevant IDs and info from a market event.
    """
    if not market_event or not market_event.get('markets'): return None
    md = market_event['markets'][0]
    
    clob_token_ids = md.get('clobTokenIds', '[]')
    if isinstance(clob_token_ids, str):
        try:
            clob_token_ids = json.loads(clob_token_ids)
        except:
            clob_token_ids = []
    
    return {
        "title": market_event.get('title'),
        "start_date": market_event.get('startDate'),
        "end_date": market_event.get('endDate'),
        "market_id": md.get('id'),
        "condition_id": md.get('conditionId'),
        "question_id": md.get('questionId'),
        "yes_id": clob_token_ids[0] if len(clob_token_ids) > 0 else "N/A",
        "no_id": clob_token_ids[1] if len(clob_token_ids) > 1 else "N/A"
    }
