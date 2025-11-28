#!/usr/bin/env python3
import sys
import os
from datetime import datetime, timezone
from dateutil import parser

# Add current directory to path
sys.path.append(os.getcwd())

from app.api import fetch_active_bitcoin_markets
from app.market import extract_ids

def find_nearest_markets():
    print("📡 Отримання даних з Polymarket...")
    events = fetch_active_bitcoin_markets()
    
    now = datetime.now(timezone.utc)
    
    markets = {
        '15m': [],
        '1h': [],
        '4h': []
    }
    
    # Keywords to exclude for 1h
    NON_H1_KEYWORDS = ["daily", "weekly", "month", "year", "4h", "quarterly"]
    
    for event in events:
        if 'endDate' not in event: continue
        try:
            end_date = parser.isoparse(event['endDate'])
        except: continue
        
        # Skip past markets
        if end_date <= now:
            continue
            
        slug = event.get('slug', '').lower()
        title = event.get('title', '').lower()
        
        # 15m Markets
        if "15m" in slug or "15 min" in title:
            markets['15m'].append((end_date, event))
            continue
            
        # 4h Markets
        if "4h" in slug or "4 hour" in title:
            markets['4h'].append((end_date, event))
            continue
            
        # 1h Markets (Default if not others and not long term)
        is_long_term = any(kw in slug for kw in NON_H1_KEYWORDS) or any(kw in title for kw in NON_H1_KEYWORDS)
        if not is_long_term:
            markets['1h'].append((end_date, event))

    # Sort by date and pick nearest
    results = {}
    for timeframe in markets:
        markets[timeframe].sort(key=lambda x: x[0])
        if markets[timeframe]:
            results[timeframe] = markets[timeframe][0][1]
        else:
            results[timeframe] = None
            
    return results

def main():
    found_markets = find_nearest_markets()
    
    print("\n" + "="*60)
    print("📊 АКТУАЛЬНІ РИНКИ (BTC UP/DOWN)")
    print("="*60)
    
    order = ['15m', '1h', '4h']
    
    for timeframe in order:
        market = found_markets.get(timeframe)
        print(f"\n🔹 Ринок {timeframe.upper()}:")
        
        if market:
            data = extract_ids(market)
            print(f"   Назва:      {data['title']}")
            print(f"   Закриття:   {data['end_date']}")
            print(f"   Market ID:  {data['market_id']}")
            print(f"   ✅ YES ID:  {data['yes_id']}")
            print(f"   ❌ NO ID:   {data['no_id']}")
        else:
            print("   ⚠️  Не знайдено")
            
    print("\n" + "="*60)

if __name__ == "__main__":
    main()
