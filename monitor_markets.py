#!/usr/bin/env python3
import time
import csv
import os
import sys
import json
import concurrent.futures
import requests
from datetime import datetime, timezone

# Додаємо поточну директорію в path
sys.path.append(os.getcwd())

from fetch_markets import find_nearest_markets, extract_ids

# Базова папка для збереження даних
BASE_DATA_DIR = "data_monitor"

def fetch_btc_price():
    """
    Отримує поточну ціну Bitcoin з Binance.
    """
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    try:
        r = requests.get(url, timeout=2)
        if r.status_code == 200:
            data = r.json()
            return data.get('price')
    except Exception as e:
        # print(f"Error fetching BTC price: {e}")
        pass
    return None

def fetch_orderbook(token_id):
    """
    Отримує ордербук для конкретного токена.
    """
    url = f"https://clob.polymarket.com/book?token_id={token_id}"
    try:
        r = requests.get(url, timeout=2)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        pass
    return None

def parse_book(book_data):
    """
    Витягує 5 найкращих бідів та асків.
    """
    if not book_data:
        return [""] * 20 
    
    bids = book_data.get('bids', [])
    asks = book_data.get('asks', [])
    
    bids.sort(key=lambda x: float(x['price']), reverse=True)
    asks.sort(key=lambda x: float(x['price']))
    
    row = []
    
    # Top 5 Bids
    for i in range(5):
        if i < len(bids):
            row.append(bids[i]['price'])
            row.append(bids[i]['size'])
        else:
            row.append("")
            row.append("")
            
    # Top 5 Asks
    for i in range(5):
        if i < len(asks):
            row.append(asks[i]['price'])
            row.append(asks[i]['size'])
        else:
            row.append("")
            row.append("")
            
    return row

def fetch_trades(condition_id):
    """
    Отримує останні угоди для ринку (всі токени).
    """
    url = f"https://data-api.polymarket.com/trades?market={condition_id}&limit=50"
    try:
        r = requests.get(url, timeout=2)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        pass
    return []

def parse_trades(trades_data, token_id, last_check_time):
    """
    Аналізує угоди для конкретного токена:
    1. Фільтрує угоди по token_id (asset).
    2. Знаходить ціну останньої угоди.
    3. Рахує об'єм та формує список угод за останню секунду.
    
    Повертає: (last_price, volume_1s, trades_str)
    """
    if not trades_data:
        return "", 0, ""
    
    # Filter by token_id (asset)
    # Asset ID in response is string, token_id is string.
    # Note: Polymarket API might return asset ID as decimal string of the hex token ID?
    # Wait, in test_trades.py output:
    # 'asset': '109810486116897938257170230814033713159036440060832628675547240993710198643395'
    # This looks like the decimal representation of the token ID.
    # My token_id from extract_ids is also a large decimal string (e.g. "7309...")
    # So direct comparison should work.
    
    token_trades = [t for t in trades_data if t.get('asset') == token_id]
    
    if not token_trades:
        return "", 0, ""
        
    # 1. Last Price (перший елемент - найсвіжіший)
    last_price = token_trades[0].get('price', "")
    
    # 2. Filter trades in the last second
    relevant_trades = []
    volume_1s = 0.0
    
    for t in token_trades:
        try:
            ts = float(t.get('timestamp', 0))
            if ts > last_check_time:
                relevant_trades.append(t)
                volume_1s += float(t.get('size', 0))
            else:
                break
        except:
            continue
            
    # 3. Format trades string
    trades_list = []
    for t in relevant_trades:
        p = t.get('price')
        s = t.get('size')
        trades_list.append(f"{p}@{s}")
        
    trades_str = "|".join(trades_list)
    
    return last_price, volume_1s, trades_str

def create_session_structure(market_4h_info, market_1h_info=None):
    """
    Створює структуру папок для сесії.
    Повертає шлях до кореневої папки сесії.
    """
    # Формуємо назву папки на основі 4-годинного ринку
    # Якщо ринку немає, використовуємо 1-годинний, або поточний час (стабільний в межах години)
    if market_4h_info:
        try:
            end_dt = datetime.fromisoformat(market_4h_info['end_date'].replace('Z', '+00:00'))
            date_str = end_dt.strftime('%Y%m%d_%H%M')
            session_name = f"session_4h_close_{date_str}"
        except:
            session_name = f"session_{datetime.now().strftime('%Y%m%d_%H')}"
    elif market_1h_info:
        try:
            end_dt = datetime.fromisoformat(market_1h_info['end_date'].replace('Z', '+00:00'))
            date_str = end_dt.strftime('%Y%m%d_%H%M')
            session_name = f"session_1h_close_{date_str}"
        except:
            session_name = f"session_{datetime.now().strftime('%Y%m%d_%H')}"
    else:
        # Fallback: Daily/Hourly session to avoid creating new folders on every restart
        session_name = f"session_{datetime.now().strftime('%Y%m%d_%H')}"
        
    session_dir = os.path.join(BASE_DATA_DIR, session_name)
    
    # Створюємо підпапки
    os.makedirs(os.path.join(session_dir, "market_4h"), exist_ok=True)
    os.makedirs(os.path.join(session_dir, "market_1h"), exist_ok=True)
    os.makedirs(os.path.join(session_dir, "market_15m"), exist_ok=True)
    
    return session_dir

def init_market_file(folder_path, market_info, timeframe):
    """
    Створює файл ринку в заданій папці.
    Перевіряє, чи існує файл і чи відповідає він поточному Market ID.
    """
    filename = f"market_{timeframe}.csv"
    full_path = os.path.join(folder_path, filename)
    
    # Якщо файл вже існує, перевіряємо ID
    if os.path.exists(full_path):
        try:
            existing_id = None
            with open(full_path, 'r') as f:
                # Читаємо перші 5 рядків, щоб знайти Market ID
                for _ in range(5):
                    line = f.readline()
                    if "Market ID" in line:
                        parts = line.strip().split(',')
                        if len(parts) > 1:
                            existing_id = parts[1].strip()
                        break
            
            # Порівнюємо ID (як рядки)
            if existing_id and str(existing_id) == str(market_info['market_id']):
                # ID співпадає, продовжуємо писати в цей файл
                return full_path
            else:
                # ID відрізняється (новий ринок), архівуємо старий файл
                archive_name = f"market_{timeframe}_{existing_id if existing_id else 'old'}.csv"
                archive_path = os.path.join(folder_path, archive_name)
                
                # Якщо архівний файл вже існує, додаємо timestamp
                if os.path.exists(archive_path):
                    archive_name = f"market_{timeframe}_{existing_id if existing_id else 'old'}_{int(time.time())}.csv"
                    archive_path = os.path.join(folder_path, archive_name)
                    
                os.rename(full_path, archive_path)
                print(f"📦 Архівовано старий файл {timeframe}: {archive_name}")
                
        except Exception as e:
            print(f"⚠️  Помилка при перевірці файлу {filename}: {e}")
            # Якщо помилка читання, краще не чіпати файл і створити новий з унікальним ім'ям?
            # Або просто перезаписати? Безпечніше архівувати.
            try:
                backup_name = f"market_{timeframe}_backup_{int(time.time())}.csv"
                os.rename(full_path, os.path.join(folder_path, backup_name))
            except:
                pass

    with open(full_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["# METADATA_START"])
        writer.writerow(["Market Title", market_info['title']])
        writer.writerow(["Market ID", market_info['market_id']])
        writer.writerow(["Timeframe", timeframe])
        writer.writerow(["YES Token ID", market_info['yes_id']])
        writer.writerow(["NO Token ID", market_info['no_id']])
        writer.writerow(["Start Time (UTC)", datetime.now(timezone.utc).isoformat()])
        writer.writerow(["# METADATA_END"])
        
        cols = ["Timestamp_UTC"]
        
        # YES Token Columns
        cols.extend(["YES_Last_Price", "YES_Vol_1s", "YES_Trades_1s"])
        for i in range(1, 6):
            cols.extend([f"YES_Bid_{i}_Price", f"YES_Bid_{i}_Size"])
        for i in range(1, 6):
            cols.extend([f"YES_Ask_{i}_Price", f"YES_Ask_{i}_Size"])
            
        # NO Token Columns
        cols.extend(["NO_Last_Price", "NO_Vol_1s", "NO_Trades_1s"])
        for i in range(1, 6):
            cols.extend([f"NO_Bid_{i}_Price", f"NO_Bid_{i}_Size"])
        for i in range(1, 6):
            cols.extend([f"NO_Ask_{i}_Price", f"NO_Ask_{i}_Size"])
        
        writer.writerow(cols)
        
    return full_path

def init_btc_file(session_dir):
    """
    Створює файл для моніторингу ціни BTC.
    """
    full_path = os.path.join(session_dir, "btc_price_monitoring.csv")
    
    # Якщо файл існує, просто повертаємо шлях (дописуємо)
    if os.path.exists(full_path):
        return full_path
    
    with open(full_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp_UTC", "BTC_Price_USDT"])
        
    return full_path

def main():
    print("🔍 Пошук актуальних ринків...")
    markets = find_nearest_markets()
    
    # Отримуємо інфо про ринки для назви сесії
    info_4h = None
    if markets.get('4h'):
        info_4h = extract_ids(markets['4h'])
        
    info_1h = None
    if markets.get('1h'):
        info_1h = extract_ids(markets['1h'])
        
    # Створюємо структуру папок
    session_dir = create_session_structure(info_4h, info_1h)
    print(f"📂 Сесія: {session_dir}")
    
    active_monitors = {}
    
    # Ініціалізація ринків
    for tf in ['15m', '1h', '4h']:
        m = markets.get(tf)
        if m:
            info = extract_ids(m)
            yes_id = info['yes_id']
            no_id = info['no_id']
            condition_id = info['condition_id']
            
            if yes_id == "N/A" or no_id == "N/A":
                print(f"⚠️  Пропускаємо {tf}: відсутні ID токенів.")
                continue

            # Визначаємо папку для цього таймфрейму
            market_dir = os.path.join(session_dir, f"market_{tf}")
            filepath = init_market_file(market_dir, info, tf)
            
            active_monitors[tf] = {
                'yes_id': yes_id,
                'no_id': no_id,
                'condition_id': condition_id,
                'file': filepath,
                'title': info['title']
            }
            print(f"✅ {tf.upper()} -> {filepath}")
        else:
            print(f"⚠️  Ринок {tf} не знайдено.")
            
    # Ініціалізація BTC моніторингу
    btc_file = init_btc_file(session_dir)
    print(f"✅ BTC -> {btc_file}")

    if not active_monitors:
        print("❌ Немає ринків для моніторингу. Вихід.")
        return

    print("\n🚀 Моніторинг запущено! (Оновлення щосекунди)")
    print("   Натисніть Ctrl+C для зупинки.\n")
    
    # Зберігаємо час останньої перевірки для кожного токена, щоб не дублювати угоди
    # Але для простоти будемо використовувати глобальний loop_start попереднього циклу
    last_loop_time = time.time() - 1.0 
    
    try:
        while True:
            loop_start = time.time()
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            
            # 1. Отримуємо ціну BTC
            btc_price = fetch_btc_price()
            
            if btc_price:
                with open(btc_file, 'a', newline='') as f:
                    csv.writer(f).writerow([timestamp, btc_price])
            
            # 2. Отримуємо дані ринків паралельно (Book + Trades)
            # Створюємо список завдань
            tasks = []
            for tf, data in active_monitors.items():
                tasks.append((tf, 'YES', 'BOOK', data['yes_id']))
                tasks.append((tf, 'NO', 'BOOK', data['no_id']))
                tasks.append((tf, 'MARKET', 'TRADES', data['condition_id']))
            
            results = {} 
            # Structure: results[tf][type][data_kind]
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
                future_to_task = {}
                for tf, type_, kind, id_val in tasks:
                    if kind == 'BOOK':
                        future = executor.submit(fetch_orderbook, id_val)
                    else:
                        future = executor.submit(fetch_trades, id_val)
                    future_to_task[future] = (tf, type_, kind)
                
                for future in concurrent.futures.as_completed(future_to_task):
                    tf, type_, kind = future_to_task[future]
                    if tf not in results: results[tf] = {'YES': {}, 'NO': {}, 'MARKET': {}}
                    results[tf][type_][kind] = future.result()
            
            # Записуємо дані ринків
            for tf, data in active_monitors.items():
                # Market Trades (All)
                market_trades = results.get(tf, {}).get('MARKET', {}).get('TRADES', [])
                
                # YES Data
                yes_book = results.get(tf, {}).get('YES', {}).get('BOOK')
                yes_book_row = parse_book(yes_book)
                yes_last_price, yes_vol, yes_trades_str = parse_trades(market_trades, data['yes_id'], last_loop_time)
                
                # NO Data
                no_book = results.get(tf, {}).get('NO', {}).get('BOOK')
                no_book_row = parse_book(no_book)
                no_last_price, no_vol, no_trades_str = parse_trades(market_trades, data['no_id'], last_loop_time)
                
                # Формуємо повний рядок
                full_row = [timestamp]
                
                full_row.extend([yes_last_price, yes_vol, yes_trades_str])
                full_row.extend(yes_book_row)
                
                full_row.extend([no_last_price, no_vol, no_trades_str])
                full_row.extend(no_book_row)
                
                with open(data['file'], 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(full_row)
            
            # Оновлюємо час останньої перевірки
            last_loop_time = loop_start
            
            # Контроль частоти (1 секунда)
            elapsed = time.time() - loop_start
            sleep_time = max(0, 1.0 - elapsed)
            time.sleep(sleep_time)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Моніторинг зупинено.")
        print(f"📊 Всі дані збережено у: {session_dir}")

if __name__ == "__main__":
    main()
