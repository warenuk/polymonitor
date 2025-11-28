#!/usr/bin/env python3
import time
import csv
import os
import sys
import json
import concurrent.futures
import requests
import threading
from datetime import datetime, timezone, timedelta

# Додаємо поточну директорію в path
sys.path.append(os.getcwd())

from fetch_markets import find_nearest_markets, extract_ids

# Базова папка для збереження даних
BASE_DATA_DIR = "data_monitor"

class SessionManager:
    def __init__(self):
        self.lock = threading.Lock()
        self.current_session_dir = None
        self.current_session_end_dt = None
        
    def get_session_dir(self, market_end_dt):
        """
        Повертає шлях до папки сесії для заданого часу закінчення ринку.
        Якщо час виходить за межі поточної сесії, створює нову.
        """
        with self.lock:
            if self.current_session_dir is None:
                self._create_new_session(market_end_dt)
                return self.current_session_dir
            
            if market_end_dt <= self.current_session_end_dt + timedelta(minutes=1):
                return self.current_session_dir
            
            print(f"🔄 [Session] Час ринку {market_end_dt} виходить за межі сесії {self.current_session_end_dt}. Створення нової...")
            self._create_new_session(market_end_dt)
            return self.current_session_dir
            
    def _create_new_session(self, target_dt):
        """
        Створює нову структуру папок на основі 4-годинних інтервалів.
        """
        base_time = target_dt.replace(minute=0, second=0, microsecond=0)
        remainder = base_time.hour % 4
        hours_to_add = 4 - remainder
        if hours_to_add == 0 and target_dt.minute == 0:
             session_end = target_dt
        else:
            session_end = base_time + timedelta(hours=hours_to_add)
            
        self.current_session_end_dt = session_end
        
        date_str = session_end.strftime('%Y%m%d_%H%M')
        session_name = f"session_4h_close_{date_str}"
        self.current_session_dir = os.path.join(BASE_DATA_DIR, session_name)
        
        os.makedirs(os.path.join(self.current_session_dir, "market_4h"), exist_ok=True)
        os.makedirs(os.path.join(self.current_session_dir, "market_1h"), exist_ok=True)
        os.makedirs(os.path.join(self.current_session_dir, "market_15m"), exist_ok=True)
        
        print(f"📂 [Session] Нова сесія: {session_name} (End: {session_end})")


# Глобальний менеджер сесій
session_manager = SessionManager()


def fetch_btc_price():
    url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
    try:
        r = requests.get(url, timeout=2)
        if r.status_code == 200:
            data = r.json()
            return data.get('price')
    except:
        pass
    return None

def fetch_orderbook(token_id):
    url = f"https://clob.polymarket.com/book?token_id={token_id}"
    try:
        r = requests.get(url, timeout=2)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return None

def parse_book(book_data):
    if not book_data:
        return [""] * 20 
    
    bids = book_data.get('bids', [])
    asks = book_data.get('asks', [])
    
    bids.sort(key=lambda x: float(x['price']), reverse=True)
    asks.sort(key=lambda x: float(x['price']))
    
    row = []
    for i in range(5):
        if i < len(bids):
            row.append(bids[i]['price'])
            row.append(bids[i]['size'])
        else:
            row.append("")
            row.append("")
    for i in range(5):
        if i < len(asks):
            row.append(asks[i]['price'])
            row.append(asks[i]['size'])
        else:
            row.append("")
            row.append("")
    return row

def fetch_trades(condition_id):
    url = f"https://data-api.polymarket.com/trades?market={condition_id}&limit=50"
    try:
        r = requests.get(url, timeout=2)
        if r.status_code == 200:
            return r.json()
    except:
        pass
    return []

def parse_trades(trades_data, token_id, last_check_time):
    if not trades_data:
        return "", 0, ""
    
    token_trades = [t for t in trades_data if t.get('asset') == token_id]
    if not token_trades:
        return "", 0, ""
        
    last_price = token_trades[0].get('price', "")
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
            
    trades_list = []
    for t in relevant_trades:
        p = t.get('price')
        s = t.get('size')
        trades_list.append(f"{p}@{s}")
        
    trades_str = "|".join(trades_list)
    return last_price, volume_1s, trades_str

def init_market_file(folder_path, market_info, timeframe):
    filename = f"market_{timeframe}.csv"
    full_path = os.path.join(folder_path, filename)
    
    if os.path.exists(full_path):
        try:
            existing_id = None
            with open(full_path, 'r') as f:
                for _ in range(5):
                    line = f.readline()
                    if "Market ID" in line:
                        parts = line.strip().split(',')
                        if len(parts) > 1:
                            existing_id = parts[1].strip()
                        break
            
            if existing_id and str(existing_id) == str(market_info['market_id']):
                return full_path
            else:
                archive_name = f"market_{timeframe}_{existing_id if existing_id else 'old'}_{int(time.time())}.csv"
                archive_path = os.path.join(folder_path, archive_name)
                os.rename(full_path, archive_path)
                print(f"📦 [{timeframe}] Архівовано старий файл: {archive_name}")
                
        except Exception as e:
            print(f"⚠️  [{timeframe}] Помилка при перевірці файлу: {e}")
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
        cols.extend(["YES_Last_Price", "YES_Vol_1s", "YES_Trades_1s"])
        for i in range(1, 6): cols.extend([f"YES_Bid_{i}_Price", f"YES_Bid_{i}_Size"])
        for i in range(1, 6): cols.extend([f"YES_Ask_{i}_Price", f"YES_Ask_{i}_Size"])
        cols.extend(["NO_Last_Price", "NO_Vol_1s", "NO_Trades_1s"])
        for i in range(1, 6): cols.extend([f"NO_Bid_{i}_Price", f"NO_Bid_{i}_Size"])
        for i in range(1, 6): cols.extend([f"NO_Ask_{i}_Price", f"NO_Ask_{i}_Size"])
        
        writer.writerow(cols)
        
    return full_path

def init_btc_file(session_dir):
    full_path = os.path.join(session_dir, "btc_price_monitoring.csv")
    if os.path.exists(full_path):
        return full_path
    with open(full_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["Timestamp_UTC", "BTC_Price_USDT"])
    return full_path

def monitor_btc():
    """
    Окремий потік для моніторингу ціни BTC.
    """
    print("🚀 [BTC] Моніторинг запущено.")
    
    now = datetime.now(timezone.utc)
    session_dir = session_manager.get_session_dir(now)
    current_btc_file = init_btc_file(session_dir)
    current_session_path = session_dir
    
    while True:
        loop_start = time.time()
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        
        new_session_dir = session_manager.get_session_dir(datetime.now(timezone.utc))
        
        if new_session_dir != current_session_path:
            current_session_path = new_session_dir
            current_btc_file = init_btc_file(current_session_path)
            print(f"🔄 [BTC] Перемикання на нову папку: {current_session_path}")
        
        price = fetch_btc_price()
        if price:
            with open(current_btc_file, 'a', newline='') as f:
                csv.writer(f).writerow([timestamp, price])
        
        elapsed = time.time() - loop_start
        time.sleep(max(0, 1.0 - elapsed))

def monitor_single_market(timeframe, market_info):
    """
    Цикл моніторингу одного ринку.
    """
    try:
        end_dt = datetime.fromisoformat(market_info['end_date'].replace('Z', '+00:00'))
    except:
        end_dt = datetime.now(timezone.utc) + timedelta(hours=1)
        
    session_dir = session_manager.get_session_dir(end_dt)
    market_dir = os.path.join(session_dir, f"market_{timeframe}")
    file_path = init_market_file(market_dir, market_info, timeframe)
    
    print(f"✅ [{timeframe}] Старт: {market_info['title']} (End: {end_dt})")
    
    yes_id = market_info['yes_id']
    no_id = market_info['no_id']
    condition_id = market_info['condition_id']
    
    last_loop_time = time.time() - 1.0
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)
    
    try:
        while True:
            loop_start = time.time()
            now = datetime.now(timezone.utc)
            
            if now >= end_dt:
                print(f"🏁 [{timeframe}] Завершено: {market_info['title']}")
                break
                
            timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            
            future_yes_book = executor.submit(fetch_orderbook, yes_id)
            future_no_book = executor.submit(fetch_orderbook, no_id)
            future_trades = executor.submit(fetch_trades, condition_id)
            
            yes_book = future_yes_book.result()
            no_book = future_no_book.result()
            trades = future_trades.result()
            
            yes_book_row = parse_book(yes_book)
            no_book_row = parse_book(no_book)
            
            yes_last, yes_vol, yes_str = parse_trades(trades, yes_id, last_loop_time)
            no_last, no_vol, no_str = parse_trades(trades, no_id, last_loop_time)
            
            full_row = [timestamp, yes_last, yes_vol, yes_str] + yes_book_row + \
                       [no_last, no_vol, no_str] + no_book_row
                       
            with open(file_path, 'a', newline='') as f:
                csv.writer(f).writerow(full_row)
                
            last_loop_time = loop_start
            elapsed = time.time() - loop_start
            time.sleep(max(0, 1.0 - elapsed))
            
    finally:
        executor.shutdown(wait=False)

def monitor_lifecycle(timeframe):
    print(f"🔄 [{timeframe}] Потік запущено.")
    
    while True:
        try:
            all_markets = find_nearest_markets()
            market_data = all_markets.get(timeframe)
            
            if not market_data:
                print(f"⏳ [{timeframe}] Ринок не знайдено. Чекаємо...")
                time.sleep(5)
                continue
                
            info = extract_ids(market_data)
            
            if info['yes_id'] == "N/A" or info['no_id'] == "N/A":
                print(f"⚠️ [{timeframe}] Некоректні ID. Пропуск...")
                time.sleep(5)
                continue
                
            monitor_single_market(timeframe, info)
            
            print(f"🔄 [{timeframe}] Пошук наступного ринку...")
            time.sleep(1)
            
        except Exception as e:
            print(f"❌ [{timeframe}] Помилка: {e}")
            time.sleep(5)

def main():
    print("🚀 Запуск системи моніторингу ринків...")
    
    threads = []
    
    t_btc = threading.Thread(target=monitor_btc, daemon=True)
    t_btc.start()
    threads.append(t_btc)
    
    for tf in ['15m', '1h', '4h']:
        t = threading.Thread(target=monitor_lifecycle, args=(tf,), daemon=True)
        t.start()
        threads.append(t)
        
    print("\n✅ Всі потоки активні. Ctrl+C для виходу.\n")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Зупинка...")

if __name__ == "__main__":
    main()
