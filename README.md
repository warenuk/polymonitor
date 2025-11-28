# Polymarket Monitor

This tool monitors Polymarket events (specifically Bitcoin UP/DOWN markets) and records order book and trade data to CSV files.

## Usage

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Run the monitor:
   ```bash
   python monitor_markets.py
   ```

The script will:
- Find active 15m, 1h, and 4h markets.
- Create a session directory in `data_monitor/`.
- Continuously record top 5 bids/asks and recent trades every second.
