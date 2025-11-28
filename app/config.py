"""
Configuration constants for the application.
"""

# WebSocket configuration according to Polymarket documentation
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Gamma API configuration
GAMMA_API_URL = "https://gamma-api.polymarket.com/events"
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json"
}

# Time to prefetch next market (seconds)
PREFETCH_TIME = 30
