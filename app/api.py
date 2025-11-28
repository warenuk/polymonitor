"""
API interaction functions.
"""

import requests
from app.config import GAMMA_API_URL, HEADERS

def fetch_active_bitcoin_markets():
    """
    Fetch active Bitcoin markets from Gamma API.
    """
    params = {
        "limit": 100,
        "active": "true",
        "closed": "false",
        "tag_slug": "bitcoin",
        "order": "endDate",
        "ascending": "true"
    }
    try:
        print(f"📡 Запит до Polymarket API...")
        r = requests.get(GAMMA_API_URL, params=params, headers=HEADERS, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"❌ Помилка: {e}")
        return []
