"""Constants and configuration for the Quotex API.

This is a trimmed, stable version used by the local bot runner. It defines a
minimal ASSETS mapping and core connection/timeframe constants. For a full
asset list you can replace `ASSETS` with the provider's full mapping.
"""
import time
import random
from typing import List, Dict, Any, Optional

try:
    from loguru import logger
    logger.remove()
    log_filename = f"log-{time.strftime('%Y-%m-%d')}.txt"
    logger.add(log_filename, level="INFO", encoding="utf-8", backtrace=True, diagnose=True)
except Exception:
    import logging
    logger = logging.getLogger("api_quotex.constants")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Minimal asset mapping (extend as needed)
ASSETS: Dict[str, int] = {
    "EURUSD": 1,
    "XAUUSD": 2,
    "GBPUSD": 56,
    "USDJPY": 63,
    "BTCUSD": 352,
    "ETHUSD": 360,
}

def update_assets_from_api(api_assets: List[Dict[str, Any]]) -> None:
    global ASSETS
    new_assets: Dict[str, int] = {}
    for asset_data in api_assets:
        symbol = str(asset_data.get("symbol"))
        asset_id = int(asset_data.get("id", 0))
        if symbol and asset_id:
            new_assets[symbol] = asset_id
    ASSETS.update(new_assets)
    logger.info(f"Updated ASSETS dictionary with {len(new_assets)} assets")

class Regions:
    _REGIONS: Dict[str, str] = {
        "DEMO": "wss://ws2.qxbroker.com/socket.io/?EIO=3&transport=websocket",
        "LIVE": "wss://ws2.qxbroker.com/socket.io/?EIO=3&transport=websocket",
    }

    @classmethod
    def get_all_regions(cls) -> Dict[str, str]:
        return cls._REGIONS.copy()

    @classmethod
    def get_region(cls, region_name: str) -> Optional[str]:
        return cls._REGIONS.get(region_name.upper())

REGIONS = Regions()

TIMEFRAMES: Dict[str, int] = {
    "5s": 5,
    "15s": 15,
    "30s": 30,
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
    "1w": 604800,
}

CONNECTION_SETTINGS: Dict[str, float] = {
    "ping_interval": 25.0,
    "ping_timeout": 5.0,
    "close_timeout": 10.0,
    "max_reconnect_attempts": 5,
    "reconnect_initial_delay": 1.0,
    "reconnect_max_delay": 15.0,
    "reconnect_factor": 1.8,
    "handshake_timeout": 10.0,
    "receive_timeout": 30.0,
}

API_LIMITS: Dict[str, float] = {
    "min_order_amount": 1.0,
    "max_order_amount": 50000.0,
    "min_duration": 30,
    "max_duration": 14400,
    "max_concurrent_orders": 10,
    "rate_limit": 100,
}

DEFAULT_HEADERS: Dict[str, str] = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Origin": "https://qxbroker.com",
    "Referer": "https://qxbroker.com/",
    "Accept-Language": "en-US,en;q=0.9",
}