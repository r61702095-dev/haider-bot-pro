import argparse
import asyncio
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pandas_ta as ta  # Technical Analysis Library
from loguru import logger

from api_quotex.client import AsyncQuotexClient
from api_quotex.login import get_ssid
from api_quotex.models import OrderDirection

ROOT_DIR = Path(__file__).resolve().parent
BROKER_CONFIG_PATH = ROOT_DIR / "broker_config.json"
DEFAULT_ASSET = "EURUSD_otc"
DEFAULT_TRADE_AMOUNT = 1
DEFAULT_DURATION = 60
DEFAULT_TARGET_PROFIT = 10
DEFAULT_MAX_LOSS = -5

logger.remove()
logger.add(lambda msg: print(msg, end=""), level="INFO")


def load_broker_config() -> dict:
    if not BROKER_CONFIG_PATH.exists():
        return {}
    try:
        with BROKER_CONFIG_PATH.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        logger.warning(f"Unable to load broker_config.json: {e}")
        return {}


def build_ssid(source: str, is_demo: bool = True) -> str:
    if source.startswith("42["):
        return source
    return f'42["authorization",{{"session":"{source}","isDemo":{1 if is_demo else 0},"tournamentId":0}}]'


def get_config_session() -> tuple[str | None, bool]:
    cfg = load_broker_config().get("QUOTEX", {})
    if not isinstance(cfg, dict):
        return None, True

    ssid_source = cfg.get("ssid") or cfg.get("session") or cfg.get("token")
    is_demo = bool(cfg.get("is_demo", True))
    if ssid_source and isinstance(ssid_source, str):
        return build_ssid(ssid_source, is_demo), is_demo

    cookies = cfg.get("cookies") or cfg.get("cookie") or ""
    if isinstance(cookies, str) and cookies:
        parts = [p.strip() for p in cookies.split(";") if p.strip()]
        for part in parts:
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key.strip().lower() in ("session", "ssid", "qx_session"):
                return build_ssid(value.strip(), is_demo), is_demo

    return None, is_demo


class ProTradingBot:
    def __init__(self, asset: str, amount: float, duration: int, target_profit: float, max_loss: float):
        self.asset = asset
        self.amount = float(amount)
        self.duration = int(duration)
        self.target_profit = float(target_profit)
        self.max_loss = float(max_loss)
        self.client: AsyncQuotexClient | None = None
        self.total_profit_loss = 0.0
        self.is_running = True

    def calculate_signals(self, candles_data):
        if candles_data is None:
            return "HOLD"

        if isinstance(candles_data, pd.DataFrame):
            df = candles_data.copy()
        else:
            try:
                df = pd.DataFrame(candles_data)
            except Exception:
                return "HOLD"

        if df.empty or len(df) < 30:
            return "HOLD"

        if "close" not in df.columns:
            if "Close" in df.columns:
                df = df.rename(columns={"Close": "close"})
            else:
                return "HOLD"

        df["RSI"] = ta.rsi(df["close"], length=14)
        df["EMA_9"] = ta.ema(df["close"], length=9)
        df["EMA_21"] = ta.ema(df["close"], length=21)

        last_row = df.iloc[-1]
        rsi_val = float(last_row["RSI"])
        ema_9 = float(last_row["EMA_9"])
        ema_21 = float(last_row["EMA_21"])

        logger.info(f"Market Analysis -> RSI: {rsi_val:.2f} | EMA9: {ema_9:.5f} | EMA21: {ema_21:.5f}\n")

        if rsi_val < 30 and ema_9 > ema_21:
            return "CALL"
        if rsi_val > 70 and ema_9 < ema_21:
            return "PUT"
        return "HOLD"

    async def execute_trade(self, direction: str):
        logger.info(f"🚀 Signal -> Placing order: {direction}\n")
        if not self.client:
            logger.error("Client not initialized\n")
            return

        try:
            odir = OrderDirection.CALL if direction == "CALL" else OrderDirection.PUT
            result = await self.client.place_order(asset=self.asset, amount=self.amount, direction=odir, duration=self.duration)

            order_id = getattr(result, "order_id", getattr(result, "request_id", None))
            logger.success(f"Order placed: {order_id}\n")

            if order_id is None:
                logger.warning("Order did not return an ID. Skipping result check.\n")
                return

            profit, status = await self.client.check_win(order_id)
            if profit is not None:
                self.total_profit_loss += float(profit)
                logger.info(f"Trade result: {status} | Profit: ${profit:.2f} | Total P/L: ${self.total_profit_loss:.2f}\n")
            else:
                logger.warning("No trade result available (timed out or unknown)\n")

        except Exception as e:
            logger.error(f"Error placing order: {e}\n")

    async def start(self, demo: bool | None = None):
        ssid, is_demo = get_config_session()
        if ssid:
            logger.info("Using SSID from broker_config.json\n")
            if demo is not None:
                is_demo = demo
        else:
            logger.info("No SSID found in broker_config.json, loading session via api_quotex login\n")
            ok, session = await get_ssid()
            if not ok or not session.get("ssid"):
                logger.error("Could not obtain a valid SSID/session. Check sessions/config.json or broker_config.json.\n")
                return
            ssid = session["ssid"]
            is_demo = bool(session.get("is_demo", True)) if demo is None else demo

        self.client = AsyncQuotexClient(ssid=ssid, is_demo=is_demo)
        logger.info(f"Connecting to Quotex (demo={is_demo})...\n")

        try:
            connected = await self.client.connect()
        except Exception as e:
            logger.error(f"Connection error: {e}\n")
            return

        if not connected:
            logger.error("Login/connection failed. Aborting.\n")
            return

        logger.success("✅ Connected. Trading bot active.\n")

        try:
            while self.is_running:
                if self.total_profit_loss >= self.target_profit:
                    logger.success(f"Target profit reached: ${self.total_profit_loss:.2f}\n")
                    break
                if self.total_profit_loss <= self.max_loss:
                    logger.warning(f"Max loss hit: ${self.total_profit_loss:.2f}\n")
                    break

                try:
                    df = await self.client.get_candles_dataframe(self.asset, 60, count=50)
                except Exception as e:
                    logger.error(f"Failed fetching candles: {e}\n")
                    df = pd.DataFrame()

                signal = self.calculate_signals(df)
                logger.info(f"Generated signal: {signal}\n")

                if signal == "CALL":
                    await self.execute_trade("CALL")
                elif signal == "PUT":
                    await self.execute_trade("PUT")
                else:
                    logger.info("No clear signal. Waiting...\n")

                await asyncio.sleep(5)

        finally:
            if self.client:
                await self.client.disconnect()
                logger.info("Disconnected from Quotex.\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Quotex Pro Trading Bot")
    parser.add_argument("--asset", default=DEFAULT_ASSET, help="Asset symbol, e.g. EURUSD_otc")
    parser.add_argument("--amount", type=float, default=DEFAULT_TRADE_AMOUNT, help="Trade amount in USD")
    parser.add_argument("--duration", type=int, default=DEFAULT_DURATION, help="Trade duration in seconds")
    parser.add_argument("--target-profit", type=float, default=DEFAULT_TARGET_PROFIT, help="Daily target profit")
    parser.add_argument("--max-loss", type=float, default=DEFAULT_MAX_LOSS, help="Daily max loss")
    parser.add_argument("--demo", action="store_true", help="Force demo mode")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    bot = ProTradingBot(
        asset=args.asset,
        amount=args.amount,
        duration=args.duration,
        target_profit=args.target_profit,
        max_loss=args.max_loss,
    )
    asyncio.run(bot.start(demo=args.demo))
