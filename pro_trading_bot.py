import asyncio
import numpy as np
import pandas as pd
import pandas_ta as ta  # Technical Analysis Library
from loguru import logger

# API imports
from api_quotex.client import AsyncQuotexClient
from api_quotex.login import get_ssid
from api_quotex.models import OrderDirection

# ==========================================
# 1. BOT SETTINGS (RISK MANAGEMENT)
# ==========================================
ASSET = "EURUSD_otc"
TRADE_AMOUNT = 1
DURATION = 60  # seconds
TARGET_PROFIT = 10
MAX_LOSS = -5


class ProTradingBot:
    def __init__(self):
        self.client: AsyncQuotexClient | None = None
        self.total_profit_loss = 0.0
        self.is_running = True

    # Accept either a pandas.DataFrame or raw list of candles
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

        # Ensure column names
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

        logger.info(f"Market Analysis -> RSI: {rsi_val:.2f} | EMA9: {ema_9:.5f} | EMA21: {ema_21:.5f}")

        if rsi_val < 30 and ema_9 > ema_21:
            return "CALL"
        elif rsi_val > 70 and ema_9 < ema_21:
            return "PUT"
        return "HOLD"

    async def execute_trade(self, direction: str):
        logger.info(f"🚀 Signal -> Placing order: {direction}")
        if not self.client:
            logger.error("Client not initialized")
            return

        try:
            odir = OrderDirection.CALL if direction == "CALL" else OrderDirection.PUT
            result = await self.client.place_order(asset=ASSET, amount=float(TRADE_AMOUNT), direction=odir, duration=int(DURATION))

            logger.success(f"Order placed: {getattr(result, 'order_id', getattr(result, 'request_id', 'unknown'))}")

            # Wait for result via check_win helper
            profit, status = await self.client.check_win(getattr(result, 'order_id', getattr(result, 'request_id', None)))
            if profit is not None:
                self.total_profit_loss += float(profit)
                logger.info(f"Trade result: {status} | Profit: ${profit:.2f} | Total P/L: ${self.total_profit_loss:.2f}")
            else:
                logger.warning("No trade result available (timed out or unknown)")

        except Exception as e:
            logger.error(f"Error placing order: {e}")

    async def start(self):
        # Retrieve/refresh SSID/session
        ok, session = await get_ssid()
        if not ok or not session.get("ssid"):
            logger.error("Could not obtain a valid SSID/session. Check credentials and sessions.")
            return

        ssid = session["ssid"]
        is_demo = bool(session.get("is_demo", True))

        # Initialize client
        self.client = AsyncQuotexClient(ssid=ssid, is_demo=is_demo)

        logger.info("Connecting to Quotex...")
        try:
            connected = await self.client.connect()
        except Exception as e:
            logger.error(f"Connection error: {e}")
            return

        if not connected:
            logger.error("Login/connection failed. Aborting.")
            return

        logger.success("✅ Connected. Bot is active.")

        try:
            while self.is_running:
                if self.total_profit_loss >= TARGET_PROFIT:
                    logger.success(f"Target profit reached: ${self.total_profit_loss:.2f}")
                    break
                if self.total_profit_loss <= MAX_LOSS:
                    logger.warning(f"Max loss hit: ${self.total_profit_loss:.2f}")
                    break

                # Fetch recent candles as DataFrame (1-minute timeframe)
                try:
                    df = await self.client.get_candles_dataframe(ASSET, 60, count=50)
                except Exception as e:
                    logger.error(f"Failed fetching candles: {e}")
                    df = pd.DataFrame()

                signal = self.calculate_signals(df)

                if signal == "CALL":
                    await self.execute_trade("CALL")
                elif signal == "PUT":
                    await self.execute_trade("PUT")
                else:
                    logger.info("No clear signal. Waiting...")

                await asyncio.sleep(5)

        finally:
            if self.client:
                await self.client.disconnect()


if __name__ == "__main__":
    bot = ProTradingBot()
    asyncio.run(bot.start())
