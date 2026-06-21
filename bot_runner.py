import asyncio
import time
import os
import random

from api_quotex.client import AsyncQuotexClient, OrderDirection
from api_quotex.login import get_ssid, load_config
import app as main_app


def check_ai_signals():
    """Placeholder AI strategy — replace with your real model/logic.
    Returns "BUY" or "SELL".
    """
    # TODO: integrate real AI indicators (RSI/MACD/etc.) here
    return random.choice(["BUY", "SELL"])


async def start_bot(max_cycles: int = None, sleep_interval: int = 10):
    """Main async bot runner.

    - Tries to obtain an SSID via `get_ssid()` (uses `broker_config.json` or sessions).
    - Connects with `AsyncQuotexClient` when available.
    - Falls back to simulated orders (and logs them) when no live session.
    """
    ok, session = await get_ssid(is_demo=True)
    ssid = None
    is_demo = True
    if ok and isinstance(session, dict):
        ssid = session.get("ssid")
        is_demo = session.get("is_demo", True)

    client = None
    if ssid:
        try:
            client = AsyncQuotexClient(ssid=ssid, is_demo=is_demo)
            print("Quotex se connect ho raha hai...")
            connected = await client.connect()
            if not connected:
                print("Login Fail! Falling back to simulation.")
                client = None
            else:
                print("Login Successful! Bot active hai.")
        except Exception as e:
            print("Connection error:", e)
            client = None
    else:
        print("No SSID found — running in simulated mode.")

    cycles = 0
    try:
        while True:
            try:
                signal = check_ai_signals()
                print(f"AI Market Signal: {signal}")

                direction = OrderDirection.CALL if signal == "BUY" else OrderDirection.PUT

                if client:
                    try:
                        order = await client.place_order(asset="EURUSD", amount=1.0, direction=direction, duration="30s")
                        oid = getattr(order, "order_id", None) or str(order)
                        print("Order placed:", oid)
                        main_app.append_trade_log({
                            "broker": "Quotex",
                            "status": "placed",
                            "order_id": oid,
                            "signal": signal,
                            "asset": "EURUSD",
                            "amount": 1.0,
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        })
                    except Exception as e:
                        print("Order failed:", e)
                        main_app.append_trade_log({
                            "broker": "Quotex",
                            "status": "error",
                            "error": str(e),
                            "signal": signal,
                            "asset": "EURUSD",
                            "amount": 1.0,
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        })
                else:
                    # Simulation: create a fake order id and log it
                    oid = f"sim-{int(time.time() * 1000)}"
                    out = {
                        "broker": "Quotex",
                        "status": "simulated",
                        "order_id": oid,
                        "signal": signal,
                        "asset": "EURUSD",
                        "amount": 1.0,
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    }
                    print("Simulated order:", out)
                    main_app.append_trade_log(out)

                cycles += 1
                if max_cycles and cycles >= max_cycles:
                    print("Reached max cycles, stopping.")
                    break

                await asyncio.sleep(sleep_interval)

            except Exception as e:
                print(f"Bot runtime error: {e}")
                await asyncio.sleep(5)
    finally:
        if client:
            try:
                await client.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    # For quick testing set BOT_TEST_CYCLES env var (e.g. BOT_TEST_CYCLES=3)
    max_cycles_env = os.environ.get("BOT_TEST_CYCLES")
    max_cycles = int(max_cycles_env) if (max_cycles_env and max_cycles_env.isdigit()) else None
    try:
        asyncio.run(start_bot(max_cycles=max_cycles))
    except KeyboardInterrupt:
        print("Stopping bot")
