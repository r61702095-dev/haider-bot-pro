from flask import Flask, render_template, jsonify, request
import os
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
import json
import time
import hmac
import hashlib
import logging
from urllib.parse import urlencode
import asyncio

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Signal cache and history
SIGNAL_CACHE = {}  # (symbol, interval, limit) -> {'ts': float, 'df': DataFrame}
SIGNAL_HISTORY = []  # Last 50 signals
CACHE_EXPIRY = 30  # seconds

# Configuration
AUTH_TOKEN = os.environ.get('AUTH_TOKEN', 'your-secret-token')
QUOTEX_API_KEY = os.environ.get('QUOTEX_API_KEY', '')
QUOTEX_API_SECRET = os.environ.get('QUOTEX_API_SECRET', '')
QUOTEX_API_URL = os.environ.get('QUOTEX_API_URL', 'https://api.quotex.io/api')
BINANCE_API_KEY = os.environ.get('BINANCE_API_KEY', '')
BINANCE_API_SECRET = os.environ.get('BINANCE_API_SECRET', '')
BINANCE_API_URL = os.environ.get('BINANCE_API_URL', 'https://api.binance.com')

# Supported assets
FOREX_PAIRS = ['EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD', 'NZDUSD']
BINARY_ASSETS = ['BTCUSDT', 'ETHUSD', 'XAUUSD', 'USOUSD', 'SPX500']
CRYPTO_PAIRS = ['BTCUSDT', 'ETHUSD', 'LTCUSD', 'BCHUSD', 'XRPUSD']

# Broker configuration file (local fallback to environment variables)
BROKER_CONFIG_PATH = os.path.join(os.path.dirname(__file__), 'broker_config.json')

# Persistent trade log (newline-delimited JSON)
TRADE_LOG_PATH = os.path.join(os.path.dirname(__file__), 'trades_log.jsonl')

# Optional: lightweight integration with bundled `api_quotex` package (async client)
try:
    from api_quotex import AsyncQuotexClient, OrderDirection, get_ssid
    QUOTEX_CLIENT_AVAILABLE = True
except Exception as e:
    logger.warning('api_quotex package not available or missing deps: %s', e)
    AsyncQuotexClient = None
    OrderDirection = None
    get_ssid = None
    QUOTEX_CLIENT_AVAILABLE = False


def _parse_expiration_to_seconds(expiration: str) -> int:
    """Convert common expiration formats to seconds (e.g. '1M'->60, '30S'->30, '5m'->300)."""
    try:
        if isinstance(expiration, (int, float)):
            return int(expiration)
        s = str(expiration).strip()
        if s.isdigit():
            return int(s)
        if s.lower().endswith('ms'):
            return max(1, int(float(s[:-2]) / 1000))
        if s.lower().endswith('s'):
            return max(1, int(float(s[:-1])))
        if s.lower().endswith('m') or s.lower().endswith('min'):
            # 1M or 5m -> minutes
            num = ''.join(ch for ch in s if (ch.isdigit() or ch == '.'))
            return max(1, int(float(num) * 60))
        if s.lower().endswith('h'):
            num = ''.join(ch for ch in s if (ch.isdigit() or ch == '.'))
            return max(60, int(float(num) * 3600))
    except Exception:
        pass
    # default 60s
    return 60


def _sync_place_quotex_order_with_client(symbol, direction, amount, expiration='1M', is_demo=True):
    """Synchronous wrapper that uses `api_quotex.AsyncQuotexClient` to place an order.
    Requires `ssid` in BROKER_CONFIG['QUOTEX'] or credentials saved in the api_quotex Config.
    Falls back with an error message if client unavailable or connection fails.
    """
    async def _do():
        conf = BROKER_CONFIG.get('QUOTEX', {}) if isinstance(BROKER_CONFIG, dict) else {}
        # If cookies provided in config, try to extract session value
        def _extract_session_from_cookie_string(cookie_str: str):
            try:
                parts = [p.strip() for p in cookie_str.split(';') if p.strip()]
                for p in parts:
                    if '=' not in p:
                        continue
                    k, v = p.split('=', 1)
                    kn = k.strip().lower()
                    if kn in ('session', 'ssid', 'qx_session') and v.strip():
                        return v.strip()
            except Exception:
                return None
            return None
        # prefer explicit ssid/session/token
        ssid = conf.get('ssid') or conf.get('session') or conf.get('token') or os.environ.get('QUOTEX_SSID')
        if not ssid:
            ck = conf.get('cookies') or conf.get('cookie')
            if ck and isinstance(ck, str):
                s = _extract_session_from_cookie_string(ck)
                if s:
                    ssid = f'42["authorization",{{"session":"{s}","isDemo":{1 if is_demo else 0},"tournamentId":0}}]'
        # prefer explicit ssid/session/token
        ssid = conf.get('ssid') or conf.get('session') or conf.get('token') or os.environ.get('QUOTEX_SSID')
        # allow get_ssid via saved config if available
        if not ssid and get_ssid:
            try:
                ok, session_data = await get_ssid(is_demo=is_demo)
                if ok:
                    ssid = session_data.get('ssid') or session_data.get('token')
            except Exception:
                ssid = None

        if not ssid:
            raise RuntimeError('No SSID/session available for Quotex client; set broker_config.json QUOTEX.ssid or provide saved credentials')

        client = AsyncQuotexClient(ssid=ssid, is_demo=is_demo, persistent_connection=False)
        try:
            connected = await client.connect()
            if not connected:
                raise RuntimeError('Failed to connect to Quotex via client')

            # Map direction
            dir_enum = None
            try:
                dir_enum = OrderDirection.CALL if str(direction).upper() in ('BUY', 'CALL') else OrderDirection.PUT
            except Exception:
                # fallback string
                dir_enum = OrderDirection.CALL if str(direction).upper() in ('BUY', 'CALL') else OrderDirection.PUT

            dur = _parse_expiration_to_seconds(expiration)
            order = await client.place_order(asset=symbol, amount=float(amount), direction=dir_enum, duration=dur)
            # Return minimal standardized response
            out = {
                'status': 'executed',
                'broker': 'Quotex',
                'order_id': getattr(order, 'order_id', None),
                'raw': order
            }
            return out
        finally:
            try:
                await client.disconnect()
            except Exception:
                pass

    try:
        return asyncio.run(_do())
    except Exception as e:
        logger.exception('Quotex client error')
        return {'status': 'error', 'broker': 'Quotex', 'message': str(e)}


def append_trade_log(entry: dict):
    try:
        # Ensure timestamp present
        if 'timestamp' not in entry:
            entry['timestamp'] = datetime.utcnow().isoformat() + 'Z'
        with open(TRADE_LOG_PATH, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, default=str) + "\n")
        return True
    except Exception as e:
        logger.error('Failed to append trade log: %s', e)
        return False


def read_trade_log(limit: int = 200):
    try:
        if not os.path.exists(TRADE_LOG_PATH):
            return []
        with open(TRADE_LOG_PATH, 'r', encoding='utf-8') as f:
            lines = [l.strip() for l in f.readlines() if l.strip()]
        trades = []
        for l in lines:
            try:
                trades.append(json.loads(l))
            except Exception:
                # skip malformed
                continue
        return trades[-limit:]
    except Exception as e:
        logger.error('Failed to read trade log: %s', e)
        return []


def load_broker_config():
    try:
        if os.path.exists(BROKER_CONFIG_PATH):
            with open(BROKER_CONFIG_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.warning('Failed reading broker_config.json: %s', e)
    return {}


def simulation_allowed():
    """Return whether simulation fallback is allowed (configurable via broker_config.json)."""
    try:
        return bool(BROKER_CONFIG.get('allow_simulation', True))
    except Exception:
        return True


def save_broker_config(cfg: dict):
    try:
        with open(BROKER_CONFIG_PATH, 'w', encoding='utf-8') as f:
            json.dump(cfg, f, indent=2)
        return True
    except Exception as e:
        logger.error('Failed saving broker_config.json: %s', e)
        return False


# load at startup
BROKER_CONFIG = load_broker_config()


def fetch_klines(symbol, interval='1m', limit=200, force_refresh=False):
    """Fetch candlestick data from Binance or alternative source with in-memory cache."""
    try:
        # Map intervals
        interval_map = {
            '1m': '1m', '5m': '5m', '15m': '15m', '30m': '30m',
            '1h': '1h', '4h': '4h', '1d': '1d', '1w': '1w'
        }
        mapped_interval = interval_map.get(interval, '1m')

        cache_key = (symbol.upper(), mapped_interval, int(limit))
        now_ts = time.time()
        # Return cached DF if fresh
        if not force_refresh and cache_key in SIGNAL_CACHE:
            entry = SIGNAL_CACHE.get(cache_key)
            if entry and (now_ts - entry.get('ts', 0)) < CACHE_EXPIRY:
                return entry.get('df')

        # Binance API call
        url = f"https://api.binance.com/api/v3/klines"
        params = {
            'symbol': symbol.upper(),
            'interval': mapped_interval,
            'limit': min(limit, 1000)
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if not data:
            logger.warning(f"No data returned for {symbol} {interval}")
            return None

        # Parse into DataFrame
        df = pd.DataFrame(data, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'trades', 'taker_buy_base',
            'taker_buy_quote', 'ignore'
        ])

        # Convert to proper types
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df['timestamp'] = pd.to_datetime(df['open_time'], unit='ms')
        df = df[numeric_cols + ['timestamp']].dropna()

        if df is None or len(df) == 0:
            return None

        # Cache the DataFrame for short period
        try:
            SIGNAL_CACHE[cache_key] = {'ts': now_ts, 'df': df.copy()}
        except Exception:
            pass

        return df

    except Exception as e:
        logger.error(f"Error fetching klines for {symbol}: {str(e)}")
        return None


def calculate_indicators(df, fast=False):
    """Calculate technical indicators. If fast=True, use shorter windows for quicker signals."""
    df = df.copy()

    try:
        # Choose windows depending on fast flag
        if fast:
            sma_windows = {'sma9': 5, 'sma20': 10, 'sma50': 20, 'sma200': 50}
            ema_span_short = 6
            ema_span_long = 13
            rsi_window = 7
            macd_signal_span = 7
            bb_window = 10
            atr_window = 7
            vol_ma_window = 10
        else:
            sma_windows = {'sma9': 9, 'sma20': 20, 'sma50': 50, 'sma200': 200}
            ema_span_short = 12
            ema_span_long = 26
            rsi_window = 14
            macd_signal_span = 9
            bb_window = 20
            atr_window = 14
            vol_ma_window = 20

        # Moving Averages (keep column names expected elsewhere)
        df['sma9'] = df['close'].rolling(sma_windows['sma9']).mean()
        df['sma20'] = df['close'].rolling(sma_windows['sma20']).mean()
        df['sma50'] = df['close'].rolling(sma_windows['sma50']).mean()
        df['sma200'] = df['close'].rolling(sma_windows['sma200']).mean()

        df['ema12'] = df['close'].ewm(span=ema_span_short, adjust=False).mean()
        df['ema26'] = df['close'].ewm(span=ema_span_long, adjust=False).mean()

        # RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=rsi_window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=rsi_window).mean()
        rs = gain / loss
        df['rsi'] = 100 - (100 / (1 + rs))

        # MACD
        df['macd'] = df['ema12'] - df['ema26']
        df['macd_signal'] = df['macd'].ewm(span=macd_signal_span, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']

        # Bollinger Bands
        df['bb_middle'] = df['close'].rolling(bb_window).mean()
        df['bb_std'] = df['close'].rolling(bb_window).std()
        df['bb_upper'] = df['bb_middle'] + (df['bb_std'] * 2)
        df['bb_lower'] = df['bb_middle'] - (df['bb_std'] * 2)

        # ATR (Volatility)
        df['tr1'] = df['high'] - df['low']
        df['tr2'] = abs(df['high'] - df['close'].shift(1))
        df['tr3'] = abs(df['low'] - df['close'].shift(1))
        df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
        df['atr'] = df['tr'].rolling(atr_window).mean()

        # Volume MA
        df['volume_ma'] = df['volume'].rolling(vol_ma_window).mean()

        # Stochastic RSI (momentum confirmation built from RSI)
        try:
            stoch_window = rsi_window
            df['rsi_min'] = df['rsi'].rolling(stoch_window).min()
            df['rsi_max'] = df['rsi'].rolling(stoch_window).max()
            df['stoch_k'] = ((df['rsi'] - df['rsi_min']) / (df['rsi_max'] - df['rsi_min']).replace(0, np.nan)) * 100
            df['stoch_d'] = df['stoch_k'].rolling(3).mean()
        except Exception:
            df['stoch_k'] = np.nan
            df['stoch_d'] = np.nan

        # ADX (trend strength) - Wilder smoothing approximated with EWM
        try:
            up = df['high'].diff()
            down = -df['low'].diff()
            plus_dm = np.where((up > down) & (up > 0), up, 0.0)
            minus_dm = np.where((down > up) & (down > 0), down, 0.0)
            plus_dm = pd.Series(plus_dm, index=df.index)
            minus_dm = pd.Series(minus_dm, index=df.index)
            smoothed_tr = df['tr'].ewm(alpha=1/atr_window, adjust=False).mean()
            smoothed_plus = plus_dm.ewm(alpha=1/atr_window, adjust=False).mean()
            smoothed_minus = minus_dm.ewm(alpha=1/atr_window, adjust=False).mean()
            plus_di = 100 * (smoothed_plus / smoothed_tr.replace(0, np.nan))
            minus_di = 100 * (smoothed_minus / smoothed_tr.replace(0, np.nan))
            dx = (np.abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, np.nan)) * 100
            df['adx'] = dx.ewm(alpha=1/atr_window, adjust=False).mean()
        except Exception:
            df['adx'] = np.nan

        # cleanup small helper columns
        try:
            df.drop(columns=['tr1', 'tr2', 'tr3', 'rsi_min', 'rsi_max'], inplace=True, errors='ignore')
        except Exception:
            pass

        return df

    except Exception as e:
        logger.error(f"Error calculating indicators: {str(e)}")
        return None


def generate_signal(symbol, interval='1m', asset_type='CRYPTO', fast=False, duplicate_window=None, force_refresh=False):
    """Generate trading signal with optional fast mode and adjustable duplicate suppression."""
    try:
        # choose data limit based on fast mode
        limit = 200 if not fast else 100
        df = fetch_klines(symbol, interval=interval, limit=limit, force_refresh=force_refresh)
        if df is None or len(df) < 20:
            return {
                'signal': 'WAIT',
                'confidence': 0,
                'reason': 'Insufficient data',
                'symbol': symbol,
                'timeframe': interval,
                'price': 0,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }

        # Calculate indicators (fast uses shorter windows)
        df = calculate_indicators(df, fast=fast)
        if df is None:
            return {
                'signal': 'WAIT',
                'confidence': 0,
                'reason': 'Indicator calculation failed',
                'symbol': symbol,
                'timeframe': interval,
                'price': 0,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            }

        last = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else last

        buy_signals = 0.0
        sell_signals = 0.0
        total_signals = 0.0

        # Trend (SMA stacking)
        if last['close'] > last['sma20'] > last['sma50'] > last['sma200']:
            buy_signals += 2
        elif last['close'] < last['sma20'] < last['sma50'] < last['sma200']:
            sell_signals += 2
        total_signals += 2

        # RSI
        if 30 <= last['rsi'] <= 40:
            buy_signals += 1.5
        elif 60 <= last['rsi'] <= 70:
            sell_signals += 1.5
        if last['rsi'] < 30:
            buy_signals += 2
        elif last['rsi'] > 70:
            sell_signals += 2
        total_signals += 2

        # MACD
        if last['macd'] > last['macd_signal'] and prev['macd'] <= prev['macd_signal']:
            buy_signals += 1.5
        elif last['macd'] < last['macd_signal'] and prev['macd'] >= prev['macd_signal']:
            sell_signals += 1.5
        if last['macd_hist'] > 0:
            buy_signals += 0.5
        else:
            sell_signals += 0.5
        total_signals += 2

        # Bollinger
        if last['close'] < last['bb_lower']:
            buy_signals += 1.5
        elif last['close'] > last['bb_upper']:
            sell_signals += 1.5
        if last['close'] > last['bb_middle']:
            buy_signals += 0.5
        else:
            sell_signals += 0.5
        total_signals += 2

        # Volume
        try:
            if last['volume'] > last['volume_ma'] * 1.2:
                if last['close'] > prev['close']:
                    buy_signals += 1
                else:
                    sell_signals += 1
        except Exception:
            pass
        total_signals += 1

        # Price action (candle body)
        candle_size = 0
        try:
            open_price = last.get('open', last.get('close', 0))
            candle_size = (last['close'] - open_price) / last['close'] * 100 if open_price else 0
        except Exception:
            pass
        if candle_size > 0.5:
            buy_signals += 1
        elif candle_size < -0.5:
            sell_signals += 1
        total_signals += 1

        # Stochastic RSI (momentum confirmation)
        try:
            sk = last.get('stoch_k')
            sd = last.get('stoch_d')
            if sk is not None and not np.isnan(sk):
                if sk < 20 and sk > sd:
                    buy_signals += 1.5
                elif sk > 80 and sk < sd:
                    sell_signals += 1.5
        except Exception:
            pass
        total_signals += 1

        # ADX trend strength: increase weight if clear trend
        try:
            adx_val = last.get('adx')
            if adx_val is not None and not np.isnan(adx_val):
                if adx_val >= 25:
                    if last['close'] > last['sma20'] > last['sma50'] > last['sma200']:
                        buy_signals += 1
                    elif last['close'] < last['sma20'] < last['sma50'] < last['sma200']:
                        sell_signals += 1
                elif adx_val < 20:
                    if last.get('rsi', 50) < 50:
                        buy_signals += 0.5
                    else:
                        sell_signals += 0.5
        except Exception:
            pass
        total_signals += 1

        buy_score = (buy_signals / total_signals) * 100 if total_signals > 0 else 50
        sell_score = (sell_signals / total_signals) * 100 if total_signals > 0 else 50

        signal = 'WAIT'
        confidence = 50
        if buy_score > 65 and buy_score > sell_score:
            signal = 'BUY'
            confidence = min(95, int(buy_score))
        elif sell_score > 65 and sell_score > buy_score:
            signal = 'SELL'
            confidence = min(95, int(sell_score))
        else:
            confidence = int(max(buy_score, sell_score))

        # Multi-timeframe confirmation
        mtf_bonus = 0
        try:
            if interval != '1d':
                df_h = fetch_klines(symbol, interval='1d', limit=200)
                if df_h is not None:
                    df_h = calculate_indicators(df_h, fast=False)
                    last_h = df_h.iloc[-1]
                    if last['close'] > last_h.get('sma200', 0):
                        mtf_bonus += 10
                    elif last['close'] < last_h.get('sma200', 0):
                        mtf_bonus -= 10
        except Exception:
            pass

        # Volatility penalty
        try:
            if last['atr'] / last['close'] * 100 > 5:
                confidence = int(confidence * 0.85)
        except Exception:
            pass

        confidence = int(max(0, min(100, (confidence or 0) + mtf_bonus)))

        # Default reason
        reason = f"Signal generated from {int(total_signals)} indicators (Buy: {buy_score:.1f}%, Sell: {sell_score:.1f}%)"

        # Duplicate suppression window
        if duplicate_window is None:
            duplicate_window = 20 if fast else 60
        try:
            if SIGNAL_HISTORY:
                # check recent history (up to 3) for duplicate signals
                recent_same = [s for s in SIGNAL_HISTORY[:3] if s.get('symbol') == symbol and s.get('timeframe') == interval and s.get('signal') == signal]
                if recent_same:
                    last_ts = datetime.fromisoformat(recent_same[0].get('timestamp').replace('Z', ''))
                    if (datetime.utcnow() - last_ts).total_seconds() < duplicate_window:
                        signal = 'WAIT'
                        confidence = 0
                        reason = 'Duplicate recent signal suppressed.'
                else:
                    reason = f"Signal generated from {int(total_signals)} indicators (Buy: {buy_score:.1f}%, Sell: {sell_score:.1f}%)"
        except Exception:
            reason = f"Signal generated from {int(total_signals)} indicators (Buy: {buy_score:.1f}%, Sell: {sell_score:.1f}%)"

        payload = {
            'signal': signal,
            'confidence': confidence,
            'symbol': symbol,
            'timeframe': interval,
            'price': float(round(last['close'], 8)),
            'rsi': float(round(last.get('rsi', 0), 2)),
            'macd': float(round(last.get('macd', 0), 6)),
            'macd_signal': float(round(last.get('macd_signal', 0), 6)),
            'bb_upper': float(round(last.get('bb_upper', 0), 8)),
            'bb_middle': float(round(last.get('bb_middle', 0), 8)),
            'bb_lower': float(round(last.get('bb_lower', 0), 8)),
            'atr': float(round(last.get('atr', 0), 8)),
            'volume': float(round(last.get('volume', 0), 2)),
            'sma20': float(round(last.get('sma20', 0), 8)),
            'sma50': float(round(last.get('sma50', 0), 8)),
            'sma200': float(round(last.get('sma200', 0), 8)),
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'reason': reason
        }

        # Update history
        try:
            SIGNAL_HISTORY.insert(0, {
                'symbol': payload.get('symbol', symbol),
                'timeframe': interval,
                'signal': payload.get('signal'),
                'confidence': payload.get('confidence'),
                'timestamp': payload.get('timestamp')
            })
            while len(SIGNAL_HISTORY) > 50:
                SIGNAL_HISTORY.pop()
        except Exception:
            pass

        return payload

    except Exception as e:
        logger.error(f"Error generating signal: {str(e)}")
        return {
            'signal': 'WAIT',
            'confidence': 0,
            'reason': f'Error: {str(e)}',
            'symbol': symbol,
            'timeframe': interval,
            'price': 0,
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        }


def execute_quotex_trade(symbol, direction, amount, expiration='1M', stop_loss=None, take_profit=None, leverage=None, trade_type='binary'):
    """Execute trade on Quotex. Falls back to simulated when not configured."""
    try:
        # Prefer explicit env vars, else fallback to local broker_config.json
        conf = BROKER_CONFIG.get('QUOTEX', {}) if isinstance(BROKER_CONFIG, dict) else {}
        api_key = QUOTEX_API_KEY or conf.get('key') or conf.get('api_key')
        api_secret = QUOTEX_API_SECRET or conf.get('secret') or conf.get('api_secret')
        api_url = QUOTEX_API_URL or conf.get('url')

        # If the bundled async client is available and the config indicates to use it (or an ssid/session is present), prefer it.
        try:
            use_client = bool(conf.get('use_client') or conf.get('ssid') or conf.get('session') or conf.get('token'))
        except Exception:
            use_client = False
        if QUOTEX_CLIENT_AVAILABLE and use_client:
            # call the synchronous wrapper that runs the async client
            return _sync_place_quotex_order_with_client(symbol, direction, amount, expiration=expiration, is_demo=conf.get('is_demo', True))

        # If not configured, either simulate (if allowed) or return error
        if not api_key or not api_url:
            if simulation_allowed():
                return {
                    'status': 'simulated',
                    'broker': 'Quotex',
                    'note': 'Quotex not configured (env or broker_config.json), simulated execution'
                }
            return {'status': 'error', 'broker': 'Quotex', 'message': 'Quotex not configured and simulation disabled'}

        # Normalize direction
        dir_upper = direction.upper()
        if dir_upper in ('BUY', 'CALL'):
            direction_payload = 'CALL'
        elif dir_upper in ('SELL', 'PUT'):
            direction_payload = 'PUT'
        else:
            direction_payload = dir_upper

        # Prepare payload (include optional risk params)
        payload = {
            'symbol': symbol,
            'direction': direction_payload,
            'amount': float(amount),
            'expiration': expiration,
            'timestamp': int(time.time() * 1000),
            'trade_type': trade_type
        }
        if stop_loss is not None:
            payload['stop_loss'] = stop_loss
        if take_profit is not None:
            payload['take_profit'] = take_profit
        if leverage is not None:
            payload['leverage'] = leverage

        # Sign request when secret present
        headers = {'Content-Type': 'application/json'}
        if api_key:
            headers['Authorization'] = f'Bearer {api_key}'
        if api_secret:
            msg = json.dumps(payload, separators=(',', ':'), sort_keys=True).encode()
            sig = hmac.new(api_secret.encode(), msg, hashlib.sha256).hexdigest()
            headers['X-Signature'] = sig

        # Execute trade
        url = api_url.rstrip('/') + '/trade'
        r = requests.post(url, json=payload, headers=headers, timeout=12)
        r.raise_for_status()
        try:
            resp_json = r.json()
        except Exception:
            resp_json = {'text': r.text}
        return {'status': 'executed', 'broker': 'Quotex', 'response': resp_json}

    except Exception as e:
        logger.exception('Quotex trade error')
        return {'status': 'error', 'broker': 'Quotex', 'message': str(e)}


def execute_binance_trade(symbol, direction, amount, stop_loss=None, take_profit=None, leverage=None, trade_type='spot'):
    """Execute a market order on Binance spot using quoteOrderQty for BUY or quantity for SELL.
    'amount' is treated as quote currency amount (e.g., USDT) when possible.
    Requires BINANCE API key and secret present in env or broker_config.json under 'BINANCE'.
    """
    try:
        conf = BROKER_CONFIG.get('BINANCE', {}) if isinstance(BROKER_CONFIG, dict) else {}
        api_key = BINANCE_API_KEY or conf.get('key') or conf.get('api_key')
        api_secret = BINANCE_API_SECRET or conf.get('secret') or conf.get('api_secret')
        api_url = BINANCE_API_URL or conf.get('url') or 'https://api.binance.com'

        if not api_key or not api_secret:
            return {'status': 'error', 'broker': 'Binance', 'message': 'Binance API credentials not configured'}

        side = 'BUY' if direction.upper() in ('BUY', 'CALL') else 'SELL'

        # Use latest price when needed
        price = None
        try:
            df = fetch_klines(symbol, interval='1m', limit=1, force_refresh=True)
            if df is not None and len(df) >= 1:
                price = float(df.iloc[-1]['close'])
        except Exception:
            price = None

        params = {
            'symbol': symbol.upper(),
            'side': side,
            'type': 'MARKET',
            'timestamp': int(time.time() * 1000)
        }

        # For BUY we can use quoteOrderQty to spend 'amount' in quote asset (e.g., USDT)
        if side == 'BUY':
            params['quoteOrderQty'] = str(amount)
        else:
            # For SELL, compute quantity from amount/price if price available
            if price is None:
                # fallback: require quantity instead of amount
                return {'status': 'error', 'broker': 'Binance', 'message': 'Price unavailable to compute quantity for SELL'}
            qty = float(amount) / float(price) if float(price) > 0 else 0
            # Binance requires quantity with correct precision; round to 6 decimals
            params['quantity'] = f"{qty:.6f}"

        # Sign params
        query = urlencode(params)
        signature = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()
        signed_query = f"{query}&signature={signature}"

        headers = {'X-MBX-APIKEY': api_key}
        url = api_url.rstrip('/') + '/api/v3/order'
        r = requests.post(url + '?' + signed_query, headers=headers, timeout=15)
        r.raise_for_status()
        try:
            resp = r.json()
        except Exception:
            resp = {'text': r.text}

        # Note: we do not automatically place OCO (TP/SL) orders here; that requires extra logic.
        return {'status': 'executed', 'broker': 'Binance', 'response': resp}

    except Exception as e:
        logger.exception('Binance trade error')
        return {'status': 'error', 'broker': 'Binance', 'message': str(e)}


def execute_simulated_trade(symbol, direction, amount, stop_loss=None, take_profit=None, leverage=None, trade_type='spot'):
    """Simulated trade execution for testing. Returns a paper-trade-like response."""
    price = None
    try:
        df = fetch_klines(symbol, interval='1m', limit=1)
        if df is not None and len(df) >= 1:
            price = float(df.iloc[-1]['close'])
    except Exception:
        price = None

    return {
        'status': 'simulated',
        'broker': 'Simulated',
        'symbol': symbol,
        'direction': direction,
        'amount': amount,
        'trade_type': trade_type,
        'leverage': leverage,
        'stop_loss': stop_loss,
        'take_profit': take_profit,
        'price': price,
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'message': 'Trade executed in simulation (paper) mode'
    }


@app.route('/')
def index():
    return render_template('index.html', auth_token=AUTH_TOKEN)


@app.route('/api/signal', methods=['GET'])
def get_signal_api():
    """Get trading signal for a symbol."""
    try:
        symbol = request.args.get('symbol', 'BTCUSDT').upper()
        timeframe = request.args.get('timeframe', '1m')
        fast_flag = str(request.args.get('fast', 'false')).lower() in ('1', 'true', 'yes', 'y')
        # live param forces fresh klines (bypass in-memory cache) for more real-time signals
        live_flag = str(request.args.get('live', 'false')).lower() in ('1', 'true', 'yes', 'y')
        # default to live for 1m timeframe unless explicitly disabled
        force_refresh = live_flag or (timeframe == '1m')
        
        # Generate signal (support fast mode and optional force_refresh)
        signal_data = generate_signal(symbol, timeframe, fast=fast_flag, force_refresh=force_refresh)
        signal_data['history'] = SIGNAL_HISTORY
        return jsonify(signal_data)
        
    except Exception as e:
        logger.error(f"API error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/trade', methods=['POST'])
def execute_trade():
    """Execute a trade."""
    try:
        # Verify auth token
        auth_header = request.headers.get('Authorization', '')
        if auth_header != f'Bearer {AUTH_TOKEN}':
            return jsonify({'error': 'Unauthorized'}), 401
        
        data = request.get_json() or {}
        symbol = data.get('symbol', '').upper()
        direction = data.get('direction', '').upper()  # BUY/SELL or CALL/PUT
        amount = float(data.get('amount', 10))
        broker = data.get('broker', 'Simulated').lower()
        # Optional risk params
        stop_loss = data.get('stop_loss')
        take_profit = data.get('take_profit')
        leverage = data.get('leverage') or data.get('margin')
        trade_type = data.get('trade_type') or ('spot')
        expiration = data.get('expiration', '1M')

        # normalize numeric inputs when provided
        try:
            if stop_loss is not None:
                stop_loss = float(stop_loss)
        except Exception:
            stop_loss = None
        try:
            if take_profit is not None:
                take_profit = float(take_profit)
        except Exception:
            take_profit = None
        try:
            if leverage is not None:
                leverage = float(leverage)
        except Exception:
            leverage = None
        
        if not symbol or direction not in ['BUY', 'SELL', 'CALL', 'PUT']:
            return jsonify({'error': 'Invalid symbol or direction'}), 400
        
        # Execute based on broker
        if broker == 'binance':
            result = execute_binance_trade(symbol, direction, amount, stop_loss=stop_loss, take_profit=take_profit, leverage=leverage, trade_type=trade_type)
        elif broker == 'quotex':
            result = execute_quotex_trade(symbol, direction, amount, expiration=expiration, stop_loss=stop_loss, take_profit=take_profit, leverage=leverage, trade_type=trade_type)
        else:
            result = execute_simulated_trade(symbol, direction, amount, stop_loss=stop_loss, take_profit=take_profit, leverage=leverage, trade_type=trade_type)
        # Persist manual trade
        try:
            entry = {
                'timestamp': datetime.utcnow().isoformat() + 'Z',
                'symbol': symbol,
                'direction': direction,
                'broker': broker,
                'amount': amount,
                'trade_type': trade_type,
                'stop_loss': stop_loss,
                'take_profit': take_profit,
                'leverage': leverage,
                'result': result
            }
            append_trade_log(entry)
        except Exception:
            pass

        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Trade execution error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/assets', methods=['GET'])
def get_assets():
    """Get available trading assets."""
    return jsonify({
        'forex': FOREX_PAIRS,
        'binary': BINARY_ASSETS,
        'crypto': CRYPTO_PAIRS
    })


@app.route('/api/health', methods=['GET'])
def health():
    """Health check."""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    })
# Broker config endpoints (get/save/test)


@app.route('/api/broker/config', methods=['GET'])
def api_broker_get_config():
    """Return current broker configuration (from broker_config.json)."""
    # auth optional for GET; keep it simple but can require auth if desired
    return jsonify(BROKER_CONFIG)


@app.route('/api/broker/config', methods=['POST'])
def api_broker_save_config():
    """Save broker configuration (requires auth)."""
    if not _check_auth_header(request):
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json() or {}
    # merge into existing
    BROKER_CONFIG.update(data)
    ok = save_broker_config(BROKER_CONFIG)
    return jsonify({'ok': ok, 'config': BROKER_CONFIG})


@app.route('/api/broker/test', methods=['POST'])
def api_broker_test():
    """Test broker connectivity using provided or saved config (requires auth)."""
    if not _check_auth_header(request):
        return jsonify({'error': 'Unauthorized'}), 401
    payload = request.get_json() or {}
    broker = (payload.get('broker') or 'QUOTEX').upper()
    conf = BROKER_CONFIG.get(broker, {}) if BROKER_CONFIG else {}
    # override with payload values
    conf = {**conf, **payload.get('config', {})}
    url = conf.get('url')
    key = conf.get('key') or conf.get('api_key')
    if not url:
        return jsonify({'ok': False, 'error': 'no_url'}), 400
    try:
        headers = {'Content-Type': 'application/json'}
        if key:
            headers['Authorization'] = f'Bearer {key}'
        r = requests.post(url, json={'ping': True}, headers=headers, timeout=8)
        return jsonify({'ok': True, 'status': r.status_code, 'text': r.text})
    except Exception as e:
        return jsonify({'ok': False, 'error': str(e)}), 500


# --- Auto-Trader: background worker + control endpoints ---
import threading
from collections import deque
import traceback

# Auto-trader state
AUTO_TRADER = {
    'thread': None,
    'running': False,
    'config': {},
    'trades': [],  # recent executed trades (most recent first)
    'trade_timestamps': deque(),  # datetime objects for rate limiting
    'lock': threading.Lock()
}

DEFAULT_MIN_CONFIDENCE = int(os.environ.get('MIN_CONFIDENCE', '75'))
DEFAULT_MAX_TRADES_PER_HOUR = int(os.environ.get('MAX_TRADES_PER_HOUR', '8'))
TRADE_COOLDOWN_SECONDS = int(os.environ.get('TRADE_COOLDOWN_SECONDS', '60'))


def timeframe_to_seconds(tf):
    mapping = {'1m': 60, '5m': 300, '15m': 900, '30m': 1800, '1h': 3600, '4h': 14400, '1d': 86400}
    return mapping.get(tf, 60)


def _check_auth_header(req):
    provided = req.headers.get('Authorization', '')
    return provided == f'Bearer {AUTH_TOKEN}'


def start_auto_trader(config):
    with AUTO_TRADER['lock']:
        if AUTO_TRADER['running']:
            return False, 'already_running'
        AUTO_TRADER['config'] = config.copy()
        AUTO_TRADER['trades'] = []
        AUTO_TRADER['trade_timestamps'] = deque()
        AUTO_TRADER['running'] = True

    def worker():
        logger.info('Auto-trader worker started with config: %s', config)
        while True:
            with AUTO_TRADER['lock']:
                if not AUTO_TRADER['running']:
                    break
                cfg = AUTO_TRADER['config'].copy()

            try:
                symbol = cfg.get('symbol', 'BTCUSDT')
                timeframe = cfg.get('timeframe', '1m')
                broker = cfg.get('broker', 'Simulated')
                amount = float(cfg.get('amount', 10))
                min_conf = int(cfg.get('min_confidence', DEFAULT_MIN_CONFIDENCE))
                max_per_hour = int(cfg.get('max_trades_per_hour', DEFAULT_MAX_TRADES_PER_HOUR))
                cooldown = int(cfg.get('cooldown', TRADE_COOLDOWN_SECONDS))
                # Optional trade params
                stop_loss = cfg.get('stop_loss')
                take_profit = cfg.get('take_profit')
                leverage = cfg.get('leverage')
                trade_type = cfg.get('trade_type', 'spot')
                expiration = cfg.get('expiration', '1M')
                fast_mode = bool(cfg.get('fast', False))

                # Keep auto-trader conservative — do not force refresh (rate-limit safety)
                sig = generate_signal(symbol, timeframe, fast=fast_mode, force_refresh=False)
                now = datetime.utcnow()

                # Purge old timestamps for the sliding 1-hour window
                with AUTO_TRADER['lock']:
                    while AUTO_TRADER['trade_timestamps'] and (now - AUTO_TRADER['trade_timestamps'][0]).total_seconds() > 3600:
                        AUTO_TRADER['trade_timestamps'].popleft()
                    trades_last_hour = len(AUTO_TRADER['trade_timestamps'])
                    last_trade_time = AUTO_TRADER['trade_timestamps'][-1] if AUTO_TRADER['trade_timestamps'] else None

                cooldown_ok = True
                if last_trade_time and (now - last_trade_time).total_seconds() < cooldown:
                    cooldown_ok = False

                # Decision: execute trade when signal strong enough and rate limits allow
                if sig['signal'] in ('BUY', 'SELL') and sig['confidence'] >= min_conf and trades_last_hour < max_per_hour and cooldown_ok:
                    # Map to broker-specific directions
                    if broker.lower() == 'binance':
                        # Binance uses BUY/SELL market orders
                        res = execute_binance_trade(symbol, sig['signal'], amount, stop_loss=stop_loss, take_profit=take_profit, leverage=leverage, trade_type=trade_type)
                    elif broker.lower() == 'quotex':
                        direction = 'CALL' if sig['signal'] == 'BUY' else 'PUT'
                        res = execute_quotex_trade(symbol, direction, amount, expiration=expiration, stop_loss=stop_loss, take_profit=take_profit, leverage=leverage, trade_type=trade_type)
                    else:
                        res = execute_simulated_trade(symbol, sig['signal'], amount, stop_loss=stop_loss, take_profit=take_profit, leverage=leverage, trade_type=trade_type)

                    entry = {
                        'timestamp': now.isoformat() + 'Z',
                        'symbol': symbol,
                        'timeframe': timeframe,
                        'signal': sig['signal'],
                        'confidence': sig['confidence'],
                        'broker': broker,
                        'amount': amount,
                        'stop_loss': stop_loss,
                        'take_profit': take_profit,
                        'leverage': leverage,
                        'trade_type': trade_type,
                        'result': res
                    }
                    with AUTO_TRADER['lock']:
                        AUTO_TRADER['trades'].insert(0, entry)
                        AUTO_TRADER['trade_timestamps'].append(now)
                        # keep history small
                        if len(AUTO_TRADER['trades']) > 200:
                            AUTO_TRADER['trades'].pop()
                    # Persist trade to file
                    try:
                        append_trade_log(entry)
                    except Exception:
                        pass
                    logger.info('Auto-trader executed trade: %s %s', sig['signal'], symbol)

                # Compute next sleep: prefer explicit check_interval, otherwise a fraction of timeframe
                check_interval = int(cfg.get('check_interval', max(5, timeframe_to_seconds(timeframe) // 6)))
                # Cap to sensible bounds
                if check_interval < 5:
                    check_interval = 5
                if check_interval > 3600:
                    check_interval = 3600

                # Sleep in short increments so we can stop quickly
                slept = 0
                while AUTO_TRADER['running'] and slept < check_interval:
                    time.sleep(1)
                    slept += 1

            except Exception:
                logger.exception('Error in auto-trader loop')
                time.sleep(5)

        logger.info('Auto-trader worker exiting')

    t = threading.Thread(target=worker, daemon=True)
    with AUTO_TRADER['lock']:
        AUTO_TRADER['thread'] = t
    t.start()
    return True, 'started'


def stop_auto_trader():
    with AUTO_TRADER['lock']:
        if not AUTO_TRADER['running']:
            return False, 'not_running'
        AUTO_TRADER['running'] = False
    # Wait shortly for thread to exit
    t = AUTO_TRADER.get('thread')
    if t is not None:
        t.join(timeout=5)
    with AUTO_TRADER['lock']:
        AUTO_TRADER['thread'] = None
    return True, 'stopped'


@app.route('/api/auto/start', methods=['POST'])
def api_auto_start():
    if not _check_auth_header(request):
        return jsonify({'error': 'Unauthorized'}), 401
    data = request.get_json() or {}
    cfg = {
        'symbol': (data.get('symbol') or 'BTCUSDT').upper(),
        'timeframe': data.get('timeframe') or '1m',
        'broker': data.get('broker') or 'Simulated',
        'amount': data.get('amount', 10),
        'min_confidence': data.get('min_confidence', DEFAULT_MIN_CONFIDENCE),
        'max_trades_per_hour': data.get('max_trades_per_hour', DEFAULT_MAX_TRADES_PER_HOUR),
        'cooldown': data.get('cooldown', TRADE_COOLDOWN_SECONDS),
        'check_interval': data.get('check_interval'),
        'stop_loss': data.get('stop_loss'),
        'take_profit': data.get('take_profit'),
        'leverage': data.get('leverage'),
        'trade_type': data.get('trade_type'),
        'expiration': data.get('expiration', '1M'),
        'fast': data.get('fast', False)
    }
    ok, msg = start_auto_trader(cfg)
    return jsonify({'ok': ok, 'msg': msg, 'config': cfg})


@app.route('/api/auto/stop', methods=['POST'])
def api_auto_stop():
    if not _check_auth_header(request):
        return jsonify({'error': 'Unauthorized'}), 401
    ok, msg = stop_auto_trader()
    return jsonify({'ok': ok, 'msg': msg})


@app.route('/api/auto/status', methods=['GET'])
def api_auto_status():
    if not _check_auth_header(request):
        return jsonify({'error': 'Unauthorized'}), 401
    with AUTO_TRADER['lock']:
        cfg = AUTO_TRADER['config']
        running = AUTO_TRADER['running']
        trades = AUTO_TRADER['trades'][:10]
        trades_count = len(AUTO_TRADER['trade_timestamps'])
    last_trade = trades[0] if trades else None
    return jsonify({'running': running, 'config': cfg, 'recent_trades': trades, 'trades_last_hour': trades_count, 'last_trade': last_trade})


@app.route('/api/auto/trades', methods=['GET'])
def api_auto_trades():
    if not _check_auth_header(request):
        return jsonify({'error': 'Unauthorized'}), 401
    with AUTO_TRADER['lock']:
        return jsonify({'trades': AUTO_TRADER['trades']})


@app.route('/api/trades/log', methods=['GET'])
def api_trade_log():
    if not _check_auth_header(request):
        return jsonify({'error': 'Unauthorized'}), 401
    try:
        limit = int(request.args.get('limit', '200'))
    except Exception:
        limit = 200
    logs = read_trade_log(limit)
    return jsonify({'count': len(logs), 'trades': logs})


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
