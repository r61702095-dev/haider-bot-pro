from flask import Flask, render_template, jsonify, request
import os
import pandas as pd
import numpy as np
import requests
from datetime import datetime
import joblib
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import json
import time

app = Flask(__name__)

# In-memory history & simple win/loss tracker
SIGNAL_HISTORY = []  # stores last 20 signals
WINS = 0
LOSSES = 0

# Caches and background tasks for fast responses
MODEL_CACHE = {}   # key: (symbol, interval) -> {'model':..., 'scaler':..., 'acc':...,'ts':...}
ACCURACY_CACHE = {}  # key: (symbol, interval) -> {'accuracy': int, 'ts':...}

# Auth token: use env var AUTH_TOKEN or generate a file-local token
AUTH_TOKEN = os.environ.get('AUTH_TOKEN')
TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'auth_token.txt')
if not AUTH_TOKEN:
    try:
        if os.path.exists(TOKEN_FILE):
            with open(TOKEN_FILE, 'r') as f:
                AUTH_TOKEN = f.read().strip()
        else:
            import secrets
            AUTH_TOKEN = secrets.token_hex(16)
            with open(TOKEN_FILE, 'w') as f:
                f.write(AUTH_TOKEN)
            print(f"Generated auth token and saved to {TOKEN_FILE}: {AUTH_TOKEN}")
    except Exception:
        AUTH_TOKEN = None

def map_interval(tf):
    # Map requested timeframe to Binance-supported interval
    mapping = {
        '15s': '1m',
        '30s': '1m',
        '1m': '1m',
        '5m': '5m',
        '1d': '1d'
    }
    return mapping.get(tf, '1m')

def fetch_klines(symbol, interval='1d', limit=500):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}&interval={interval}&limit={limit}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        df = pd.DataFrame(data, columns=['open_time','o','h','l','c','v','ct','q','n','tb','tq','i'])
        df['open'] = df['o'].astype(float)
        df['high'] = df['h'].astype(float)
        df['low'] = df['l'].astype(float)
        df['close'] = df['c'].astype(float)
        df['volume'] = df['v'].astype(float)
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        return df
    except Exception as e:
        print('Fetch error', e)
        return None


def is_duplicate_signal(market, timeframe, signal, window_seconds=60):
    if not SIGNAL_HISTORY:
        return False
    last = SIGNAL_HISTORY[0]
    try:
        if last.get('market') == market.upper() and last.get('timeframe') == timeframe and last.get('signal') == signal:
            # compare timestamps
            last_ts = datetime.fromisoformat(last.get('timestamp').replace('Z', ''))
            now = datetime.utcnow()
            diff = (now - last_ts).total_seconds()
            return diff < window_seconds
    except Exception:
        return False
    return False

def calc_indicators(df):
    df = df.copy()
    df['ema_250'] = df['close'].ewm(span=250, adjust=False).mean()
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
    # RSI 14
    delta = df['close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.ewm(com=13, adjust=False).mean()
    ma_down = down.ewm(com=13, adjust=False).mean()
    rs = ma_up / ma_down
    df['rsi'] = 100 - (100 / (1 + rs))
    # MACD
    df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
    df['macd'] = df['ema12'] - df['ema26']
    df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
    # Volume MA
    df['vol_ma'] = df['volume'].rolling(window=20, min_periods=1).mean()
    return df

def build_signal(df, market, timeframe):
    df = calc_indicators(df)
    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else last

    candle_momentum = (last['close'] - last['open']) / last['open'] if last['open'] != 0 else 0

    # Conditions
    conds = []
    conds.append(last['close'] > last['ema_250'])            # long-term trend
    conds.append(last['rsi'] < 40)                            # oversold dip
    conds.append(last['volume'] > last['vol_ma'])            # volume confirmation
    conds.append(last['ema20'] > last['ema50'])              # short-term bullish
    conds.append(last['macd'] > last['macd_signal'])         # momentum
    conds.append(candle_momentum > 0.0005)                   # candle momentum filter

    # Sell equivalents
    sconds = []
    sconds.append(last['close'] < last['ema_250'])
    sconds.append(last['rsi'] > 60)
    sconds.append(last['volume'] > last['vol_ma'])
    sconds.append(last['ema20'] < last['ema50'])
    sconds.append(last['macd'] < last['macd_signal'])
    sconds.append(candle_momentum < -0.0005)

    agree_buy = sum(1 for c in conds if c)
    agree_sell = sum(1 for c in sconds if c)
    total = len(conds)

    # Dynamic accuracy: base 50 + proportion * 50
    accuracy_buy = int(50 + (agree_buy/total)*50)
    accuracy_sell = int(50 + (agree_sell/total)*50)

    # Multi-timeframe confirmation: check higher timeframe trend (if timeframe < 1d)
    mtf_bonus = 0
    try:
        if timeframe != '1d':
            higher = '1d'
            df_h = fetch_klines(market, interval=map_interval(higher), limit=300)
            if df_h is not None:
                df_h = calc_indicators(df_h)
                last_h = df_h.iloc[-1]
                # confirm long-term trend
                if last['close'] > last_h['ema_250']:
                    mtf_bonus += 10
                elif last['close'] < last_h['ema_250']:
                    mtf_bonus -= 10
    except Exception:
        pass

    # Volatility filter: penalize signals during high volatility
    vol_penalty = 0
    try:
        returns = df['close'].pct_change().dropna()
        vol = returns.rolling(14).std().iloc[-1]
        # if volatility very high, reduce confidence and possibly reject
        if vol is not None:
            if vol > 0.06:
                vol_penalty += 25
            elif vol > 0.03:
                vol_penalty += 10
    except Exception:
        vol = None

    # Support/resistance: simple recent high/low check
    sr_note = None
    try:
        recent_high = df['high'].rolling(window=50, min_periods=1).max().iloc[-2]
        recent_low = df['low'].rolling(window=50, min_periods=1).min().iloc[-2]
        if last['close'] >= recent_high * 0.995:
            sr_note = 'Near resistance'
            vol_penalty += 5
        if last['close'] <= recent_low * 1.005:
            sr_note = 'Near support'
            vol_penalty -= 2
    except Exception:
        sr_note = None

    signal = 'WAIT'
    accuracy = 0
    reason = 'No consensus across indicators.'

    if agree_buy == total:
        signal = 'BUY'
        accuracy = accuracy_buy
        reason = 'All indicators aligned for BUY (multi-confirmation).'
    elif agree_sell == total:
        signal = 'SELL'
        accuracy = accuracy_sell
        reason = 'All indicators aligned for SELL (multi-confirmation).'
    else:
        # If majority agrees, soft signal
        if agree_buy >= 4 and agree_buy > agree_sell:
            signal = 'BUY'
            accuracy = accuracy_buy
            reason = 'Majority indicators support BUY.'
        elif agree_sell >=4 and agree_sell > agree_buy:
            signal = 'SELL'
            accuracy = accuracy_sell
            reason = 'Majority indicators support SELL.'
        else:
            signal = 'WAIT'
            accuracy = int(40 + (max(agree_buy, agree_sell)/total)*40)

    # Apply multi-timeframe bonus/penalty
    accuracy = int(max(0, min(100, (int(str(accuracy).replace('%','')) if isinstance(accuracy, str) else accuracy) + mtf_bonus - vol_penalty)))

    # Confidence meter as number
    confidence = accuracy

    # Reject weak signals automatically
    if confidence < 65:
        signal = 'WAIT'

    # Prevent duplicate signals
    try:
        if is_duplicate_signal(market, timeframe, signal):
            signal = 'WAIT'
            reason = 'Duplicate signal suppressed.'
    except Exception:
        pass

    # Build payload
    payload = {
        'market': market.upper(),
        'timeframe': timeframe,
        'signal': signal,
        'accuracy': f"{accuracy}%",
        'price': float(round(last['close'], 8)),
        'rsi': float(round(last['rsi'], 2)),
        'ema20': float(round(last['ema20'], 6)),
        'ema50': float(round(last['ema50'], 6)),
        'ema250': float(round(last['ema_250'], 6)),
        'macd': float(round(last['macd'], 6)),
        'macd_signal': float(round(last['macd_signal'], 6)),
        'volume': float(round(last['volume'], 6)),
        'vol_ma': float(round(last['vol_ma'], 6)),
        'candle_momentum': float(round(candle_momentum, 6)),
        'reason': reason,
        'sr_note': sr_note,
        'confidence': confidence,
        'timestamp': datetime.utcnow().isoformat() + 'Z'
    }

    # Update history
    SIGNAL_HISTORY.insert(0, {'market': payload['market'], 'timeframe': timeframe, 'signal': signal, 'accuracy': payload['accuracy'], 'timestamp': payload['timestamp']})
    while len(SIGNAL_HISTORY) > 20:
        SIGNAL_HISTORY.pop()

    return payload


def execute_broker_trade(broker, entry):
    """Attempt to execute trade via broker adapters. For real trading, set environment
    variables for the broker API. Currently supports 'Quotex' (via QUOTEX_API_URL).
    Falls back to simulated execution and returns a dict with execution info.
    """
    broker_lower = (broker or '').strip().lower()
    # Quotex adapter (placeholder) - expects QUOTEX_API_URL, QUOTEX_API_KEY, QUOTEX_API_SECRET
    if broker_lower == 'quotex':
        api_url = os.environ.get('QUOTEX_API_URL')
        api_key = os.environ.get('QUOTEX_API_KEY')
        api_secret = os.environ.get('QUOTEX_API_SECRET')
        if api_url and api_key:
            try:
                payload = {
                    'symbol': entry['market'],
                    'side': entry['action'],
                    'amount': entry.get('amount', 0),
                    'timestamp': entry.get('timestamp')
                }
                headers = {'API-KEY': api_key, 'Content-Type': 'application/json'}
                # If secret present, sign payload with HMAC-SHA256 and add header
                if api_secret:
                    import hmac, hashlib
                    msg = (json.dumps(payload, separators=(',', ':'), sort_keys=True)).encode()
                    sig = hmac.new(api_secret.encode(), msg, hashlib.sha256).hexdigest()
                    headers['API-SIGN'] = sig
                r = requests.post(api_url, json=payload, headers=headers, timeout=15)
                r.raise_for_status()
                try:
                    resp_json = r.json()
                except Exception:
                    resp_json = {'text': r.text}
                return {'status': 'executed', 'broker': 'Quotex', 'response': resp_json}
            except Exception as e:
                return {'status': 'error', 'broker': 'Quotex', 'error': str(e)}
        else:
            # Missing config, return simulated
            return {'status': 'simulated', 'broker': 'Quotex', 'note': 'QUOTEX_API_URL or API key missing'}

    # Binance adapter (spot market) - requires BINANCE_API_KEY & BINANCE_API_SECRET
    if 'binance' in broker_lower:
        api_key = os.environ.get('BINANCE_API_KEY')
        api_secret = os.environ.get('BINANCE_API_SECRET')
        base = os.environ.get('BINANCE_API_URL', 'https://api.binance.com')
        if api_key and api_secret:
            try:
                # build market order params; amount expected as quantity
                qty = entry.get('amount', 0)
                if qty is None or float(qty) <= 0:
                    return {'status': 'error', 'broker': 'Binance', 'error': 'invalid amount'}
                params = {
                    'symbol': entry['market'],
                    'side': entry['action'].upper(),
                    'type': 'MARKET',
                    'quantity': float(qty),
                    'timestamp': int(time.time()*1000)
                }
                # build query string
                qs = '&'.join([f"{k}={str(params[k])}" for k in params])
                import hmac, hashlib
                signature = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
                qs_signed = qs + f"&signature={signature}"
                url = base.rstrip('/') + '/api/v3/order'
                headers = {'X-MBX-APIKEY': api_key}
                r = requests.post(url + '?' + qs_signed, headers=headers, timeout=15)
                r.raise_for_status()
                try:
                    resp = r.json()
                except Exception:
                    resp = {'text': r.text}
                return {'status': 'executed', 'broker': 'Binance', 'response': resp}
            except Exception as e:
                return {'status': 'error', 'broker': 'Binance', 'error': str(e)}
        else:
            return {'status': 'simulated', 'broker': 'Binance', 'note': 'BINANCE_API_KEY or secret missing'}

    # Generic adapter using env vars: e.g., BINANCE_API_URL, BINANCE_API_KEY
    broker_key = (broker or 'Simulated').upper().replace(' ', '_')
    generic_url = os.environ.get(f"{broker_key}_API_URL")
    generic_key = os.environ.get(f"{broker_key}_API_KEY")
    if generic_url and generic_key:
        try:
            payload = {
                'symbol': entry['market'],
                'side': entry['action'],
                'amount': entry.get('amount', 0),
                'timestamp': entry.get('timestamp')
            }
            headers = {'API-KEY': generic_key, 'Content-Type': 'application/json'}
            r = requests.post(generic_url, json=payload, headers=headers, timeout=10)
            r.raise_for_status()
            return {'status': 'executed', 'broker': broker_key, 'response': r.json()}
        except Exception as e:
            return {'status': 'error', 'broker': broker_key, 'error': str(e)}

    return {'status': 'simulated', 'broker': broker or 'Simulated', 'note': 'No real adapter configured'}


def estimate_historical_accuracy(symbol, interval='1d', lookback=200):
    """Estimate accuracy by backtesting the signal logic over historical candles.
    This is a simple directional next-candle test and is only an estimate.
    """
    cache_key = (symbol.upper(), interval)
    # return cached value if fresh (< 6 hours)
    cached = ACCURACY_CACHE.get(cache_key)
    if cached and (datetime.utcnow() - cached['ts']).total_seconds() < 6*3600:
        return cached['accuracy']

    try:
        df = fetch_klines(symbol, interval=interval, limit=lookback+50)
        if df is None or len(df) < 50:
            return None
        df = calc_indicators(df)
        wins = 0
        total = 0
        # start after indicators have warmed up
        start = 50
        for i in range(start, len(df)-1):
            sub = df.iloc[:i+1].copy()
            last = sub.iloc[-1]
            candle_momentum = (last['close'] - last['open']) / last['open'] if last['open'] != 0 else 0
            conds = [
                last['close'] > last['ema_250'],
                last['rsi'] < 40,
                last['volume'] > last['vol_ma'],
                last['ema20'] > last['ema50'],
                last['macd'] > last['macd_signal'],
                candle_momentum > 0.0005
            ]
            sconds = [
                last['close'] < last['ema_250'],
                last['rsi'] > 60,
                last['volume'] > last['vol_ma'],
                last['ema20'] < last['ema50'],
                last['macd'] < last['macd_signal'],
                candle_momentum < -0.0005
            ]
            agree_buy = sum(1 for c in conds if c)
            agree_sell = sum(1 for c in sconds if c)
            signal = None
            if agree_buy >= 5 and agree_buy > agree_sell:
                signal = 'BUY'
            elif agree_sell >=5 and agree_sell > agree_buy:
                signal = 'SELL'
            else:
                continue

            next_close = df['close'].iloc[i+1]
            cur_close = last['close']
            total += 1
            if signal == 'BUY' and next_close > cur_close:
                wins += 1
            if signal == 'SELL' and next_close < cur_close:
                wins += 1

        if total == 0:
            return None
        acc = int((wins/total)*100)
        ACCURACY_CACHE[cache_key] = {'accuracy': acc, 'ts': datetime.utcnow()}
        return acc
    except Exception:
        return None


def train_or_load_model(symbol, interval='1d'):
    """Train a quick logistic regression model on historical features or load existing one."""
    model_dir = os.path.join(os.path.dirname(__file__), 'models')
    os.makedirs(model_dir, exist_ok=True)
    model_path = os.path.join(model_dir, f"model_{symbol.upper()}_{interval}.pkl")
    scaler_path = os.path.join(model_dir, f"scaler_{symbol.upper()}_{interval}.pkl")

    # if exists in disk or cache, load
    cache_key = (symbol.upper(), interval)
    if cache_key in MODEL_CACHE:
        entry = MODEL_CACHE[cache_key]
        return entry.get('model'), entry.get('scaler'), entry.get('acc')
    if os.path.exists(model_path) and os.path.exists(scaler_path):
        try:
            model = joblib.load(model_path)
            scaler = joblib.load(scaler_path)
            MODEL_CACHE[cache_key] = {'model': model, 'scaler': scaler, 'acc': None, 'ts': datetime.utcnow()}
            return model, scaler, None
        except Exception:
            pass

    # train
    try:
        df = fetch_klines(symbol, interval=interval, limit=1200)
        if df is None or len(df) < 200:
            return None, None, None
        df = calc_indicators(df)
        # build features
        features = pd.DataFrame({
            'rsi': df['rsi'],
            'ema20': df['ema20'],
            'ema50': df['ema50'],
            'macd': df['macd'],
            'vol': df['volume'],
            'vol_ma': df['vol_ma'],
            'mom': (df['close'] - df['open'])/df['open']
        })
        # label: next candle direction
        labels = (df['close'].shift(-1) > df['close']).astype(int)
        # drop na
        mask = features.notnull().all(axis=1) & labels.notnull()
        X = features.loc[mask]
        y = labels.loc[mask]
        if len(X) < 200:
            return None, None, None

        scaler = StandardScaler()
        Xs = scaler.fit_transform(X)
        X_train, X_test, y_train, y_test = train_test_split(Xs, y, test_size=0.2, shuffle=False)
        model = LogisticRegression(max_iter=500)
        model.fit(X_train, y_train)
        acc = model.score(X_test, y_test)
        joblib.dump(model, model_path)
        joblib.dump(scaler, scaler_path)
        MODEL_CACHE[cache_key] = {'model': model, 'scaler': scaler, 'acc': float(acc*100), 'ts': datetime.utcnow()}
        return model, scaler, float(acc*100)
    except Exception:
        return None, None, None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_signal')
def get_signal():
    market = request.args.get('market', 'BTCUSDT')
    timeframe = request.args.get('timeframe', '1d')
    interval = map_interval(timeframe)
    limit = 300 if interval == '1d' else 500

    df = fetch_klines(market, interval=interval, limit=limit)
    if df is None or df.empty:
        return jsonify({'error': 'unable to fetch market data'}), 500

    payload = build_signal(df, market, timeframe)
    payload['history'] = SIGNAL_HISTORY
    # Try to use cached model quickly
    try:
        cache_key = (market.upper(), interval)
        model_entry = MODEL_CACHE.get(cache_key)
        if model_entry:
            model = model_entry.get('model')
            scaler = model_entry.get('scaler')
            ml_acc = model_entry.get('acc')
            if model and scaler:
                last = df.iloc[-1]
                feat = pd.DataFrame({
                    'rsi': [last['rsi']],
                    'ema20': [last['ema20']],
                    'ema50': [last['ema50']],
                    'macd': [last['macd']],
                    'vol': [last['volume']],
                    'vol_ma': [last['vol_ma']],
                    'mom': [(last['close'] - last['open'])/last['open'] if last['open']!=0 else 0]
                })
                Xs = scaler.transform(feat)
                prob = float(model.predict_proba(Xs)[0][1])
                payload['ml_proba'] = round(prob, 4)
                payload['ml_accuracy'] = f"{int(ml_acc)}%" if ml_acc is not None else None
                if prob > 0.85:
                    payload['ml_signal'] = 'BUY' if prob > 0.5 else 'SELL'
                    payload['signal'] = payload['ml_signal']
                    payload['reason'] = 'ML ensemble override (high confidence)'
                    payload['confidence'] = int(max(payload.get('confidence',50), int(prob*100)))
                else:
                    payload['ml_signal'] = 'BUY' if prob > 0.5 else 'SELL'
        else:
            # spawn background model training if not in cache
            from threading import Thread
            def background_train():
                try:
                    train_or_load_model(market, interval=interval)
                except Exception:
                    pass
            Thread(target=background_train, daemon=True).start()
    except Exception:
        pass
    # attach estimated historical accuracy when possible
    try:
        est = estimate_historical_accuracy(market, interval=interval, lookback=200)
        if est is not None:
            payload['historical_accuracy'] = f"{est}%"
    except Exception:
        pass
    return jsonify(payload)


@app.route('/trade', methods=['POST'])
def trade():
    try:
        # simple auth for trade endpoint
        provided = request.headers.get('X-AUTH-TOKEN')
        if AUTH_TOKEN and provided != AUTH_TOKEN:
            return jsonify({'error': 'forbidden, missing or invalid token'}), 403

        data = request.get_json() or {}
        market = data.get('market')
        broker = data.get('broker', 'Simulated')
        action = data.get('action')
        amount = float(data.get('amount', 0)) if data.get('amount') is not None else 0.0
        timestamp = datetime.utcnow().isoformat() + 'Z'

        if not market or not action:
            return jsonify({'error': 'market and action required'}), 400

        entry = {
            'market': market.upper(),
            'broker': broker,
            'action': action,
            'amount': amount,
            'timestamp': timestamp,
            'auto': True
        }

        # Try to execute via adapter
        exec_result = execute_broker_trade(broker, entry)

        # Record the execution result (simulated or real response) into history
        record = entry.copy()
        record['exec_result'] = exec_result
        SIGNAL_HISTORY.insert(0, record)
        while len(SIGNAL_HISTORY) > 20:
            SIGNAL_HISTORY.pop()

        return jsonify({'status': 'ok', 'executed': record, 'exec_result': exec_result})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/history')
def history():
    return jsonify(SIGNAL_HISTORY)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
    