from flask import Flask, render_template, jsonify, request
import pandas as pd
import requests

app = Flask(__name__)

# Binance API Data Function
def get_binance_data(symbol, interval="5m"):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol.upper()}&interval={interval}&limit=100"
    data = requests.get(url).json()
    df = pd.DataFrame(data, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'qav', 'num_trades', 'taker_base', 'taker_quote', 'ignore'])
    df['close'] = df['close'].astype(float)
    return df

# Pro Beast Logic with Trend Filter
def calculate_signals(df):
    delta = df['close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['rsi'] = 100 - (100 / (1 + rs))
    df['ema_20'] = df['close'].ewm(span=20, adjust=False).mean()
    
    last_row = df.iloc[-1]
    prev_row = df.iloc[-2]
    
    if last_row['rsi'] > 30 and prev_row['rsi'] <= 30 and last_row['close'] > last_row['ema_20']:
        return "🔥 STRONG BUY", round(last_row['rsi'], 2)
    elif last_row['rsi'] < 70 and prev_row['rsi'] >= 70 and last_row['close'] < last_row['ema_20']:
        return "📉 STRONG SELL", round(last_row['rsi'], 2)
    else:
        return "😴 NO TRADE", round(last_row['rsi'], 2)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/get_signal')
def get_signal():
    market = request.args.get('market', 'BTCUSDT') 
    try:
        df = get_binance_data(market)
        signal, rsi_val = calculate_signals(df)
        return jsonify({'signal': signal, 'rsi': rsi_val, 'market': market})
    except Exception as e:
        return jsonify({'status': 'Error', 'message': "Invalid Market"})

if __name__ == '__main__':
    app.run(debug=True)