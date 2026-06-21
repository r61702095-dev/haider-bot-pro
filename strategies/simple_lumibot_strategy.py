"""
Simple Lumibot Trading Strategy - RSI + Moving Average
Trades based on RSI oversold/overbought and MA crossover signals.
"""
from lumibot.backtesting import YahooDataBacktester
from lumibot.entities import Asset
from lumibot.strategies import Strategy
import pandas as pd


class SimpleLumbotStrategy(Strategy):
    """
    Simple strategy that trades based on:
    - RSI indicator (oversold < 30 = BUY, overbought > 70 = SELL)
    - SMA crossover (fast > slow = uptrend)
    - Position management with stop-loss
    """
    
    parameters = {
        "rsi_period": 14,
        "rsi_buy_level": 30,
        "rsi_sell_level": 70,
        "fast_sma": 10,
        "slow_sma": 30,
        "position_size": 0.95,  # Use 95% of cash
        "stop_loss_percent": 0.02,  # 2% stop loss
    }

    def initialize(self):
        """Called once when strategy starts."""
        self.sleeptime = "1H"  # Check signals every hour
        self.last_price = None

    def on_trading_iteration(self):
        """Called on each trading iteration (every sleeptime)."""
        # Get current date and last 100 bars of data
        symbol = "SPY"
        bars = self.get_historical_prices(symbol, 100, "day")
        
        if bars is None or len(bars.df) < self.parameters["slow_sma"]:
            self.log_message("Insufficient data")
            return

        df = bars.df.copy()
        
        # Calculate RSI
        delta = df["close"].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=self.parameters["rsi_period"]).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=self.parameters["rsi_period"]).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        # Calculate SMAs
        fast_sma = df["close"].rolling(window=self.parameters["fast_sma"]).mean()
        slow_sma = df["close"].rolling(window=self.parameters["slow_sma"]).mean()
        
        # Get latest values
        current_price = df["close"].iloc[-1]
        current_rsi = rsi.iloc[-1]
        current_fast_sma = fast_sma.iloc[-1]
        current_slow_sma = slow_sma.iloc[-1]
        
        # Check positions
        position = self.get_position(symbol)
        
        # BUY signal: RSI oversold AND fast SMA above slow SMA
        if (current_rsi < self.parameters["rsi_buy_level"] and 
            current_fast_sma > current_slow_sma and 
            position is None):
            
            cash = self.get_cash()
            qty = int((cash * self.parameters["position_size"]) / current_price)
            if qty > 0:
                order = self.create_order(symbol, qty, "buy")
                self.submit_order(order)
                self.log_message(f"BUY {qty} @ {current_price:.2f} | RSI: {current_rsi:.1f}")
        
        # SELL signal: RSI overbought OR fast SMA below slow SMA
        elif position is not None:
            if (current_rsi > self.parameters["rsi_sell_level"] or 
                current_fast_sma < current_slow_sma):
                order = self.create_order(symbol, position.quantity, "sell")
                self.submit_order(order)
                self.log_message(f"SELL {position.quantity} @ {current_price:.2f} | RSI: {current_rsi:.1f}")
            
            # Stop loss check
            stop_price = position.avg_fill_price * (1 - self.parameters["stop_loss_percent"])
            if current_price < stop_price:
                order = self.create_order(symbol, position.quantity, "sell")
                self.submit_order(order)
                self.log_message(f"STOP LOSS @ {current_price:.2f}")


def run_backtest():
    """Run a simple backtest (optional - for validation)."""
    strategy = SimpleLumbotStrategy()
    backtester = YahooDataBacktester(
        strategy,
        [Asset(symbol="SPY", asset_type="stock")],
        datetime_start="2023-01-01",
        datetime_end="2023-12-31"
    )
    results = backtester.backtest()
    print(results)


if __name__ == "__main__":
    # Quick validation
    print("SimpleLumbotStrategy loaded successfully!")
    print(f"Parameters: {SimpleLumbotStrategy.parameters}")
