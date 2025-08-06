# scripts/feature_engineering.py

import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings('ignore')


def calculate_technical_indicators(data, window=14, ema_short=12, ema_long=26, signal=9):
    """
    Calculate comprehensive technical indicators for stock price analysis.

    Args:
        data (pd.DataFrame): DataFrame with OHLC prices ('Open', 'High', 'Low', 'Close', 'Volume').
        window (int): Lookback period for RSI/SMA (default=14).
        ema_short (int): Short-term EMA period for MACD (default=12).
        ema_long (int): Long-term EMA period for MACD (default=26).
        signal (int): Signal line period for MACD (default=9).

    Returns:
        pd.DataFrame: Original data + indicator columns.
    """
    # Input validation
    if 'Close' not in data.columns:
        raise ValueError("DataFrame must contain 'Close' column")

    df = data.copy()

    # 1. Simple Moving Average (SMA) - Multiple periods
    df['SMA_14'] = df['Close'].rolling(window=window, min_periods=1).mean()
    df['SMA_50'] = df['Close'].rolling(window=50, min_periods=1).mean()
    df['SMA_200'] = df['Close'].rolling(window=200, min_periods=1).mean()

    # 2. Exponential Moving Average (EMA)
    df['EMA_12'] = df['Close'].ewm(span=ema_short, adjust=False).mean()
    df['EMA_26'] = df['Close'].ewm(span=ema_long, adjust=False).mean()

    # 3. MACD (Moving Average Convergence Divergence)
    df['MACD'] = df['EMA_12'] - df['EMA_26']
    df['MACD_Signal'] = df['MACD'].ewm(span=signal, adjust=False).mean()
    df['MACD_Histogram'] = df['MACD'] - df['MACD_Signal']

    # 4. RSI (Relative Strength Index) - Improved calculation
    delta = df['Close'].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    # Use exponential smoothing (more accurate than simple rolling)
    avg_gain = gain.ewm(alpha=1 / window, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / window, adjust=False).mean()

    rs = avg_gain / avg_loss
    df['RSI'] = 100 - (100 / (1 + rs))

    # 5. Bollinger Bands
    df['BB_Middle'] = df['Close'].rolling(window=20).mean()
    bb_std = df['Close'].rolling(window=20).std()
    df['BB_Upper'] = df['BB_Middle'] + (bb_std * 2)
    df['BB_Lower'] = df['BB_Middle'] - (bb_std * 2)
    df['BB_Width'] = df['BB_Upper'] - df['BB_Lower']
    df['BB_Position'] = (df['Close'] - df['BB_Lower']) / (df['BB_Upper'] - df['BB_Lower'])

    # 6. Momentum (Multiple periods)
    df['Momentum_10'] = df['Close'] - df['Close'].shift(10)
    df['Momentum_14'] = df['Close'] - df['Close'].shift(window)
    df['Price_Change_Pct'] = df['Close'].pct_change() * 100

    # 7. Volume-based indicators (if Volume exists)
    if 'Volume' in df.columns:
        df['Volume_SMA'] = df['Volume'].rolling(window=20).mean()
        df['Volume_Ratio'] = df['Volume'] / df['Volume_SMA']
        # On-Balance Volume (OBV)
        df['OBV'] = (np.sign(df['Close'].diff()) * df['Volume']).fillna(0).cumsum()

    # 8. Volatility indicators
    df['Volatility'] = df['Close'].rolling(window=window).std()
    df['ATR'] = calculate_atr(df) if all(col in df.columns for col in ['High', 'Low', 'Close']) else np.nan

    # 9. Support/Resistance levels
    if all(col in df.columns for col in ['High', 'Low']):
        df['Resistance'] = df['High'].rolling(window=20).max()
        df['Support'] = df['Low'].rolling(window=20).min()

    # 10. Trend indicators
    df['Price_Above_SMA50'] = (df['Close'] > df['SMA_50']).astype(int)
    df['EMA_Cross'] = np.where(df['EMA_12'] > df['EMA_26'], 1,
                               np.where(df['EMA_12'] < df['EMA_26'], -1, 0))

    return df


def calculate_atr(data, window=14):
    """Calculate Average True Range"""
    high_low = data['High'] - data['Low']
    high_close = np.abs(data['High'] - data['Close'].shift())
    low_close = np.abs(data['Low'] - data['Close'].shift())

    ranges = pd.concat([high_low, high_close, low_close], axis=1)
    true_range = np.max(ranges, axis=1)
    return true_range.rolling(window).mean()


def get_trading_signals(data):
    """Generate basic trading signals based on indicators"""
    df = data.copy()

    # Buy/Sell signals
    df['Buy_Signal'] = 0
    df['Sell_Signal'] = 0

    # RSI signals
    df.loc[df['RSI'] < 30, 'Buy_Signal'] = 1  # Oversold
    df.loc[df['RSI'] > 70, 'Sell_Signal'] = 1  # Overbought

    # MACD signals
    macd_buy = (df['MACD'] > df['MACD_Signal']) & (df['MACD'].shift(1) <= df['MACD_Signal'].shift(1))
    macd_sell = (df['MACD'] < df['MACD_Signal']) & (df['MACD'].shift(1) >= df['MACD_Signal'].shift(1))

    df.loc[macd_buy, 'Buy_Signal'] = 1
    df.loc[macd_sell, 'Sell_Signal'] = 1

    # Bollinger Bands signals
    df.loc[df['Close'] < df['BB_Lower'], 'Buy_Signal'] = 1  # Below lower band
    df.loc[df['Close'] > df['BB_Upper'], 'Sell_Signal'] = 1  # Above upper band

    return df


# Example Usage with real-world improvements
if __name__ == "__main__":
    # Create more realistic sample data
    np.random.seed(42)
    dates = pd.date_range(start="2023-01-01", periods=250, freq='D')

    # Simulate OHLC data with realistic patterns
    close_prices = 100 + np.cumsum(np.random.normal(0, 1, 250))
    high_prices = close_prices + np.random.uniform(0, 2, 250)
    low_prices = close_prices - np.random.uniform(0, 2, 250)
    open_prices = close_prices + np.random.normal(0, 0.5, 250)
    volume = np.random.randint(1000000, 5000000, 250)

    sample_data = pd.DataFrame({
        'Open': open_prices,
        'High': high_prices,
        'Low': low_prices,
        'Close': close_prices,
        'Volume': volume
    }, index=dates)

    # Calculate indicators
    analyzed_data = calculate_technical_indicators(sample_data)

    # Generate trading signals
    final_data = get_trading_signals(analyzed_data)

    # Display results
    print(" Technical Analysis Results:")
    print("=" * 50)

    # Show key indicators
    key_indicators = ['Close', 'SMA_50', 'RSI', 'MACD', 'BB_Position', 'Buy_Signal', 'Sell_Signal']
    print(final_data[key_indicators].tail(10))

    print(f"\n Summary Statistics:")
    print(f"RSI Range: {final_data['RSI'].min():.1f} - {final_data['RSI'].max():.1f}")
    print(f"Buy Signals: {final_data['Buy_Signal'].sum()}")
    print(f"Sell Signals: {final_data['Sell_Signal'].sum()}")
    print(f"Days above SMA50: {final_data['Price_Above_SMA50'].sum()}")
