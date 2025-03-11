import numpy as np
import pandas as pd
import pandas_ta as ta
from sklearn.linear_model import LinearRegression


class TechnicalAnalysis:
    @staticmethod
    def calculate_atr(df, period=14):
        high_low = df['high'] - df['low']
        high_close = (df['high'] - df['close'].shift()).abs()
        low_close = (df['low'] - df['close'].shift()).abs()
        true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        atr = true_range.rolling(window=period).mean()
        return atr

    def calculate_signals(self, df, a=2, c=1):
        df['EMA_1'] = ta.ema(close=df['close'], length=1)
        df['atr'] = self.calculate_atr(df, period=c)
        df['nLoss'] = a * df['atr']
        df['src'] = df['close']
        df['xATRTrailingStop'] = np.nan

        # Initialize xATRTrailingStop
        if len(df) > 0:
            df.loc[df.index[0], 'xATRTrailingStop'] = df.loc[df.index[0], 'src']
        for i in range(1, len(df)):
            if df.iloc[i]['src'] > df.iloc[i - 1]['xATRTrailingStop'] and df.iloc[i - 1]['src'] > \
                    df.iloc[i - 1]['xATRTrailingStop']:
                df.loc[df.index[i], 'xATRTrailingStop'] = max(df.iloc[i - 1]['xATRTrailingStop'],
                                                              df.iloc[i]['src'] - df.iloc[i]['nLoss'])
            elif df.iloc[i]['src'] < df.iloc[i - 1]['xATRTrailingStop'] and df.iloc[i - 1]['src'] < \
                    df.iloc[i - 1]['xATRTrailingStop']:
                df.loc[df.index[i], 'xATRTrailingStop'] = min(df.iloc[i - 1]['xATRTrailingStop'],
                                                              df.iloc[i]['src'] + df.iloc[i]['nLoss'])
            elif df.iloc[i]['src'] > df.iloc[i - 1]['xATRTrailingStop']:
                df.loc[df.index[i], 'xATRTrailingStop'] = df.iloc[i]['src'] - df.iloc[i]['nLoss']
            else:
                df.loc[df.index[i], 'xATRTrailingStop'] = df.iloc[i]['src'] + df.iloc[i]['nLoss']

        df['pos'] = np.where(
            (df['src'].shift(1) < df['xATRTrailingStop'].shift(1)) & (df['src'] > df['xATRTrailingStop']),
            1, np.where(
                (df['src'].shift(1) > df['xATRTrailingStop'].shift(1)) & (df['src'] < df['xATRTrailingStop']), -1,
                np.nan))
        df['pos'] = df['pos'].ffill().fillna(0)
        df['buy_signal'] = (df['src'] > df['xATRTrailingStop']) & (df['EMA_1'] > df['xATRTrailingStop'])
        df['sell_signal'] = (df['src'] < df['xATRTrailingStop']) & (df['EMA_1'] < df['xATRTrailingStop'])

        return df

    @staticmethod
    def analyze_for_position(applied_ce_df):
        if applied_ce_df.iloc[-2].buy_signal and not applied_ce_df.iloc[-3].buy_signal:
            return True, applied_ce_df.iloc[-1].close
        else:
            return False, applied_ce_df.iloc[-1].close

    @staticmethod
    def analyze_for_exit(existing_ce_df):
        if existing_ce_df.iloc[-2].sell_signal and not existing_ce_df.iloc[-3].sell_signal:
            return True, existing_ce_df.iloc[-1].close, existing_ce_df.iloc[-1].close - float(
                existing_ce_df.iloc[-2].xATRTrailingStop)
        else:
            return False, existing_ce_df.iloc[-1].close, existing_ce_df.iloc[-1].close - float(
                existing_ce_df.iloc[-2].xATRTrailingStop)

    def linreg(self, series, length):
        """
        Computes a rolling linear regression similar to TradingView's linreg().

        Parameters:
        - series (pd.Series): The price series (Open, High, Low, or Close).
        - length (int): The regression length.

        Returns:
        - np.array: The rolling regression values.
        """
        lr_values = np.full(len(series), np.nan)

        for i in range(len(series) - length + 1):
            x = np.arange(length).reshape(-1, 1)  # Time index as feature
            y = series.iloc[i:i + length].values.reshape(-1, 1)  # Price values

            model = LinearRegression().fit(x, y)
            lr_values[i + length - 1] = model.predict([[length - 1]])[0][0]  # Predict the last value in the window

        return lr_values

    def linreg_candles(self, df, linreg_length=11, signal_length=7, use_sma=True):
        """
        Converts OHLC DataFrame to Linear Regression Candles.

        Parameters:
        - df (pd.DataFrame): DataFrame containing 'Open', 'High', 'Low', 'Close' columns.
        - linreg_length (int): The window size for Linear Regression.
        - signal_length (int): The window size for SMA/EMA smoothing.
        - use_sma (bool): If True, uses SMA; otherwise, uses EMA.

        Returns:
        - Updated DataFrame with Linear Regression Candlesticks and Signal Line.
        """
        df = df.copy()

        # Compute Linear Regression for each OHLC column
        df['LR_Open'] = self.linreg(df['open'], linreg_length)
        df['LR_High'] = self.linreg(df['high'], linreg_length)
        df['LR_Low'] = self.linreg(df['low'], linreg_length)
        df['LR_Close'] = self.linreg(df['close'], linreg_length)

        # Compute Signal Smoothing (SMA or EMA)
        if use_sma:
            df['LR_Signal'] = df['LR_Close'].rolling(window=signal_length).mean()
        else:
            df['LR_Signal'] = df['LR_Close'].ewm(span=signal_length, adjust=False).mean()

        # Identify Bullish and Bearish Candles
        df['Bullish'] = df['LR_Open'] < df['LR_Close']

        return df
