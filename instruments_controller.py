import pymysql
import pyotp
from pymysql.cursors import DictCursor

from config import db_config
import pandas as pd


def get_refresh_totp(totp_token):
    totp = pyotp.TOTP(totp_token)
    return totp.now()


class InstrumentsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)
        self.create_kite_instruments_table()
        self.create_flat_trade_instruments_table()

    def create_kite_instruments_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS zerodha_instruments (
                                zerodha_instrument_token INT,
                                zerodha_exchange_token INT,
                                zerodha_trading_symbol VARCHAR(255),
                                zerodha_name VARCHAR(255),
                                zerodha_last_price FLOAT,
                                zerodha_expiry DATE,
                                zerodha_strike FLOAT,
                                zerodha_tick_size FLOAT,
                                zerodha_lot_size INT,
                                zerodha_instrument_type VARCHAR(255),
                                zerodha_segment VARCHAR(255),
                                zerodha_exchange VARCHAR(255)
                            )
                        ''')
            self.conn.commit()

    def create_flat_trade_instruments_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS flat_trade_instruments (
                            Exchange VARCHAR(10),
                            Token INT,
                            Lotsize INT,
                            Symbol VARCHAR(50),
                            Tradingsymbol VARCHAR(100),
                            Instrument VARCHAR(20),
                            Expiry DATE,
                            Strike DECIMAL(10, 2),
                            Optiontype VARCHAR(5)
                        )
                        ''')
            self.conn.commit()

    def clear_kite_instruments(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''TRUNCATE TABLE zerodha_instruments''')
            self.conn.commit()

    def clear_flat_trade_instruments(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''TRUNCATE TABLE flat_trade_instruments''')
            self.conn.commit()

    def load_flat_trade_instruments(self):
        csv_files = [
            "https://flattrade.s3.ap-south-1.amazonaws.com/scripmaster/Nfo_Index_Derivatives.csv",
        ]
        try:
            with self.conn.cursor() as cursor:
                for csv_url in csv_files:
                    try:
                        # Load the CSV into a DataFrame
                        df = pd.read_csv(csv_url)

                        # Replace NaN with None
                        df = df.where(pd.notnull(df), None)

                        # Convert DataFrame to a list of tuples for insertion
                        data_tuples = [
                            (
                                row.Exchange,
                                row.Token,
                                row.Lotsize,
                                row.Symbol,
                                row.Tradingsymbol,
                                row.Instrument,
                                pd.to_datetime(row.Expiry).date() if row.Expiry else None,
                                row.Strike,
                                row.Optiontype,
                            )
                            for _, row in df.iterrows()
                        ]

                        # Insert data into the database
                        insert_query = """
                            INSERT INTO flat_trade_instruments (
                                Exchange, Token, Lotsize, Symbol, Tradingsymbol, Instrument, Expiry, Strike, Optiontype
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """
                        cursor.executemany(insert_query, data_tuples)
                        self.conn.commit()
                    except Exception as e:
                        print(f"An error occurred with {csv_url}: {e}")
            return True, "Flat trade instruments loaded successfully."
        except Exception as e:
            return False, str(e)

    def load_kite_instruments(self, kite):
        try:
            all_instruments = kite.instruments()
            insert_query = """INSERT INTO zerodha_instruments (zerodha_instrument_token, zerodha_exchange_token,
             zerodha_trading_symbol, zerodha_name, zerodha_last_price, zerodha_expiry, zerodha_strike, 
             zerodha_tick_size, zerodha_lot_size, zerodha_instrument_type, zerodha_segment, zerodha_exchange )
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
            with self.conn.cursor() as cursor:
                for instrument in all_instruments:
                    data = (
                        instrument['instrument_token'],
                        instrument['exchange_token'],
                        instrument['tradingsymbol'],
                        instrument['name'],
                        instrument['last_price'],
                        instrument['expiry'],
                        instrument['strike'],
                        instrument['tick_size'],
                        instrument['lot_size'],
                        instrument['instrument_type'],
                        instrument['segment'],
                        instrument['exchange']
                    )
                    cursor.execute(insert_query, data)
                    self.conn.commit()
            return True, "Zerodha instruments load successful"
        except Exception as e:
            return False, str(e)
