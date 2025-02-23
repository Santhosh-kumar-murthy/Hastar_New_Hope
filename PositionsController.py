import datetime
from contextlib import closing

import pymysql
import requests
from pymysql.cursors import DictCursor

from config import db_config


class PositionsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)
        self.create_positions_table()

    def create_positions_table(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS positions (
                                position_id INT AUTO_INCREMENT PRIMARY KEY,
                                zerodha_instrument_token INT,
                                zerodha_trading_symbol VARCHAR(255),
                                index_name VARCHAR(255),
                                zerodha_exchange VARCHAR(255),
                                direction INT,
                                lot_size INT,
                                expiry DATE,
                                position_entry_time DATETIME,
                                position_entry_price FLOAT,
                                position_exit_time DATETIME,
                                position_sl_exit_time DATETIME,
                                position_exit_price FLOAT,
                                position_sl_exit_price FLOAT,
                                exit_reason VARCHAR(255),
                                profit FLOAT,
                                sl_profit FLOAT,
                                flat_token INT,
                                flat_lot_size INT,
                                flat_symbol VARCHAR(50),
                                flat_trading_symbol VARCHAR(100),
                                flat_instrument VARCHAR(20),
                                flat_option_type VARCHAR(5)
                            )
                        ''')
            self.conn.commit()

    def check_for_existing_position(self, instrument):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                'SELECT * FROM positions WHERE index_name = %s AND position_exit_time IS NULL',
                (instrument,))
            active_trades = cursor.fetchall()
        return active_trades

    def get_option_for_buying(self, instrument, position_type, current_price):
        instrument_types = {
            1: 'CE',
            2: 'PE'
        }
        instrument_type = instrument_types.get(position_type, 'Unknown')

        queries = {
            "zerodha_long_query": """ SELECT * FROM zerodha_instruments WHERE zerodha_segment IN ('NFO-OPT', 'BFO-OPT')
             AND zerodha_name = %s AND zerodha_instrument_type = %s AND zerodha_expiry >= CURDATE() AND 
             zerodha_strike < %s ORDER BY zerodha_expiry ASC, zerodha_strike DESC LIMIT 1; """,
            "zerodha_short_query": """ SELECT * FROM zerodha_instruments WHERE zerodha_segment IN 
            ('NFO-OPT', 'BFO-OPT') AND zerodha_name = %s AND zerodha_instrument_type = %s AND 
            zerodha_expiry >= CURDATE() AND zerodha_strike > %s ORDER BY zerodha_expiry ASC, 
            zerodha_strike ASC LIMIT 1; """,

            "flat_trade_long_query": """ SELECT * FROM flat_trade_instruments WHERE Exchange = 'NFO'
                     AND Symbol = %s AND Optiontype = %s AND Expiry >= CURDATE() AND 
                     Strike < %s ORDER BY Expiry ASC, Strike DESC LIMIT 1; """,
            "flat_trade_short_query": """ SELECT * FROM flat_trade_instruments WHERE Exchange = 'NFO'
             AND Symbol = %s AND Optiontype = %s AND 
                    Expiry >= CURDATE() AND Strike > %s ORDER BY Expiry ASC, 
                    Strike ASC LIMIT 1; """,
        }
        zerodha_query = queries.get('zerodha_long_query' if position_type == 1 else 'zerodha_short_query', 'Unknown')
        flat_trade_query = queries.get('flat_trade_long_query' if position_type == 1 else 'flat_trade_short_query',
                                       'Unknown')

        with closing(self.conn.cursor()) as cursor:
            cursor.execute(zerodha_query, (instrument, instrument_type, current_price))
            zerodha_option = cursor.fetchone()
            cursor.execute(flat_trade_query, (instrument, instrument_type, current_price))
            flat_trade_option = cursor.fetchone()
            return {
                "zerodha_option": zerodha_option,
                "flat_trade_option": flat_trade_option
            }

    def enter_new_position(self, index_name, option_data, buy_price, direction):
        with self.conn.cursor() as cursor:
            cursor.execute('INSERT INTO positions (zerodha_instrument_token,zerodha_trading_symbol,index_name,'
                           'zerodha_exchange,direction,lot_size,expiry,position_entry_time,position_entry_price,'
                           'flat_token,flat_lot_size,flat_symbol,flat_trading_symbol,flat_instrument,flat_option_type)'
                           'VALUES (%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s,%s,%s,%s,%s,%s)',
                           (option_data['zerodha_option']['zerodha_instrument_token'],
                            option_data['zerodha_option']['zerodha_trading_symbol'], index_name,
                            option_data['zerodha_option']['zerodha_exchange'], direction,
                            option_data['zerodha_option']['zerodha_lot_size'],
                            option_data['zerodha_option']['zerodha_expiry'],
                            buy_price,
                            option_data['flat_trade_option']['Token'],
                            option_data['flat_trade_option']['Lotsize'],
                            option_data['flat_trade_option']['Symbol'],
                            option_data['flat_trade_option']['Tradingsymbol'],
                            option_data['flat_trade_option']['Instrument'],
                            option_data['flat_trade_option']['Optiontype'],
                            ))
        self.conn.commit()
        requests.post("http://127.0.0.1:7000/api/place_order", json={
            "buy_or_sell": "B",
            "product_type": "M",
            "tradingsymbol": option_data['flat_trade_option']['Tradingsymbol'],
            "lot_size": option_data['flat_trade_option']['Lotsize']
        })
        print("ENTRY", option_data['zerodha_option']['zerodha_trading_symbol'], buy_price, datetime.datetime.now())

    def exit_position_strategic(self, position, exit_price, exit_reason):
        profit = (float(exit_price) - float(position['position_entry_price'])) * position['lot_size']
        if position['position_sl_exit_price'] is None:
            sl_profit = profit
            position_sl_exit_price = exit_price
            position_sl_exit_time = datetime.datetime.now()

        else:
            sl_profit = position['sl_profit']
            position_sl_exit_price = position['position_sl_exit_price']
            position_sl_exit_time = position['position_sl_exit_time']
        with self.conn.cursor() as cursor:
            cursor.execute(
                'UPDATE positions SET position_exit_price = %s,position_exit_time = NOW(), position_sl_exit_price = %s,'
                'position_sl_exit_time = %s, exit_reason = %s, '
                'profit = %s, sl_profit = %s WHERE position_id = %s',
                (exit_price, position_sl_exit_price, position_sl_exit_time, exit_reason, profit, sl_profit,
                 position['position_id']))
        self.conn.commit()
        requests.post("http://127.0.0.1:7000/api/place_order", json={
            "buy_or_sell": "S",
            "product_type": "M",
            "tradingsymbol": position['flat_trading_symbol'],
            "lot_size": position['flat_lot_size']
        })
        print("EXIT", position['zerodha_trading_symbol'], exit_price, datetime.datetime.now(), exit_reason)

    def check_for_existing_index_position(self, instrument):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                'SELECT * FROM positions WHERE index_name = %s AND position_exit_time IS NULL',
                instrument)
            active_trades = cursor.fetchall()
        return active_trades
