import datetime
from contextlib import closing

import pymysql
# import requests
from pymysql.cursors import DictCursor

from config import db_config


class ThreadMysqlController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)

    def check_for_existing_position(self, instrument):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                'SELECT * FROM positions WHERE zerodha_instrument_token = %s AND position_sl_exit_time IS NULL',
                (instrument,))
            active_trades = cursor.fetchall()
        return active_trades

    def exit_position(self, position, exit_price, exit_reason):
        profit = (float(exit_price) - float(position['position_entry_price'])) * position['lot_size']
        with self.conn.cursor() as cursor:
            cursor.execute(
                'UPDATE positions SET position_sl_exit_price = %s,position_sl_exit_time = NOW(), exit_reason = %s, '
                'sl_profit = %s WHERE position_id = %s',
                (exit_price, exit_reason, profit, position['position_id']))
        self.conn.commit()
        # x = requests.post("http://127.0.0.1:7000/api/place_order", json={
        #     "buy_or_sell": "S",
        #     "product_type": "M",
        #     "tradingsymbol": position['flat_trading_symbol'],
        #     "lot_size": position['flat_lot_size']
        # })
        # print(x.json())
        print("EXIT", position['zerodha_trading_symbol'], exit_price, datetime.datetime.now(), exit_reason)

    def check_for_existing_index_position(self, instrument):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                'SELECT * FROM positions WHERE index_name = %s AND position_exit_time IS NULL',
                instrument)
            active_trades = cursor.fetchall()
        return active_trades
