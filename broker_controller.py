import datetime
import time

import pandas as pd
import pyotp

from broker_libs.kite_trade import get_enctoken, KiteApp
from config import kite_config
from technical_analysis import TechnicalAnalysis


class BrokerController:
    def __init__(self):
        self.technical_analysis_controller = TechnicalAnalysis()

    @staticmethod
    def get_refresh_totp(totp_token):
        totp = pyotp.TOTP(totp_token)
        return totp.now()

    def kite_login(self):
        enc_token = get_enctoken(
            kite_config['user_id'],
            kite_config['password'],
            self.get_refresh_totp(kite_config['totp']))
        kite = KiteApp(enctoken=enc_token)
        return kite

    def kite_historic_data(self, kite, instrument_token, interval, a, c):
        from_datetime = datetime.datetime.now() - datetime.timedelta(days=4)
        to_datetime = datetime.datetime.now()
        interval = interval
        candle_data = pd.DataFrame(kite.historical_data(instrument_token, from_datetime, to_datetime, interval,
                                                        continuous=False, oi=False))
        applied_df = self.technical_analysis_controller.calculate_signals(candle_data, a=a, c=c)
        time.sleep(0.5)
        return applied_df

    @staticmethod
    def get_ltp_kite(broker, instrument_token):
        from_datetime = datetime.datetime.now() - datetime.timedelta(days=3)
        to_datetime = datetime.datetime.now()
        candle_data = broker.historical_data(
            instrument_token,
            from_datetime,
            to_datetime,
            'minute',
        )
        time.sleep(0.3)
        if len(candle_data) == 0:
            return 0
        return candle_data[-1]['close']
