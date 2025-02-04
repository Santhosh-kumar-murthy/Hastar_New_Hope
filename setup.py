from broker_controller import BrokerController
from instruments_controller import InstrumentsController


def kite_instrument_setup():
    instrument_load_manager = InstrumentsController()
    broker_controller = BrokerController()
    kite = broker_controller.kite_login()
    instrument_load_manager.clear_kite_instruments()
    status, log_text = instrument_load_manager.load_kite_instruments(kite)
    print(status, log_text)


def flat_trade_instrument_setup():
    instrument_load_manager = InstrumentsController()
    instrument_load_manager.clear_flat_trade_instruments()
    status, log_text = instrument_load_manager.load_flat_trade_instruments()
    print(status, log_text)


kite_instrument_setup()
flat_trade_instrument_setup()
