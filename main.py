from datetime import datetime, time
from time import sleep

from kiteconnect import KiteTicker

from PositionsController import PositionsController
from broker_controller import BrokerController
from threadMysqlController import ThreadMysqlController

# Observable instruments
observable_indices = [
    {"name": "BANKNIFTY", "token": 260105, "exchange": "NSE"},
]

# Initialize controllers
broker_controller = BrokerController()

# Kite Login
kite = broker_controller.kite_login()
user_id = kite.profile()["user_id"]

# Initialize WebSocket
kws = KiteTicker(api_key="AlgoTrader", access_token=kite.enctoken + "&user_id=" + user_id)
subscribed_tokens = set()


def fetch_historical_data(instrument_token):
    """Fetch historical data for different timeframes (1min, 5min, 15min)."""
    try:
        return {
            '1min': broker_controller.kite_historic_data(kite, instrument_token, 'minute', a=2, c=1),
            '5min': broker_controller.kite_historic_data(kite, instrument_token, '3minute', a=2, c=1),
            # '15min': broker_controller.kite_historic_data(kite, instrument_token, '5minute', a=2, c=1)
        }
    except Exception as e:
        print(f"Error fetching historical data for token {instrument_token}: {e}")
        return None


def on_ticks(ws, ticks):
    """Handle real-time market data from WebSocket."""
    for tick in ticks:
        instrument_token = tick["instrument_token"]
        ltp = tick.get("last_price", None)
        process_tick_data(instrument_token, ltp)


def on_connect(ws, response):
    """Subscribe to instrument tokens when WebSocket connects."""
    global subscribed_tokens


def on_close(ws, code, reason):
    """Handle WebSocket disconnection."""
    print("WebSocket closed:", code, reason)


def on_error(ws, code, reason):
    """Handle WebSocket errors."""
    print("WebSocket error:", code, reason)


def add_position_to_ws(instrument_token):
    """Subscribe to an instrument token for real-time tracking."""
    if instrument_token not in subscribed_tokens:
        kws.subscribe([instrument_token])
        kws.set_mode(kws.MODE_FULL, [instrument_token])
        subscribed_tokens.add(instrument_token)
        print(f"Subscribed to {instrument_token}")


def remove_position_from_ws(instrument_token):
    """Unsubscribe from an instrument token."""
    if instrument_token in subscribed_tokens:
        kws.unsubscribe([instrument_token])
        subscribed_tokens.remove(instrument_token)
        print(f"Unsubscribed from {instrument_token}")


# Dictionary to track trailing stop-loss for each instrument
trailing_stoploss = {}

# Fixed stop-loss settings
MASTER_STOPLOSS_POINTS = 30
TRADING_END_TIME = time(15, 15)


def process_tick_data(instrument_token, ltp):
    threadMysqlController = ThreadMysqlController()
    """Apply Trailing Stop-Loss logic using fixed points."""
    # Get active position for this instrument
    active_positions = threadMysqlController.check_for_existing_position(instrument_token)
    if not active_positions:
        return  # No active trade for this instrument

    for position in active_positions:
        entry_price = position['position_entry_price']

        # Set initial stop-loss in points if not already set
        if instrument_token not in trailing_stoploss:
            trailing_stoploss[instrument_token] = entry_price - MASTER_STOPLOSS_POINTS  # SL is 20 points below entry
            print("Initial stop loss set to ", entry_price - MASTER_STOPLOSS_POINTS)

        # Check Stop-Loss Hit Condition
        if ltp <= trailing_stoploss[instrument_token]:  # If price drops to SL, exit

            threadMysqlController.exit_position(position, ltp, exit_reason="Master Stop-Loss Hit")
            print(f"Stop-Loss hit for {instrument_token} at {ltp}. Exiting position.")
            remove_position_from_ws(instrument_token)  # Unsubscribe from WebSocket tracking
            del trailing_stoploss[instrument_token]  # Remove from tracking


def startAlgo():
    """Main strategy loop to check for trade conditions and enter positions."""
    while True:
        try:
            positions_controller = PositionsController()
            current_time = datetime.now().time()
            if current_time > TRADING_END_TIME:
                for index in observable_indices:
                    active_position = positions_controller.check_for_existing_index_position(index['name'])
                    if active_position:
                        for position in active_position:
                            exit_price = broker_controller.get_ltp_kite(kite, position['zerodha_instrument_token'])
                            positions_controller.exit_position_strategic(position, exit_price, exit_reason="End of Day")
                break
            for index in observable_indices:
                active_positions = positions_controller.check_for_existing_index_position(index['name'])

                if not active_positions:
                    print("Running algo to take positions", index['name'])
                    # Refresh option strikes
                    ltp = broker_controller.get_ltp_kite(kite, index['token'])
                    index['ce_option'] = positions_controller.get_option_for_buying(index['name'], 1, ltp)
                    index['pe_option'] = positions_controller.get_option_for_buying(index['name'], 2, ltp)

                    # Fetch historical data for both CE & PE
                    ce_data = fetch_historical_data(index['ce_option']['zerodha_option']['zerodha_instrument_token'])
                    pe_data = fetch_historical_data(index['pe_option']['zerodha_option']['zerodha_instrument_token'])
                    # Entry conditions for Call Option
                    if (ce_data and ce_data['1min'].iloc[-2].buy_signal and ce_data['5min'].iloc[-1].buy_signal
                            # and
                            # ce_data['15min'].iloc[-1].buy_signal
                    ):
                        positions_controller.enter_new_position(
                            index['name'], index['ce_option'], ce_data['1min'].iloc[-1].close, 1
                        )
                        add_position_to_ws(index['ce_option']['zerodha_option']['zerodha_instrument_token'])
                        print(f"Entered new CE position for {index['name']}")
                        continue

                    # Entry conditions for Put Option
                    if (pe_data and pe_data['1min'].iloc[-2].buy_signal and pe_data['5min'].iloc[-1].buy_signal
                            # and
                            # pe_data['15min'].iloc[-1].buy_signal
                    ):
                        positions_controller.enter_new_position(
                            index['name'], index['pe_option'], pe_data['1min'].iloc[-1].close, 2
                        )
                        add_position_to_ws(index['pe_option']['zerodha_option']['zerodha_instrument_token'])
                        print(f"Entered new PE position for {index['name']}")
                        continue
                else:
                    for position in active_positions:
                        position_one_min_df = broker_controller.kite_historic_data(kite,
                                                                                   position['zerodha_instrument_token'],
                                                                                   'minute', a=2, c=1)
                        if position_one_min_df.iloc[-2].sell_signal:
                            positions_controller.exit_position_strategic(position, position_one_min_df.iloc[-1].close,
                                                                         "Strategy Exit")
        except Exception as e:
            print(f"MAIN_FUNCTION ERROR: {e}")


if __name__ == '__main__':
    # Attach WebSocket event handlers
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.on_error = on_error

    # Start WebSocket in a separate thread
    kws.connect(threaded=True)

    # Wait until WebSocket connects
    while not kws.is_connected():
        sleep(1)

    print("WebSocket: Connected")

    # Start the trading algorithm
    try:
        startAlgo()
    except KeyboardInterrupt:
        print("Stopping WebSocket connection...")
        kws.close()
