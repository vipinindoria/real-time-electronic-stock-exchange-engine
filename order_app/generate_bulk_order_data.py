import pandas as pd
from datetime import datetime, timedelta
import random
import time
from common import config_utils

cfg = config_utils.get_config('resources', 'app.yaml')

# Define a function to generate a random datetime within the next 24 hours
def random_datetime():
    return datetime.now() + timedelta(minutes=random.randint(1, 1440))


def get_instrument_data():
    return pd.read_csv(cfg.orderapp.instrument.filepath)


# Define a function to generate the instrument data
def generate_instrument_data(num_instruments, instrumentdata):
    instruments = [f"INSTR{random.randint(1, 99)}" for i in range(num_instruments)]
    price = []
    order_ids = []
    buy_sell_list = []
    volumes = []
    for instrument in instruments:
        min_price = instrumentdata.loc[instrumentdata['instrument'] == instrument, 'price'].iloc[0]
        min_price_per_volume = min_price / instrumentdata.loc[instrumentdata['instrument'] == instrument, 'volume'].iloc[0]
        buy_sell = random.choice(["Buy", "Sell"])
        volume = random.randint(1, 1000)
        trade_price = min_price_per_volume * volume
        volumes.append(volume)
        if buy_sell == "Sell":
            price.append(trade_price + random.randint(0, 100))
        if buy_sell == "Buy":
            price.append(trade_price + random.randint(100, 200))
        buy_sell_list.append(buy_sell)
        order_ids.append(f"{buy_sell}-{instrument}-{int(time.time() * 1000)}")
    data = {
        "order_id": order_ids,
        "instrument": instruments,
        "price": price,
        "volume": volumes,
        "expiry": [datetime.now() + timedelta(days=0, hours=random.randint(0, 0), minutes=random.randint(5, 15), seconds=random.randint(0, 0)) for i in range(num_instruments)],
        "buy_sell": buy_sell_list
    }
    return pd.DataFrame(data)


# Generate 1000 instruments and write to a CSV file
data = generate_instrument_data(10000, get_instrument_data())
data.to_csv(cfg.orderapp.bulkorder.filepath, index=False)
