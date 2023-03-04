import pandas as pd
from datetime import datetime, timedelta
import random
import time


# Define a function to generate a random datetime within the next 24 hours
def random_datetime():
    return datetime.now() + timedelta(minutes=random.randint(1, 1440))


def get_instrument_data():
    return pd.read_csv('/mnt/c/Users/vipin/work/spaassignmentdata/instruments.csv')


# Define a function to generate the instrument data
def generate_instrument_data(num_instruments, instrumentdata):
    instruments = [f"INSTR{random.randint(1, num_instruments)}" for i in range(num_instruments)]
    price = []
    order_ids = []
    for instrument in instruments:
        min_price = instrumentdata.loc[instrumentdata['instrument'] == instrument, 'price'].iloc[0]
        price.append(round(random.uniform(min_price, 10000), 2))
        order_ids.append(f"ORDER-{instrument}-{int(time.time() * 1000)}")
    data = {
        "order_id": order_ids,
        "instrument": instruments,
        "price": price,
        "volume": [random.randint(1, 10000) for i in range(num_instruments)],
        "expiry": [datetime.now() + timedelta(days=0, hours=random.randint(0, 9), minutes=random.randint(0, 59), seconds=random.randint(0, 59)) for i in range(num_instruments)],
        "buy_sell": [random.choice(["Buy", "Sell"]) for i in range(num_instruments)]
    }
    return pd.DataFrame(data)


# Generate 1000 instruments and write to a CSV file
data = generate_instrument_data(1000, get_instrument_data())
data.to_csv("/mnt/c/Users/vipin/work/spaassignmentdata/bulkorder.csv", index=False)
