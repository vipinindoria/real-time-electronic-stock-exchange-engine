import pandas as pd
from datetime import datetime, timedelta
import random
from common import config_utils

cfg = config_utils.get_config('resources', 'app.yaml')

# Define a function to generate a random datetime within the next 24 hours
def random_datetime():
    return datetime.now() + timedelta(minutes=random.randint(1, 1440))


# Define a function to generate the instrument data
def generate_instrument_data(num_instruments):
    data = {
        "instrument": [f"INSTR{i}" for i in range(num_instruments)],
        "price": [round(random.uniform(1, 10000), 2) for i in range(num_instruments)],
        "volume": [random.randint(1, 1000) for i in range(num_instruments)],
        "expiry": [datetime.now() + timedelta(days=0, hours=random.randint(1, 8), minutes=random.randint(0, 0), seconds=random.randint(0, 0)) for i in range(num_instruments)]
    }
    return pd.DataFrame(data)


# Generate 1000 instruments and write to a CSV file
data = generate_instrument_data(100)
data.to_csv(cfg.orderapp.instrument.filepath, index=False)
