import pandas as pd
from datetime import datetime, timedelta
import random


# Define a function to generate a random datetime within the next 24 hours
def random_datetime():
    return datetime.now() + timedelta(minutes=random.randint(1, 1440))


# Define a function to generate the instrument data
def generate_instrument_data(num_instruments):
    data = {
        "instrument": [f"INSTR{i}" for i in range(num_instruments)],
        "price": [round(random.uniform(1, 10000), 2) for i in range(num_instruments)],
        "volume": [random.randint(1, 10000) for i in range(num_instruments)],
        "expiry": [random_datetime() for i in range(num_instruments)]
    }
    return pd.DataFrame(data)


# Generate 1000 instruments and write to a CSV file
data = generate_instrument_data(1000)
data.to_csv("/mnt/c/Users/vipin/work/spaassignmentdata/instruments.csv", index=False)
