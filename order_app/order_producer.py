import json

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from common import kafka_utils


# Define a function to simulate the exchange data
def get_exchange_data(cfg):
    return pd.read_csv(cfg.orderapp.instrument.filepath, parse_dates=['expiry'])


# Define a function to display the instruments and their details
def display_instruments(instruments):
    st.write("## Instruments")
    st.table(instruments)


# Define a function to get the instrument details from the user
def get_instrument_details(instruments):
    st.write("## Order Details")
    instrument = st.selectbox("Select an instrument", instruments["instrument"])
    price = st.number_input("Price")
    volume = st.number_input("Volume", min_value=1, value=1)
    expiry = st.time_input("Expiry Time", value=datetime.now().time())
    return {"instrument": instrument, "price": price, "volume": volume, "expiry": datetime.combine(datetime.now().date(), expiry)}


# Define a function to place the order
def place_order(order, exchange_data, logger, cfg, producer):
    exchange_data = exchange_data.loc[exchange_data["instrument"] == order["instrument"], :]

    if len(exchange_data) == 0:
        st.error("Invalid instrument selected")
        return
    if datetime.now() > exchange_data.iloc[0]["expiry"]:
        st.error("Order expiry time has passed")
        return
    if order["price"] < exchange_data.iloc[0]["price"]:
        st.error(f"Order price is lower than the current market price {exchange_data.iloc[0]['price']}")
        return
    order["expiry"] = order["expiry"].strftime('%Y-%m-%d %H:%M:%S')
    producer.produce(cfg.orderapp.kafka.topic, json.dumps(order).encode('ascii'),
                     callback=lambda err, msg: kafka_utils.delivery_callback(err, msg, logger))
    producer.flush()


def run(logger, cfg, producer):
    # Get or initialize the session state
    state = st.session_state.get("state", {
        "exchange_data": None,
        "instruments": None
    })

    # Load the exchange data if it hasn't been loaded yet
    if state["exchange_data"] is None:
        state["exchange_data"] = get_exchange_data(cfg)

    # Display the instruments if they haven't been loaded yet
    #if state["instruments"] is None:
    #    state["instruments"] = state["exchange_data"]["instrument"].unique()
    #    display_instruments(state["exchange_data"])

    # Get the instrument details from the user
    order = get_instrument_details(state["exchange_data"])

    # Place the order
    if st.button("Place Order"):
        place_order(order, state["exchange_data"], logger, cfg, producer)

    # Update the session state
    st.session_state.state = state
