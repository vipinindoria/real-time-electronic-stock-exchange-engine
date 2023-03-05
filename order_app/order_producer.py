import json
import time

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
    buy_sell_options = ["Buy", "Sell"]
    buy_sell = st.selectbox("Select buy/sell option", buy_sell_options)
    order_id = f"{buy_sell}-{instrument}-{int(time.time() * 1000)}"
    return {"order_id": order_id, "instrument": instrument, "price": price, "volume": volume, "expiry": datetime.combine(datetime.now().date(), expiry), "buy_sell": buy_sell}


def get_bulk_order_details(csv_file):
    orders = []
    df = pd.read_csv(csv_file, parse_dates=['expiry'])
    for _, row in df.iterrows():
        order = {"order_id": row["order_id"], "instrument": row["instrument"], "price": row["price"], "volume": row["volume"],
                 "expiry": row["expiry"], "buy_sell": row["buy_sell"]}
        orders.append(order)
    return orders


# Define a function to place the order
def place_order(order, exchange_data, logger, cfg, producer):
    exchange_data = exchange_data.loc[exchange_data["instrument"] == order["instrument"], :]
    msg_slot = st.empty()
    with st.spinner('Placing order...'):
        if len(exchange_data) == 0:
            msg_slot.error(f"Invalid instrument [{order['instrument']}] selected ")
            return
        if datetime.now() > order["expiry"]:
            msg_slot.error(f"Order expiry time [{order['expiry']}] has passed for [{order['instrument']}]")
            return
        per_volume_order = round(order["price"] / order["volume"], 2)
        per_volume_exchange = round(exchange_data.iloc[0]["price"] / exchange_data.iloc[0]["volume"], 2)
        if per_volume_order < per_volume_exchange:
            msg_slot.error(f"Per Volume Order price {per_volume_order} is lower than the current market per volume price {per_volume_exchange}")
            return
        order["expiry"] = order["expiry"].strftime('%Y-%m-%d %H:%M:%S')
        order["order_time"] = int(time.time()) #datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        order["initial_order_time"] = order["order_time"]
        producer.produce(topic=cfg.orderapp.kafka.topic, key=f"{order['order_id']}_{order['order_time']}", value=json.dumps(order).encode('ascii'),
                         callback=lambda err, msg: kafka_utils.delivery_callback(err, msg, logger, msg_slot))
        producer.flush()
        #time.sleep(2)
        msg_slot.empty()


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

    # Allow the user to upload a CSV file
    st.write("## Bulk Order Upload")
    uploaded_file = st.file_uploader("Upload a CSV file", type="csv")
    if uploaded_file is not None:
        orders = get_bulk_order_details(uploaded_file)
        if st.button("Place Bulk Orders"):
            for order in orders:
                place_order(order, state["exchange_data"], logger, cfg, producer)

    # Get the instrument details from the user
    order = get_instrument_details(state["exchange_data"])

    # Place the order
    if st.button("Place Order"):
        place_order(order, state["exchange_data"], logger, cfg, producer)

    # Update the session state
    st.session_state.state = state
