import sys
from confluent_kafka import Producer
import streamlit as st


def get_producer(logger, cfg):
    logger.info("Creating producer instance on kafka.")
    try:
        return Producer(cfg)
    except Exception as e:
        logger.error(f"Error while creating producer instance on kafka - {e}")
        sys.exit(1)


def delivery_callback(err, msg, logger):
    if err:
        err_msg = 'ERROR: Message failed delivery: {}'.format(err)
        st.error(err_msg)
        logger.error(err_msg)
    else:
        key = msg.key().decode('utf-8') if msg.key() is not None else ""
        value = msg.value().decode('utf-8') if msg.value() is not None else ""
        success_msg = "Produced event to topic {topic}: key = {key:12} value = {value:12}".format(topic=msg.topic(), key=key, value=value)
        st.success(success_msg)
        logger.info(success_msg)
