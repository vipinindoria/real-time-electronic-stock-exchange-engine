from common import config_utils
from common import logging_utils
from order_app import order_producer
from common import kafka_utils

from omegaconf import OmegaConf
import streamlit as st


if __name__ == '__main__':
    # Get or initialize the session state
    appstate = st.session_state.get("appstate", {
        "appcfg": None,
        "applogger": None,
        "producerapp": None
    })
    if appstate["appcfg"] is None:
        appstate["appcfg"] = config_utils.get_config('resources', 'app.yaml')
    if appstate["applogger"] is None:
        appstate["applogger"] = logging_utils.get_logger(name=appstate["appcfg"].log.loggername, cfg=appstate["appcfg"].log)
    if appstate["producerapp"] is None:
        appstate["producerapp"] = kafka_utils.get_producer(appstate["applogger"], OmegaConf.to_container(appstate["appcfg"].enviornment.kafka, resolve=True))
    order_producer.run(appstate["applogger"], appstate["appcfg"], appstate["producerapp"])
    # Update the session state
    st.session_state.appstate = appstate
