from common import config_utils
from common import logging_utils
from order_app import order_producer
from common import kafka_utils

from omegaconf import OmegaConf

if __name__ == '__main__':
    cfg = config_utils.get_config('resources', 'app.yaml')
    logger = logging_utils.get_logger(name=cfg.log.loggername, cfg=cfg.log)
    producer = kafka_utils.get_producer(logger, OmegaConf.to_container(cfg.enviornment.kafka, resolve=True))
    order_producer.run(logger, cfg, producer)
