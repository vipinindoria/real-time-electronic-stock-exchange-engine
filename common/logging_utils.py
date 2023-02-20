import sys
import logging


def get_logger(name, cfg):
    log = logging.getLogger(name=name)
    log.setLevel(cfg.filehandlerlevel)
    formatter = logging.Formatter(cfg.formatter)
    if cfg.createfile:
        fh = logging.FileHandler(cfg.logfilename)
        fh.setLevel(level=cfg.filehandlerlevel)
        fh.setFormatter(formatter)
        log.addHandler(fh)
    if cfg.logonconsole:
        ce = logging.StreamHandler(sys.stderr)
        ce.setLevel(level=cfg.consoleerrorhandlerlevel)
        ce.setFormatter(formatter)
        log.addHandler(ce)
        co = logging.StreamHandler(sys.stdout)
        co.setLevel(level=cfg.consoleoutputhandlerlevel)
        co.setFormatter(formatter)
        log.addHandler(co)
    return log
