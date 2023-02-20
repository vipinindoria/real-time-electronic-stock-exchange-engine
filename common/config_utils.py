import os
from hydra import compose, initialize
from hydra.core import global_hydra


def get_config(config_root, root_file):
    global_hydra.GlobalHydra.instance().clear()
    initialize(config_path=os.path.join('./../', config_root))
    return compose(config_name=root_file)