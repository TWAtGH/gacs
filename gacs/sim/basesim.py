
from gacs.common.logging import setup_logging
from gacs.common.utils import setup_utils
from gacs.common import logging, utils

class BaseSim:
    def __init__(self):
        self.logger = logging.setup_logging()

        utils.setup_utils()
