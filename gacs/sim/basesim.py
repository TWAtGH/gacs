
from gacs.common.logging import setup_logging
from gacs.common.utils import setup_utils

class BaseSim:
    def __init__(self):
        self.logger = setup_logging()

        setup_utils()
