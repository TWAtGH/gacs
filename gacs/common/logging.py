
import logging


def setup_logging(lvl=logging.INFO):
    logger = logging.getLogger('gacs')

    logger.setLevel(lvl)
    hdlr = logging.StreamHandler()
    def emit_decorator(fnc):
        def func(*args):
            levelno = args[0].levelno
            if levelno >= logging.CRITICAL:
                color = '\033[31;1m'
            elif levelno >= logging.ERROR:
                color = '\033[31;1m'
            elif levelno >= logging.WARNING:
                color = '\033[33;1m'
            elif levelno >= logging.INFO:
                color = '\033[32;1m'
            elif levelno >= logging.DEBUG:
                color = '\033[36;1m'
            else:
                color = '\033[0m'

            if not getattr(args[0], 'simtime', None):
                args[0].simtime = '-'

            format_str = '[{simtime:>10}: {name:<20}] {message}'

            formatter = logging.Formatter(color + format_str + '\033[0m', style='{')
            hdlr.setFormatter(formatter)
            return fnc(*args)
        return func

    hdlr.emit = emit_decorator(hdlr.emit)
    logger.addHandler(hdlr)
    return SimLogger(logger)


class SimLogger:
    def __init__(self, logger):
        if isinstance(logger, str):
            self.logger = logging.getLogger(logger)
        else:
            self.logger = logger

    def getChild(self, name):
        return SimLogger(self.logger.getChild(name))

    def debug(self, message, simtime=None):
        self.logger.debug(message, extra={'simtime': simtime})

    def info(self, message, simtime=None):
        self.logger.info(message, extra={'simtime': simtime})

    def warning(self, message, simtime=None):
        self.logger.warning(message, extra={'simtime': simtime})

    def error(self, message, simtime=None):
        self.logger.error(message, extra={'simtime': simtime})

    def critical(self, message, simtime=None):
        self.logger.critical(message, extra={'simtime': simtime})
