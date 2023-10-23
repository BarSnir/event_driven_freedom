import logging
import os
from logging import Logger
from colorlog import ColoredFormatter

class Logger:

    _LOGFORMAT = "%(log_color)s%(levelname)-8s%(reset)s | %(log_color)s%(message)s%(reset)s"

    _log_level_dict = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'CRITICAL': logging.CRITICAL,
    }

    def __init__(self)-> Logger:
        log_level = self._log_level_dict.get(os.getenv('LOG_LEVEL', 'INFO'))
        logging.root.setLevel(log_level)
        formatter = ColoredFormatter(self._LOGFORMAT)
        stream = logging.StreamHandler()
        stream.setLevel(log_level)
        stream.setFormatter(formatter)
        logger = logging.getLogger(os.getenv('LOGGER_NAME', 'root'))
        logger.setLevel(log_level)
        logger.addHandler(stream)
        return logger