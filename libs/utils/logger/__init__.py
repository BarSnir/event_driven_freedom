import logging
import os
from logging import Logger
from colorlog import ColoredFormatter

class ColorLogger:

    _LOGFORMAT = "%(log_color)s%(levelname)-8s%(reset)s | %(log_color)s%(message)s%(reset)s"

    _log_level_dict = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'CRITICAL': logging.CRITICAL,
    }

    def __init__(self, logger_name: str)-> Logger:
        self._logger_name = logger_name
        self._log_level = self._log_level_dict.get(os.getenv('LOG_LEVEL', 'INFO'))

    def get_logger(self)-> Logger:
        logging.root.setLevel(self._log_level)
        formatter = ColoredFormatter(self._LOGFORMAT)
        stream = logging.StreamHandler()
        stream.setLevel(self._log_level)
        stream.setFormatter(formatter)
        logger = logging.getLogger()
        logger.setLevel(self._log_level)
        logger.addHandler(stream)
        return logger
    
    @staticmethod
    def log_new_step_dashes(logger):
        logger.info("------------------------------------------------------------------------------")