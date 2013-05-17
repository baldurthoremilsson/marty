# -*- coding: utf-8 -*-

import sys
import logging

from inspector import SlaveInspector, HistoryInspector
from populator import HistoryPopulator, DevPopulator


__all__ = ('SlaveInspector', 'HistoryInspector', 'HistoryPopulator',
        'DevPopulator', 'get_logger')


def get_logger(name):
    formatter = logging.Formatter(logging.BASIC_FORMAT)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger

