#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import sys
import psycopg2

from utils import Inspector, Populator

SLAVE = {
    'host': 'localhost',
    'port': 5435,
    'user': 'baldur',
    'database': 'baldur',
}

HISTORY = {
    'host': 'localhost',
    'port': 5436,
    'user': 'baldur',
    'database': 'history',
}


def connect():
    slavecon = psycopg2.connect(**SLAVE)
    histcon = psycopg2.connect(**HISTORY)
    return slavecon, histcon

def get_logger(name):
    formatter = logging.Formatter(logging.BASIC_FORMAT)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger

if __name__ == "__main__":
    slavecon, histcon = connect()

    inspector_logger = get_logger('inspector')
    populator_logger = get_logger('populator')

    inspector = Inspector(slavecon, logger=inspector_logger)
    populator = Populator(histcon, logger=populator_logger)
    populator.create_tables()
    populator.lock_update()
    for schema in inspector.schemas():
        populator.add_schema(schema)
        for table in inspector.tables(schema):
            inspector.columns(table)
            populator.add_table(table)
            populator.create_table(table)
            populator.fill_table(table)

