#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
from utils import HistoryInspector, DevPopulator, get_logger

DEV = {
    'host': 'localhost',
    'port': 5437,
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
    devcon = psycopg2.connect(**DEV)
    histcon = psycopg2.connect(**HISTORY)
    return devcon, histcon


if __name__ == "__main__":
    devcon, histcon = connect()

    inspector_logger = get_logger('inspector')
    populator_logger = get_logger('populator')

    inspector = HistoryInspector(histcon, logger=inspector_logger)
    populator = DevPopulator(devcon, inspector.update, HISTORY, logger=populator_logger)
    populator.initialize()
    for schema in inspector.schemas():
        populator.create_schema(schema)
        for table in inspector.tables(schema):
            inspector.columns(table)
            populator.create_table(table)
    devcon.commit()
