#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2

from utils import SlaveInspector, HistoryPopulator, get_logger

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

if __name__ == "__main__":
    slavecon, histcon = connect()

    inspector_logger = get_logger('inspector')
    populator_logger = get_logger('populator')

    inspector = SlaveInspector(slavecon, logger=inspector_logger)
    populator = HistoryPopulator(histcon, logger=populator_logger)
    populator.create_tables()
    populator.lock_update()
    for schema in inspector.schemas():
        populator.add_schema(schema)
        for table in inspector.tables(schema):
            inspector.columns(table)
            populator.add_table(table)
            populator.create_table(table)
            populator.fill_table(table)

