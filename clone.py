#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
from utils import HistoryInspector, ClonePopulator, get_logger

CLONE = {
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
    clonecon = psycopg2.connect(**CLONE)
    histcon = psycopg2.connect(**HISTORY)
    return clonecon, histcon


if __name__ == "__main__":
    clonecon, histcon = connect()

    inspector_logger = get_logger('inspector')
    populator_logger = get_logger('populator')

    inspector = HistoryInspector(histcon, logger=inspector_logger)
    populator = ClonePopulator(clonecon, inspector.update, HISTORY, logger=populator_logger)
    populator.initialize()
    for schema in inspector.schemas():
        populator.create_schema(schema)
        for table in inspector.tables(schema):
            inspector.columns(table)
            populator.create_table(table)
    clonecon.commit()
