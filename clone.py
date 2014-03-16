#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import argparse
import psycopg2
from utils import HistoryInspector, ClonePopulator, get_logger


def connect(history, clone):
    histcon = psycopg2.connect(**history)
    clonecon = psycopg2.connect(**clone)
    histcon.autocommit = True
    clonecon.autocommit = True
    return histcon, clonecon


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--history-host', help='Hostname or IP of the history database')
    parser.add_argument('--history-port', help='Port number for the history database')
    parser.add_argument('--history-user', help='Username for the history database')
    parser.add_argument('--history-password', help='Password for the history database')
    parser.add_argument('--history-database', help='Name of the history database')

    parser.add_argument('--clone-host', help='Hostname or IP of the clone database')
    parser.add_argument('--clone-port', help='Port number for the clone database')
    parser.add_argument('--clone-user', help='Username for the clone database')
    parser.add_argument('--clone-password', help='Password for the clone database')
    parser.add_argument('--clone-database', help='Name of the clone database')

    args = parser.parse_args()

    history = {
        'host': args.history_host,
        'port': args.history_port,
        'user': args.history_user,
        'password': args.history_password,
        'database': args.history_database
    }

    clone = {
        'host': args.clone_host,
        'port': args.clone_port,
        'user': args.clone_user,
        'password': args.clone_password,
        'database': args.clone_database
    }

    histcon, clonecon = connect(history, clone)

    inspector_logger = get_logger('inspector')
    populator_logger = get_logger('populator')

    inspector = HistoryInspector(histcon, logger=inspector_logger)
    populator = ClonePopulator(clonecon, inspector.update, history, logger=populator_logger)
    populator.initialize()
    for schema in inspector.schemas():
        populator.create_schema(schema)
        for table in inspector.tables(schema):
            inspector.columns(table)
            populator.create_table(table)
    clonecon.commit()


if __name__ == '__main__':
    main()
