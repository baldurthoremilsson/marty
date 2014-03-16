#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
import sys
import argparse
import re

from utils import SlaveInspector, HistoryPopulator, get_logger
from utils.dbobjects import Schema


class RegExer(object):
    def __init__(self):
        self.m = None

        rel_tid = 'rel (?P<spc_node>\d+)/(?P<db_node>\d+)/(?P<rel_node>\d+); tid (?P<block>\d+)/(?P<offset>\d+)'

        self.regexes = {
            'insert': re.compile(r'Heap - insert(?:\(init\))?: {}'.format(rel_tid)),
            'update': re.compile(r'Heap - (?:hot_)?update: {} xmax \d+ (?:[A-Z_]+ )?; new tid (?P<new_block>\d+)/(?P<new_offset>\d+) xmax \d+'.format(rel_tid)),
            'delete': re.compile(r'Heap - delete: {}'.format(rel_tid)),
            'lastup': re.compile(r'LOG:  database system was interrupted; last known up at (?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})'),
            'connect': re.compile(r'LOG:  database system is ready to accept read only connections'),
            'paused': re.compile(r'LOG:  recovery has paused'),
            'redo': re.compile(r'LOG:  REDO @ [0-9A-F]+/[0-9A-F]+; LSN [0-9A-F]+/[0-9A-F]+: prev [0-9A-F]+/[0-9A-F]+; xid [0-9]+; len [0-9]+(?:; bkpb[0-9]+)?: (.*)'),
            'commit': re.compile(r'Transaction - commit: (?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)'),
        }

    def match(self, regex, pattern):
        self.m = self.regexes[regex].match(pattern)
        return self.m

    @property
    def groupdict(self):
        return self.m.groupdict()

    def __getitem__(self, key):
        return self.groupdict[key]

    def get(self, key, val):
        try:
            return self[key]
        except KeyError:
            return val



class Worker(object):
    def __init__(self, infile, regexer, connect_callback):
        self.infile = infile
        self.regexer = regexer
        self.connect_callback = connect_callback
        self.inspector = None
        self.populator = None
        self.slavecon = None
        self._work = []
        self._commited = False
        self._timestamp = None

    def consume(self):
        self.infile.flush()
        line = self.infile.readline()
        if self.regexer.match('lastup', line):
            self._timestamp = self.regexer.m.groupdict()['timestamp']
        elif self.regexer.match('connect', line):
            self.slavecon, self.inspector, self.populator = self.connect_callback(self._timestamp)
            self.inspector.resume()
            self._timestamp = None
        elif self.regexer.match('paused', line):
            if self.inspector:
                self.inspector.resume()
        elif self.regexer.match('redo', line):
            work = self.regexer.m.groups()[0]
            if self.regexer.match('commit', work):
                if self.inspector:
                    self._commited = True
                    self._timestamp = self.regexer.m.groupdict()['timestamp']
            elif self._commited:
                self.populator.update(self._timestamp)
                for w in self._work:
                    self.work(w)
                self._work = []
                self._commited = False
                self._timestamp = None
            self._work.append(work)

    def work(self, work):
        for action in 'insert', 'update', 'delete':
            if self.regexer.match(action, work):
                break
        else:
            # If the work is not an insert, update or delete action we leave
            # (we only run the else part if the for loop does not break)
            return

        db_node = int(self.regexer.get('db_node', 0))
        rel_node = int(self.regexer.get('rel_node', 0))
        block = int(self.regexer.get('block', 0))
        offset = int(self.regexer.get('offset', 0))
        new_block = int(self.regexer.get('new_block', 0))
        new_offset = int(self.regexer.get('new_offset', 0))

        if db_node != self.inspector.db_oid:
            return

        if rel_node in self.inspector.system_tables:
            table = self.inspector.system_tables[rel_node]
            if table.name == 'pg_namespace':
                self.schema_change(action, block, offset, new_block, new_offset)
            elif table.name == 'pg_class':
                self.table_change(action, block, offset, new_block, new_offset)
            elif table.name == 'pg_attribute':
                self.column_change(action, block, offset, new_block, new_offset)
            return

        table = self.inspector.tabledict.get(rel_node, None)
        if not table:
            return

        if action == 'insert':
            self.insert(table, block, offset)
        elif action == 'update':
            self.update(table, block, offset, new_block, new_offset)
        elif action == 'delete':
            self.delete(table, block, offset)

    def ctid(self, block, offset):
        return '({},{})'.format(block, offset)

    def schema_change(self, action, block, offset, new_block, new_offset):
        if action == 'insert':
            schema = self.inspector.get_schema(self.ctid(block, offset))
            self.populator.add_schema(schema)
        elif action == 'update':
            schema = self.inspector.get_schema(self.ctid(new_block, new_offset))
            self.populator.add_schema(schema)
            self.populator.remove_schema(self.ctid(block, offset))
        elif action == 'delete':
            self.populator.remove_schema(self.ctid(block, offset))

    def table_change(self, action, block, offset, new_block, new_offset):
        if action == 'insert':
            table = self.inspector.get_table(self.ctid(block, offset))
            if table:
                self.populator.add_table(table)
                self.populator.create_table(table)
        elif action == 'update':
            table = self.inspector.get_table(self.ctid(new_block, new_offset))
            if table:
                self.populator.add_table(table)
            self.populator.remove_table(self.ctid(block, offset))
        elif action == 'delete':
            table = self.populator.get_table(self.ctid(block, offset))
            if table:
                self.populator.delete_all(table)
            self.populator.remove_table(self.ctid(block, offset))

    def column_change(self, action, block, offset, new_block, new_offset):
        update = self.populator.update_id
        if action == 'insert':
            column = self.inspector.get_column(self.ctid(block, offset), update=update)
            if column:
                self.populator.add_column(column)
                self.populator.add_data_column(column)
        if action == 'update':
            old_column = self.populator.get_column(self.ctid(block, offset))
            column = self.inspector.get_column(self.ctid(new_block, new_offset),
                    update=update, internal_name=old_column.internal_name)
            if column:
                self.populator.add_column(column)
            self.populator.remove_column(self.ctid(block, offset))
        if action == 'delete':
            self.populator.remove_column(self.ctid(block, offset))

    def insert(self, table, block, offset):
        row = self.inspector.get(table, block, offset)
        self.populator.insert(table, block, offset, row)

    def update(self, table, block, offset, new_block, new_offset):
        self.delete(table, block, offset)
        self.insert(table, new_block, new_offset)

    def delete(self, table, block, offset):
        self.populator.delete(table, block, offset)


def connect(slave, history):
    slavecon = psycopg2.connect(**slave)
    histcon = psycopg2.connect(**history)
    slavecon.autocommit = True
    histcon.autocommit = True
    return slavecon, histcon

def connect_callback(timestamp):
    parser = argparse.ArgumentParser()

    parser.add_argument('--slave-host', help='Hostname or IP of the slave database')
    parser.add_argument('--slave-port', help='Port number for the slave database')
    parser.add_argument('--slave-user', help='Username for the slave database')
    parser.add_argument('--slave-password', help='Password for the slave database')
    parser.add_argument('--slave-database', help='Name of the slave database')

    parser.add_argument('--history-host', help='Hostname or IP of the history database')
    parser.add_argument('--history-port', help='Port number for the history database')
    parser.add_argument('--history-user', help='Username for the history database')
    parser.add_argument('--history-password', help='Password for the history database')
    parser.add_argument('--history-database', help='Name of the history database')

    args = parser.parse_args()

    slave = {
        'host': args.slave_host,
        'port': args.slave_port,
        'user': args.slave_user,
        'password': args.slave_password,
        'database': args.slave_database
    }

    history = {
        'host': args.history_host,
        'port': args.history_port,
        'user': args.history_user,
        'password': args.history_password,
        'database': args.history_database
    }


    slavecon, histcon = connect(slave, history)

    inspector_logger = get_logger('inspector')
    populator_logger = get_logger('populator')

    inspector = SlaveInspector(slavecon, logger=inspector_logger)
    populator = HistoryPopulator(histcon, logger=populator_logger)

    populator.create_tables()
    populator.update(timestamp)
    for schema in inspector.schemas():
        populator.add_schema(schema)
        for table in inspector.tables(schema):
            inspector.columns(table)
            populator.add_table(table)
            populator.create_table(table)
            populator.fill_table(table)

    return slavecon, inspector, populator


def main():
    worker = Worker(sys.stdin, RegExer(), connect_callback)
    while True:
        worker.consume()


if __name__ == "__main__":
    main()
