#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2
import sys
import re

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


class RegExer(object):
    def __init__(self):
        self.m = None

        rel_tid = 'rel (?P<spc_node>\d+)/(?P<db_node>\d+)/(?P<rel_node>\d+); tid (?P<block>\d+)/(?P<offset>\d+)'

        self.regexes = {
            'insert': re.compile(r'Heap - insert(?:\(init\))?: {}'.format(rel_tid)),
            'update': re.compile(r'Heap - (?:hot_)?update: {} xmax \d+ ; new tid (?P<new_block>\d+)/(?P<new_offset>\d+) xmax \d+'.format(rel_tid)),
            'delete': re.compile(r'Heap - delete: {}'.format(rel_tid)),
            'connect': re.compile(r'LOG:  database system is ready to accept read only connections'),
            'paused': re.compile(r'LOG:  recovery has paused'),
            'redo': re.compile(r'LOG:  REDO @ [0-9A-F]+/[0-9A-F]+; LSN [0-9A-F]+/[0-9A-F]+: prev [0-9A-F]+/[0-9A-F]+; xid [0-9]+; len [0-9]+: (.*)'),
            'commit': re.compile(r'Transaction - commit'),
        }

    def match(self, regex, pattern):
        self.m = self.regexes[regex].match(pattern)
        return self.m



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

    def consume(self):
        self.infile.flush()
        line = self.infile.readline()
        if self.regexer.match('connect', line):
            self.slavecon, self.inspector, self.populator = self.connect_callback()
            self.inspector.resume()
        elif self.regexer.match('paused', line):
            if self.inspector:
                self.inspector.resume()
        elif self.regexer.match('redo', line):
            work = self.regexer.m.groups()[0]
            if self.regexer.match('commit', work):
                if self.inspector:
                    self._commited = True
            elif self._commited:
                self.populator.update()
                for work in self._work:
                    self.work(work)
                self._work = []
                self._commited = False
            self._work.append(work)

    def work(self, work):
        if self.regexer.match('insert', work):
            self.insert(**self.regexer.m.groupdict())
        elif self.regexer.match('update', work):
            self.update(**self.regexer.m.groupdict())
        elif self.regexer.match('delete', work):
            self.delete(**self.regexer.m.groupdict())

    def insert(self, spc_node, db_node, rel_node, block, offset):
        if int(db_node) != self.inspector.db_oid:
            return
        table = self.inspector.tabledict.get(int(rel_node), None)
        if not table:
            return
        row = self.inspector.get(table, block, offset)
        self.populator.insert(table, block, offset, row)
        print 'insert', row

    def update(self, spc_node, db_node, rel_node, block, offset, new_block, new_offset):
        self.delete(spc_node, db_node, rel_node, block, offset)
        self.insert(spc_node, db_node, rel_node, new_block, new_offset)

    def delete(self, spc_node, db_node, rel_node, block, offset):
        if int(db_node) != self.inspector.db_oid:
            return
        table = self.inspector.tabledict.get(int(rel_node), None)
        if not table:
            return
        self.populator.delete(table, block, offset)
        print 'delete', table.name


def connect():
    slavecon = psycopg2.connect(**SLAVE)
    histcon = psycopg2.connect(**HISTORY)
    return slavecon, histcon

def connect_callback():
    slavecon, histcon = connect()

    inspector_logger = get_logger('inspector')
    populator_logger = get_logger('populator')

    inspector = SlaveInspector(slavecon, logger=inspector_logger)
    populator = HistoryPopulator(histcon, logger=populator_logger)
    populator.create_tables()
    populator.update()
    for schema in inspector.schemas():
        populator.add_schema(schema)
        for table in inspector.tables(schema):
            inspector.columns(table)
            populator.add_table(table)
            populator.create_table(table)
            populator.fill_table(table)

    return slavecon, inspector, populator


if __name__ == "__main__":
    worker = Worker(sys.stdin, RegExer(), connect_callback)
    while True:
        worker.consume()

