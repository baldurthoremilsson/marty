#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2

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


class Populator(object):

    def __init__(self, devcon, histcon):
        self.devcon = devcon
        self.histcon = histcon
        self.update_id = None

    def populate(self):
        self._get_update_id()
        self._populate_schemas()
        self.devcon.commit()

    def _get_update_id(self):
        cur = self.histcon.cursor()
        cur.execute("""
        SELECT id
        FROM marty_updates
        ORDER BY time DESC
        LIMIT 1
        """)
        self.update_id = cur.fetchone()[0]
        cur.close()

    def _populate_schemas(self):
        cur = self.histcon.cursor()
        cur.execute("""
        SELECT oid, name
        FROM marty_schemas
        WHERE start <= %s AND (stop IS NULL OR stop > %s)
        """, (self.update_id, self.update_id))
        for oid, name in cur:
            schema_oid = self._create_schema(oid, name)
            self._populate_tables(schema_oid, oid, name)
        cur.close()

    def _create_schema(self, oid, name):
        cur = self.devcon.cursor()
        cur.execute('CREATE SCHEMA IF NOT EXISTS {}'.format(name))
        cur.execute("""
        SELECT oid
        FROM pg_namespace
        WHERE nspname = %s
        """, (name,))
        schema_oid = cur.fetchone()[0]
        cur.close()
        return schema_oid

    def _populate_tables(self, local_schema_oid, schema_oid, schema_name):
        cur = self.histcon.cursor()
        cur.execute("""
        SELECT oid, name
        FROM marty_tables
        WHERE schema = %s AND start <= %s AND (stop IS NULL OR stop > %s)
        """, (schema_oid, self.update_id, self.update_id))
        for oid, name in cur:
            self._create_table(local_schema_oid, schema_name, oid, name)
        cur.close()

    def _get_columns(self, table_oid):
        cur = self.histcon.cursor()
        cur.execute("""
        SELECT name, type, length
        FROM marty_columns
        WHERE table_oid = %s AND start <= %s AND (stop IS NULL OR stop > %s)
        """, (table_oid, self.update_id, self.update_id))
        columns = cur.fetchall()
        cur.close()
        return columns

    def _create_table(self, schema_oid, schema_name, oid, name):
        table_name = '{}.{}'.format(schema_name, name)
        columns = self._get_columns(oid)
        query = 'CREATE TABLE {}({})'
        cols = ', '.join('{} {}'.format(col[0], col[1]) for col in columns)
        cur = self.devcon.cursor()
        cur.execute(query.format(table_name, cols))
        cur.execute("""
        SELECT oid
        FROM pg_class
        WHERE relnamespace = %s AND relname = %s
        """, (schema_oid, name))
        table_oid = cur.fetchone()[0]
        print table_oid
        for column in columns:
            cur.execute("""
            UPDATE pg_attribute
            SET atttypmod = %s
            WHERE attrelid = %s AND attname = %s
            """, (column[2], table_oid, column[0]))
        cur.close()


def connect():
    devcon = psycopg2.connect(**DEV)
    histcon = psycopg2.connect(**HISTORY)
    return devcon, histcon


if __name__ == "__main__":
    devcon, histcon = connect()
    populator = Populator(devcon, histcon)
    populator.populate()
