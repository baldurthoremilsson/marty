#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import psycopg2

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


def create_tables(con):
    cur = con.cursor()

    # marty_updates
    cur.execute("""
    CREATE TABLE IF NOT EXISTS marty_updates(
        id SERIAL PRIMARY KEY,
        time TIMESTAMP DEFAULT current_timestamp NOT NULL
    )
    """)

    # marty_schemas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS marty_schemas(
        oid oid NOT NULL,
        name name NOT NULL,
        start integer REFERENCES marty_updates(id) NOT NULL,
        stop integer REFERENCES marty_updates(id)
    )
    """)

    # marty_tables
    cur.execute("""
    CREATE TABLE IF NOT EXISTS marty_tables(
        oid oid NOT NULL,
        name name NOT NULL,
        schema oid NOT NULL,
        start integer REFERENCES marty_updates(id) NOT NULL,
        stop integer REFERENCES marty_updates(id)
    )
    """)
    # marty_columns
    cur.execute("""
    CREATE TABLE IF NOT EXISTS marty_columns(
        table_oid oid NOT NULL,
        name name NOT NULL,
        number int2 NOT NULL,
        type name NOT NULL,
        length int4 NOT NULL,
        start integer REFERENCES marty_updates(id) NOT NULL,
        stop integer REFERENCES marty_updates(id)
    )
    """)
    # marty_constraints

    cur.close()


def cleanup(con):
    cur = con.cursor()
    cur.execute("""
    BEGIN;
    DROP TABLE IF EXISTS marty_tables;
    DROP TABLE IF EXISTS marty_schemas;
    DROP TABLE IF EXISTS marty_updates;
    COMMIT;
    """)
    cur.close()
    con.commit()


class Populator(object):

    def __init__(self, slavecon, histcon):
        self.slavecon = slavecon
        self.histcon = histcon
        self.update_id = None

    def populate(self):
        self._get_update_id()
        self._populate_schemas()
        self.histcon.commit()

        print self.update_id

    def _get_update_id(self):
        cur = self.histcon.cursor()
        cur.execute("""
        INSERT INTO marty_updates DEFAULT VALUES RETURNING id
        """)
        self.update_id = cur.fetchone()[0]
        cur.close()

    def _populate_schemas(self):
        cur = self.slavecon.cursor()
        cur.execute("""
        SELECT oid, nspname
        FROM pg_namespace
        WHERE nspname NOT LIKE 'information_schema' AND nspname NOT LIKE 'pg_%'
        """)
        for oid, name in cur:
            self._store_schema(oid, name)
            self._populate_tables(oid)
        cur.close()

    def _store_schema(self, oid, name):
        cur = self.histcon.cursor()
        cur.execute("""
        INSERT INTO marty_schemas(oid, name, start) VALUES(%s, %s, %s)
        """, (oid, name, self.update_id))
        cur.close()

    def _populate_tables(self, schema_oid):
        """
        Missing:
            indexes (relkind = i)
            sequences (relkind = S)
            views (relkind = v)
            materialized views (relkind = m)
            composite type (relkind = c)
            TOAST tables (relkind = t)
            foreign tables (relkind = f)
        """
        cur = self.slavecon.cursor()
        cur.execute("""
        SELECT oid, relname
        FROM pg_class
        WHERE relnamespace = %s AND relkind = 'r'
        """, (schema_oid,))
        for oid, name in cur:
            self._store_table(oid, name, schema_oid)
            self._populate_columns(oid, name)
        cur.close()

    def _store_table(self, oid, name, schema_oid):
        cur = self.histcon.cursor()
        cur.execute("""
        INSERT INTO marty_tables(oid, name, schema, start)
        VALUES(%s, %s, %s, %s)
        """, (oid, name, schema_oid, self.update_id))
        cur.close()

    def _populate_columns(self, table_oid, table_name):
        """
        Missing:
            arrays (attndims)
            data in TOAST tables (attstorage)
            not null (attnotnull)
            default value (atthasdef)
            attislocal?
            attinhcount?
            collation (attcollation)
            attoptions?
            attfdwoptions?
        """
        cur = self.slavecon.cursor()
        cur.execute("""
        SELECT attname, typname, attnum, atttypmod
        FROM pg_attribute
        LEFT JOIN pg_type ON pg_attribute.atttypid = pg_type.oid
        WHERE attrelid = %s AND attisdropped = false AND attnum > 0
        """, (table_oid,))
        for name, number, type, length in cur:
            self._store_column(table_oid, name, number, type, length)
        cur.close()

    def _store_column(self, table_oid, name, number, type, length):
        cur = self.histcon.cursor()
        cur.execute("""
        INSERT INTO marty_columns(table_oid, name, number, type, length, start)
        VALUES(%s, %s, %s, %s, %s)
        """, (table_oid, name, type, length, self.update_id))
        cur.close()

    def _populate_constraints(self):
        pass


def connect():
    slavecon = psycopg2.connect(**SLAVE)
    histcon = psycopg2.connect(**HISTORY)
    return slavecon, histcon

if __name__ == "__main__":
    slavecon, histcon = connect()
    if len(sys.argv) == 2 and sys.argv[1] == 'clean':
        cleanup(histcon)
        sys.exit(0)
    create_tables(histcon)
    populator = Populator(slavecon, histcon)
    populator.populate()

