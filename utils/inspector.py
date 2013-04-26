# -*- coding: utf-8 -*-

from dbobjects import Schema, Table, Column, StartColumn, StopColumn

class Inspector(object):

    def __init__(self, con, logger=None):
        self.con = con
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger()
            self.logger.addHandler(logging.NullHandler())

    def schemas(self):
        cur = self.con.cursor()
        cur.execute("""
        SELECT oid, nspname
        FROM pg_namespace
        WHERE nspname NOT LIKE 'information_schema' AND nspname NOT LIKE 'pg_%'
        """)
        for oid, name in cur:
            self.logger.info('schema {}, {}'.format(oid, name))
            yield Schema(oid, name)
        cur.close()

    def tables(self, schema):
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
        cur = self.con.cursor()
        cur.execute("""
        SELECT oid, relname
        FROM pg_class
        WHERE relnamespace = %s AND relkind = 'r'
        """, (schema.oid,))
        for oid, name in cur:
            self.logger.info('table {}, {}'.format(oid, name))
            yield Table(self.con, schema, oid, name)
        cur.close()

    def columns(self, table):
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
        cur = self.con.cursor()
        cur.execute("""
        SELECT attname, attnum, typname, atttypmod
        FROM pg_attribute
        LEFT JOIN pg_type ON pg_attribute.atttypid = pg_type.oid
        WHERE attrelid = %s AND attisdropped = false AND attnum > 0
        ORDER BY attnum ASC
        """, (table.oid,))
        for name, number, type, length in cur:
            self.logger.info('column {} {}({})'.format(name, type, length))
            table.add_column(name, number, type, length)
        cur.close()

