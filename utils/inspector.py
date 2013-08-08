# -*- coding: utf-8 -*-

from dbobjects import Schema, Table, Column, StartColumn, StopColumn

class SlaveInspector(object):

    def __init__(self, con, logger=None):
        self.con = con
        self.db_oid = self._get_db_oid()
        self.tabledict = {}
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger()
            self.logger.addHandler(logging.NullHandler())

    def _get_db_oid(self):
        cur = self.con.cursor()
        cur.execute('SELECT oid FROM pg_database WHERE datname = current_database()')
        row = cur.fetchone()
        cur.close()
        return row[0]

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
        SELECT oid, relname, relfilenode
        FROM pg_class
        WHERE relnamespace = %s AND relkind = 'r'
        """, (schema.oid,))
        for oid, name, filenode in cur:
            self.logger.info('table {}, {} ({})'.format(oid, name, filenode))
            table = Table(schema, oid, name, filenode, con=self.con)
            self.tabledict[filenode] = table
            yield table
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

    def resume(self):
        cur = self.con.cursor()
        cur.execute('SELECT pg_xlog_replay_resume()')
        cur.close()

    def get(self, table, block, offset):
        cur = self.con.cursor()
        print "SELECT * FROM {} WHERE ctid = '({},{})'".format(table.long_name, block, offset)
        cur.execute("SELECT * FROM {} WHERE ctid = '({},{})'".format(table.long_name, block, offset))
        row = cur.fetchone()
        cur.close()
        return row


class HistoryInspector(object):

    def __init__(self, con, logger=None):
        self.con = con
        if logger:
            self.logger = logger
        else:
            self.logger = logging.getLogger()
            self.logger.addHandler(logging.NullHandler())
        self.update = self._update()

    def _update(self):
        cur = self.con.cursor()
        cur.execute("""
        SELECT id, time
        FROM marty_updates
        ORDER BY time DESC LIMIT 1
        """)
        update_id, time = cur.fetchone()
        cur.close()
        self.logger.debug('got update id {} from {}'.format(update_id, time))
        return update_id

    def schemas(self):
        cur = self.con.cursor()
        cur.execute("""
        SELECT oid, name
        FROM marty_schemas
        WHERE start <= %(update_id)s AND (stop IS NULL OR stop > %(update_id)s)
        """, {'update_id': self.update})
        for oid, name, in cur:
            yield Schema(oid, name)
        cur.close()

    def tables(self, schema):
        cur = self.con.cursor()
        cur.execute("""
        SELECT oid, name, internal_name
        FROM marty_tables
        WHERE schema = %(schema_id)s
          AND start <= %(update_id)s AND (stop IS NULL OR stop > %(update_id)s)
        """, {'schema_id': schema.oid, 'update_id': self.update})
        for oid, name, internal_name in cur:
            yield Table(schema, oid, name, internal_name)
        cur.close()

    def columns(self, table):
        cur = self.con.cursor()
        cur.execute("""
        SELECT name, number, type, length, internal_name
        FROM marty_columns
        WHERE table_oid = %(table_oid)s
          AND start <= %(update_id)s AND (stop IS NULL OR stop > %(update_id)s)
        ORDER BY number ASC
        """, {'table_oid': table.oid, 'update_id': self.update})
        for name, number, type, length, internal_name in cur:
            table.add_column(name, number, type, length, internal_name=internal_name)
        cur.close()

